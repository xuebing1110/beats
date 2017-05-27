package zabbix

import (
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/packetbeat/publish"
)

// Transaction Publisher.
type transPub struct {
	sendRequest  bool
	sendResponse bool

	results publish.Transactions
	zapi    *zabbixAPI
}

func (pub *transPub) onTransaction(requ, resp *message) error {
	if pub.results == nil {
		return nil
	}

	event := pub.createEvent(requ, resp)
	pub.results.PublishTransaction(event)
	return nil
}

func (pub *transPub) createEvent(requ, resp *message) common.MapStr {
	// resp_time in milliseconds
	responseTime := int32(resp.Ts.Sub(requ.Ts).Nanoseconds() / 1e6)

	//status
	status := resp.status
	if status == "" {
		status = common.ERROR_STATUS
	}

	event := common.MapStr{
		"@timestamp":   common.Time(requ.Ts),
		"type":         "zabbix",
		"status":       status,
		"responsetime": responseTime,
		"ip":           requ.Tuple.DstIP.String(),
		"item":         requ.item,
		"value":        resp.value,
	}
	if pub.zapi == nil {
		event["value"] = resp.value
	} else {
		vt := pub.zapi.getItemValueType(requ.item)
		switch vt {
		case VALUE_TYPE_FLOAT:
			event["value_number"] = resp.value
		case VALUE_TYPE_UINT:
			event["value_number"] = resp.value
		default:
			event["value_str"] = resp.value
		}
	}

	// add processing notes/errors to event
	if len(requ.Notes)+len(resp.Notes) > 0 {
		event["notes"] = append(requ.Notes, resp.Notes...)
	}

	if pub.sendRequest {
		event["request"] = requ
	}
	if pub.sendResponse {
		event["response"] = resp
	}

	return event
}
