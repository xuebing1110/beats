package zabbix

import (
	"bytes"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/packetbeat/publish"
	"strconv"
	"time"
)

var (
	ZBX_NOTSUPPORTED []byte = []byte("ZBX_NOTSUPPORTED")
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

	events := pub.createEvents(requ, resp)
	for _, event := range events {
		pub.results.PublishTransaction(event)
	}
	return nil
}

func (pub *transPub) createEvents(requ, resp *message) []common.MapStr {
	// resp_time in milliseconds
	responseTime := int32(resp.Ts.Sub(requ.Ts).Nanoseconds() / 1e6)

	//events
	events := make([]common.MapStr, 0)

	//passive
	if requ.passive {
		//notes
		notes := make([]string, 0)

		event := common.MapStr{
			"@timestamp":   common.Time(requ.Ts),
			"type":         "zabbix",
			"status":       common.OK_STATUS,
			"responsetime": responseTime,
			"ip":           requ.Tuple.DstIP.String(),
			"item":         string(requ.data),
		}
		if bytes.HasPrefix(resp.data, ZBX_NOTSUPPORTED) {
			if len(resp.data) > 0 {
				notes = append(notes, string(resp.data[17:]))
				event["status"] = common.CLIENT_ERROR_STATUS
			}
		} else {
			value := string(resp.data)
			if pub.zapi == nil {
				event["value"] = value
			} else {
				item := string(requ.data)
				value_type, value_content := getSchemaByZapi(pub.zapi, item, value)
				event[value_type] = value_content
			}

		}

		// add processing notes/errors to event
		if len(requ.Notes)+len(resp.Notes) > 0 {
			notes = append(notes, requ.Notes...)
			notes = append(notes, resp.Notes...)
		}
		event["notes"] = notes

		if pub.sendRequest {
			event["request"] = requ
		}
		if pub.sendResponse {
			event["response"] = resp
		}

		events = append(events, event)
	} else {
		//active
		itemdata_req, err := Unmarshal(requ.data)
		if err != nil {
			logp.Warn("parse zabbix active request data err:%v", err)
			return events
		}

		responseTime := int32(resp.Ts.Sub(requ.Ts).Nanoseconds() / 1e6)
		for _, itemdata := range itemdata_req.Data {
			event := common.MapStr{
				"@timestamp":   common.Time(time.Unix(itemdata.Clock, itemdata.Ns)),
				"type":         "zabbix",
				"status":       requ.status,
				"responsetime": responseTime,
				"ip":           itemdata.Host,
				"item":         itemdata.Key,
			}

			if itemdata.Status == 0 {
				event["status"] = common.OK_STATUS
				value_type, value_content := getSchemaByZapi(pub.zapi, itemdata.Key, itemdata.Value)
				event[value_type] = value_content
			} else {
				event["status"] = common.CLIENT_ERROR_STATUS
				event["notes"] = [1]string{itemdata.Value}
			}

			events = append(events, event)
		}
	}

	return events
}

func getSchemaByZapi(zapi *zabbixAPI, item, value string) (string, interface{}) {
	vt := zapi.getItemValueType(item)

	switch vt {
	case VALUE_TYPE_FLOAT:
		num, err := strconv.ParseFloat(value, 10)
		if err != nil {
			return "value", string(value)
		} else {
			return "value_number", num
		}
	case VALUE_TYPE_UINT:
		num, err := strconv.Atoi(value)
		if err != nil {
			return "value", string(value)
		} else {
			return "value_number", num
		}
	default:
		return "value_str", value
	}
}
