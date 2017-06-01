package zabbix

import (
	"github.com/elastic/beats/libbeat/logp"
	"github.com/rday/zabbix"
	"sync"
)

type ValueType string

var (
	VALUE_TYPE_UNKNOWN ValueType = ""
	VALUE_TYPE_FLOAT   ValueType = "0"
	VALUE_TYPE_STRING  ValueType = "1"
	VALUE_TYPE_LOG     ValueType = "2"
	VALUE_TYPE_UINT    ValueType = "3"
	VALUE_TYPE_TEXT    ValueType = "4"
)

type zabbixAPI struct {
	api           *zabbix.API
	itemRWLock    sync.RWMutex
	itemValueType map[string]ValueType
}

func newZabbixAPI(url, user, pwd string) (zapi *zabbixAPI, err error) {
	zapi = &zabbixAPI{
		itemValueType: make(map[string]ValueType),
	}
	zapi.api, err = zabbix.NewAPI(url, user, pwd)
	if err != nil {
		return
	}

	_, err = zapi.api.Login()
	if err != nil {
		return
	}

	return
}

func (zapi *zabbixAPI) getItemValueType(item string) (vt ValueType) {
	zapi.itemRWLock.RLock()

	var found bool
	vt, found = zapi.itemValueType[item]
	if found {
		zapi.itemRWLock.RUnlock()
		return
	}

	//read
	zapi.itemRWLock.RUnlock()
	zapi.itemRWLock.Lock()
	defer zapi.itemRWLock.Unlock()

	itemInfo, err := GetItemByKey(zapi.api, item)
	if err != nil {
		logp.Err("get item error from zabbix api:%v", err)
		vt = VALUE_TYPE_UNKNOWN
	} else {
		vt = ValueType(itemInfo["value_type"].(string))
	}

	return
}

func GetItemByKey(api *zabbix.API, item string) (map[string]interface{}, error) {
	params := make(map[string]interface{}, 0)
	filter := map[string]string{"key_": item}
	params["filter"] = filter
	params["limit"] = 1

	response, err := api.ZabbixRequest("item.get", params)
	if err != nil {
		return nil, err
	}

	if response.Error.Code != 0 {
		return nil, &response.Error
	}

	items, ok := response.Result.([]interface{})
	if !ok {
		return nil, &zabbix.ZabbixError{0, "", "can't convert to []map[string]interface{}"}
	}

	if len(items) == 0 {
		return nil, &zabbix.ZabbixError{0, "", "can't get zabbix item:" + item}
	} else {
		return items[0].(map[string]interface{}), nil
	}
}