package zabbix

import (
	"encoding/json"
)

type itemDataReq struct {
	Request string `json:"request"`
	Host    string `json:"host"`
	Data    []itemData
}

type itemData struct {
	Host  string `json:"host"`
	Key   string `json:"key"`
	Clock int64  `json:"clock"`
	Ns    int64  `json:"ns"`
	Value string `json:"value"`
}

func Unmarshal(data []byte) (req *itemDataReq, err error) {
	req = &itemDataReq{}
	err = json.Unmarshal(data, req)
	return
}
