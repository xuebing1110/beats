package zabbix

import (
	"github.com/elastic/beats/packetbeat/config"
	"github.com/elastic/beats/packetbeat/protos"
)

type zabbixConfig struct {
	config.ProtocolCommon `config:",inline"`
}

var (
	defaultConfig = zabbixConfig{
		ProtocolCommon: config.ProtocolCommon{
			TransactionTimeout: protos.DefaultTransactionExpiration,
		},
	}
)

func (c *zabbixConfig) Validate() error {
	return nil
}
