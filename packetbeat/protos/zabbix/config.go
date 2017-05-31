package zabbix

import (
	"github.com/elastic/beats/packetbeat/config"
	"github.com/elastic/beats/packetbeat/protos"
)

type zabbixConfig struct {
	config.ProtocolCommon `config:",inline"`
	ApiUrl                string `config:"api_url"`
	User                  string `config:"user"`
	Password              string `config:"password"`
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
