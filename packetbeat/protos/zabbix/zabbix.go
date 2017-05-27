package zabbix

import (
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/elastic/beats/packetbeat/protos"
	"github.com/elastic/beats/packetbeat/protos/tcp"
	"github.com/elastic/beats/packetbeat/publish"
)

// zabbixPlugin application level protocol analyzer plugin
type zabbixPlugin struct {
	ports        protos.PortsConfig
	parserConfig parserConfig
	transConfig  transactionConfig
	pub          transPub
}

// Application Layer tcp stream data to be stored on tcp connection context.
type connection struct {
	streams [2]*stream
	trans   transactions
}

// Uni-directioal tcp stream state for parsing messages.
type stream struct {
	parser parser
}

var (
	debugf = logp.MakeDebug("zabbix")

	// use isDebug/isDetailed to guard debugf/detailedf to minimize allocations
	// (garbage collection) when debug log is disabled.
	isDebug = false
)

func init() {
	protos.Register("zabbix", New)
}

// New create and initializes a new zabbix protocol analyzer instance.
func New(
	testMode bool,
	results publish.Transactions,
	cfg *common.Config,
) (protos.Plugin, error) {
	p := &zabbixPlugin{}
	config := defaultConfig
	if !testMode {
		if err := cfg.Unpack(&config); err != nil {
			return nil, err
		}
	}

	if err := p.init(results, &config); err != nil {
		return nil, err
	}
	return p, nil
}

func (zp *zabbixPlugin) init(results publish.Transactions, config *zabbixConfig) error {
	if err := zp.setFromConfig(config); err != nil {
		return err
	}
	zp.pub.results = results

	isDebug = logp.IsDebug("http")

	return nil
}

func (zp *zabbixPlugin) setFromConfig(config *zabbixConfig) error {

	// set module configuration
	if err := zp.ports.Set(config.Ports); err != nil {
		return err
	}

	// set parser configuration
	parser := &zp.parserConfig
	parser.maxBytes = tcp.TCPMaxDataInStream
	parser.agentPorts = config.Ports

	// set transaction correlator configuration
	trans := &zp.transConfig
	trans.transactionTimeout = config.TransactionTimeout

	// set transaction publisher configuration
	pub := &zp.pub
	pub.sendRequest = config.SendRequest
	pub.sendResponse = config.SendResponse

	// zabbix api
	if config.ApiUrl != "" {
		var err error
		pub.zapi, err = newZabbixAPI(config.ApiUrl, config.User, config.Password)
		if err != nil {
			return err
		}
	}

	return nil
}

// ConnectionTimeout returns the per stream connection timeout.
// Return <=0 to set default tcp module transaction timeout.
func (zp *zabbixPlugin) ConnectionTimeout() time.Duration {
	return zp.transConfig.transactionTimeout
}

// GetPorts returns the ports numbers packets shall be processed for.
func (zp *zabbixPlugin) GetPorts() []int {
	return zp.ports.Ports
}

// Parse processes a TCP packet. Return nil if connection
// state shall be dropped (e.g. parser not in sync with tcp stream)
func (zp *zabbixPlugin) Parse(
	pkt *protos.Packet,
	tcptuple *common.TCPTuple, dir uint8,
	private protos.ProtocolData,
) protos.ProtocolData {
	defer logp.Recover("Parse zabbixPlugin exception")

	conn := zp.ensureConnection(private)
	st := conn.streams[dir]
	if st == nil {
		st = &stream{}
		st.parser.init(&zp.parserConfig, func(msg *message) error {
			return conn.trans.onMessage(tcptuple.IPPort(), dir, msg)
		})
		conn.streams[dir] = st
	}

	if err := st.parser.feed(pkt.Ts, pkt.Payload); err != nil {
		debugf("%v, dropping TCP stream for error in direction %v.", err, dir)
		zp.onDropConnection(conn)
		return nil
	}
	return conn
}

// ReceivedFin handles TCP-FIN packet.
func (zp *zabbixPlugin) ReceivedFin(
	tcptuple *common.TCPTuple, dir uint8,
	private protos.ProtocolData,
) protos.ProtocolData {
	return private
}

// GapInStream handles lost packets in tcp-stream.
func (zp *zabbixPlugin) GapInStream(tcptuple *common.TCPTuple, dir uint8,
	nbytes int,
	private protos.ProtocolData,
) (protos.ProtocolData, bool) {
	conn := getConnection(private)
	if conn != nil {
		zp.onDropConnection(conn)
	}

	return nil, true
}

// onDropConnection processes and optionally sends incomplete
// transaction in case of connection being dropped due to error
func (zp *zabbixPlugin) onDropConnection(conn *connection) {
}

func (zp *zabbixPlugin) ensureConnection(private protos.ProtocolData) *connection {
	conn := getConnection(private)
	if conn == nil {
		conn = &connection{}
		conn.trans.init(&zp.transConfig, zp.pub.onTransaction)
	}
	return conn
}

func (conn *connection) dropStreams() {
	conn.streams[0] = nil
	conn.streams[1] = nil
}

func getConnection(private protos.ProtocolData) *connection {
	if private == nil {
		return nil
	}

	priv, ok := private.(*connection)
	if !ok {
		logp.Warn("zabbix connection type error")
		return nil
	}
	if priv == nil {
		logp.Warn("Unexpected: zabbix connection data not set")
		return nil
	}
	return priv
}
