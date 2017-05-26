package zabbix

import (
	"bytes"
	"errors"
	"time"

	"github.com/elastic/beats/libbeat/common/streambuf"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/packetbeat/protos/applayer"
)

var (
	ZABBIX_RESP_PREFIX []byte = []byte("ZBXD")
)

type parser struct {
	buf     streambuf.Buffer
	config  *parserConfig
	message *message

	onMessage func(m *message) error
}

type parserConfig struct {
	maxBytes   int
	agentPorts []int
}

type message struct {
	applayer.Message

	// indicator for parsed message being complete or requires more messages
	// (if false) to be merged to generate full message.
	isComplete bool

	failed bool
	item   string
	value  interface{}

	// list element use by 'transactions' for correlation
	next *message
}

// Error code if stream exceeds max allowed size on append.
var (
	ErrStreamTooLarge = errors.New("Stream data too large")
)

func (p *parser) init(
	cfg *parserConfig,
	onMessage func(*message) error,
) {
	*p = parser{
		buf:       streambuf.Buffer{},
		config:    cfg,
		onMessage: onMessage,
	}
}

func (p *parser) append(data []byte) error {
	_, err := p.buf.Write(data)
	if err != nil {
		return err
	}

	if p.config.maxBytes > 0 && p.buf.Total() > p.config.maxBytes {
		return ErrStreamTooLarge
	}
	return nil
}

func (p *parser) feed(ts time.Time, data []byte) error {
	if err := p.append(data); err != nil {
		return err
	}

	for p.buf.Total() > 0 {
		if p.message == nil {
			// allocate new message object to be used by parser with current timestamp
			p.message = p.newMessage(ts)
		}

		msg, err := p.parse()
		if err != nil {
			return err
		}
		if msg == nil {
			break // wait for more data
		}

		// reset buffer and message -> handle next message in buffer
		p.buf.Reset()
		p.message = nil

		// call message handler callback
		if err := p.onMessage(msg); err != nil {
			return err
		}
	}

	return nil
}

func (p *parser) newMessage(ts time.Time) *message {
	return &message{
		Message: applayer.Message{
			Ts: ts,
		},
	}
}

func (p *parser) parse() (*message, error) {
	// wait for message being complete
	buf, err := p.buf.CollectUntil([]byte{'\n'})
	if err == streambuf.ErrNoMoreBytes {
		return nil, nil
	}

	msg := p.message
	msg.Size = uint64(p.buf.BufferConsumed())

	isRequest := true
	dir := applayer.NetOriginalDirection

	found := false
	for _, port := range p.config.agentPorts {
		if uint16(port) == msg.Tuple.DstPort {
			found = true
			break
		}
	}
	isRequest = found
	if !isRequest {
		dir = applayer.NetReverseDirection
	}

	if len(buf) >= 4 {
		logp.Info("get buf: %s", string(buf))
		prefix := buf[1:5]
		msg.failed = !bytes.Equal(ZABBIX_RESP_PREFIX, prefix)
		buf = buf[1:]
	}

	// msg.content = common.NetString(buf)
	msg.IsRequest = isRequest
	msg.Direction = dir

	return msg, nil

}
