package zabbix

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"

	"github.com/elastic/beats/libbeat/common/streambuf"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/elastic/beats/packetbeat/protos/applayer"
)

var (
	ZABBIX_RESP_PREFIX []byte = []byte{
		0x5a,
		0x42,
		0x58,
		0x44,
		0x01,
	}
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
	bufCap := p.buf.Cap()
	buf, err := p.buf.Collect(bufCap)
	if err == streambuf.ErrNoMoreBytes {
		return nil, nil
	}

	//msg type
	msg := p.message
	if buf[bufCap-1] == '\n' {
		msg.IsRequest = true
	} else {
		msg.IsRequest = false
	}

	//dir
	dir := applayer.NetOriginalDirection
	if !msg.IsRequest {
		dir = applayer.NetReverseDirection
	}

	//get reponse body
	if msg.IsRequest {
		msg.item = string(buf[0:bufCap])
		logp.Info("get zabbix request:%s", msg.item)
	} else {
		logp.Info("get zabbix response...")

		//head
		logp.Info("get buf head: %s", string(buf[0:4]))
		if !bytes.Equal(buf, ZABBIX_RESP_PREFIX) {
			return nil, nil
		}

		//length
		var bufLength uint64
		buf, err = p.buf.Collect(8)
		if err == streambuf.ErrNoMoreBytes {
			return nil, nil
		}
		var reverseBuf = make([]byte, 8)
		for i := 0; i < 8; i++ {
			reverseBuf[i] = buf[7-i]
		}
		err = binary.Read(bytes.NewBuffer(reverseBuf), binary.BigEndian, &bufLength)
		if err != nil {
			return nil, err
		}
		logp.Info("lenth: %d", bufLength)

		buf, err = p.buf.Collect(int(bufLength))
		if err == streambuf.ErrNoMoreBytes {
			return nil, nil
		}

		msg.value = string(buf)
		logp.Info("get value: %s", msg.value)
	}

	// msg.content = common.NetString(buf)
	msg.Size = uint64(p.buf.BufferConsumed())
	msg.Direction = dir

	return msg, nil

}
