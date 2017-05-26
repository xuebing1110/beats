package zabbix

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"

	"github.com/elastic/beats/libbeat/common"
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

func (p *parser) feed(ts time.Time, tuple *common.IPPortTuple, data []byte) error {
	if err := p.append(data); err != nil {
		return err
	}

	for p.buf.Total() > 0 {
		if p.message == nil {
			// allocate new message object to be used by parser with current timestamp
			p.message = p.newMessage(ts)
		}
		p.message.Tuple = *tuple

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
	msg := p.message

	//check wether it is response
	resp_found := false
	logp.Info("ports compare: %+v <=> %+v", p.config.agentPorts, msg.Tuple)
	for _, port := range p.config.agentPorts {
		if uint16(port) == msg.Tuple.SrcPort {
			resp_found = true
			break
		}
	}

	//get reponse body
	var buf []byte
	if resp_found {
		logp.Info("get zabbix response...")
		var err error

		//head
		buf, err = p.buf.Collect(5)
		if err == streambuf.ErrNoMoreBytes {
			return nil, nil
		}
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

		logp.Info("get buf: %s", string(buf))
	}

	isRequest := !resp_found
	dir := applayer.NetOriginalDirection
	if !isRequest {
		dir = applayer.NetReverseDirection
	}

	// msg.content = common.NetString(buf)
	msg.value = string(buf)
	msg.Size = uint64(p.buf.BufferConsumed())
	msg.IsRequest = isRequest
	msg.Direction = dir

	return msg, nil

}
