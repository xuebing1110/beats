package zabbix

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/elastic/beats/libbeat/common"
	// "strings"
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

	ZBX_NOTSUPPORTED            []byte = []byte("ZBX_NOTSUPPORTED")
	ZBX_ACTIVEDATA_REQ_FREPFIX  []byte = []byte(`{"request":`)
	ZBX_ACTIVEDATA_RESP_FREPFIX []byte = []byte(`{"response":`)
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

	passive bool

	status string
	data   []byte

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
			logp.Err("parse err:%v", err)
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

func pred(b byte) bool {
	if b == '\n' {
		return false
	} else if b == 0x01 {
		return false
	} else {
		return true
	}
}

func (p *parser) parse() (*message, error) {
	buf, err := p.buf.CollectWhile(pred)
	if err != nil {
		return nil, err
	}

	//data

	//msg type
	msg := p.message
	if bytes.Equal(buf, ZABBIX_RESP_PREFIX) {
		data, status, err := getData(p.buf)
		if err != nil {
			return msg, err
		}

		msg.data = data
		msg.status = status

		if bytes.HasPrefix(data, ZBX_ACTIVEDATA_REQ_FREPFIX) {
			msg.IsRequest = true
			msg.passive = false
		} else if bytes.HasPrefix(data, ZBX_ACTIVEDATA_RESP_FREPFIX) {
			msg.IsRequest = false
			msg.passive = false
		} else {
			msg.passive = true
			msg.IsRequest = false
		}
	} else {
		msg.IsRequest = true
		msg.passive = true

		msg.data = buf[:len(buf)-1]
		msg.status = common.OK_STATUS
	}

	//dir
	dir := applayer.NetOriginalDirection
	if !msg.IsRequest {
		dir = applayer.NetReverseDirection
	}
	msg.Direction = dir

	// msg.content = common.NetString(buf)
	msg.Size = uint64(p.buf.BufferConsumed())

	return msg, nil

}

func getData(buf streambuf.Buffer) ([]byte, string, error) {
	//length
	var bufLength uint64
	length_buf, err := buf.Collect(8)
	if err != nil {
		return nil, common.SERVER_ERROR_STATUS, err
	}
	var reverseBuf = make([]byte, 8)
	for i := 0; i < 8; i++ {
		reverseBuf[i] = length_buf[7-i]
	}
	err = binary.Read(bytes.NewBuffer(reverseBuf), binary.BigEndian, &bufLength)
	if err != nil {
		return nil, common.SERVER_ERROR_STATUS, err
	}

	//data
	value_bytes, err := buf.Collect(int(bufLength))
	if err != nil {
		return nil, common.SERVER_ERROR_STATUS, err
	}

	if bytes.HasPrefix(value_bytes, ZBX_NOTSUPPORTED) {
		return value_bytes[17:], common.CLIENT_ERROR_STATUS, nil
	} else {
		return value_bytes, common.OK_STATUS, nil
	}
}
