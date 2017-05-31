package zabbix

import (
	"bytes"
	"encoding/binary"
	"errors"
	// "github.com/elastic/beats/libbeat/common"
	// "strings"
	"time"

	"github.com/elastic/beats/libbeat/common/streambuf"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/elastic/beats/packetbeat/protos/applayer"
)

var (
	ZABBIX_HEADER_PREFIX []byte = []byte{
		0x5a,
		0x42,
		0x58,
		0x44,
		0x01,
	}

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

	length int
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
	buf, err := p.buf.Collect(p.buf.Cap())
	if err != nil {
		logp.Info("get %s from %d bytes,err:%v", string(buf), p.buf.Cap(), err)
		return nil, err
	}

	msg := p.message

	// the first packet
	if bytes.HasPrefix(buf, ZABBIX_HEADER_PREFIX) {
		// data length
		data_len, err := getDataLength(buf[5:13])
		if err != nil {
			return nil, err
		}

		// save to message
		msg.length = data_len
		msg.data = buf[13:]

		// the part of whole message
		if p.buf.BufferConsumed()+msg.length > len(buf) {
			logp.Info("get a part of message: %s", string(msg.data))
		} else {
			logp.Info("get a whole message: %s", string(msg.data))
			msg.isComplete = true
		}

		// request or response
		if bytes.HasPrefix(msg.data, ZBX_ACTIVEDATA_REQ_FREPFIX) {
			msg.passive = true
			msg.IsRequest = true
		} else if bytes.HasPrefix(msg.data, ZBX_ACTIVEDATA_RESP_FREPFIX) {
			msg.passive = true
			msg.IsRequest = true
		} else {
			msg.passive = false
			msg.IsRequest = false
		}
	} else if buf[len(buf)-1] == '\n' {
		msg.IsRequest = true
		msg.passive = true
		msg.data = buf[:len(buf)-1]
		logp.Info("get passive request: %s", string(msg.data))
	} else {
		// the other parts of message
		msg.IsRequest = true
		msg.passive = false
		msg.data = buf
		logp.Info("get a part of active request message: %s", string(msg.data))
	}

	//dir
	dir := applayer.NetOriginalDirection
	if !msg.IsRequest {
		dir = applayer.NetReverseDirection
	}
	msg.Direction = dir

	//size
	msg.Size = uint64(p.buf.BufferConsumed())

	return msg, nil
}

func getDataLength(buf []byte) (int, error) {
	// reserve
	var reverseBuf = make([]byte, 8)
	for i := 0; i < 8; i++ {
		reverseBuf[i] = buf[7-i]
	}

	//convert to number
	var bufLength uint64
	err := binary.Read(bytes.NewBuffer(reverseBuf), binary.BigEndian, &bufLength)
	if err != nil {
		return 0, err
	}

	return int(bufLength), nil
}
