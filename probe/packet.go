package probe

import (
	"bufio"
	"errors"

	"github.com/golang/glog"
	"github.com/xwb1989/sqlparser"
)

type MysqlBasePacket struct {
	Len  []byte
	Seq  byte
	Data []byte
}

func ReadMysqlBasePacket(reader *bufio.Reader) (*MysqlBasePacket, error) {
	var err error
	packet := &MysqlBasePacket{Len: make([]byte, 3)}
	if _, err = reader.Read(packet.Len); err != nil {
		return nil, err
	}
	length := int(uint32(packet.Len[0]) | uint32(packet.Len[1])<<8 | uint32(packet.Len[2])<<16)

	if packet.Seq, err = reader.ReadByte(); err != nil {
		return nil, err
	}

	packet.Data = make([]byte, length)
	if _, err = reader.Read(packet.Data); err != nil {
		return nil, err
	}
	return packet, nil
}

type MysqlPacket interface {
	Seq() uint8
	Stmt() sqlparser.Statement
	Sql() string
	Err() error
}

type MysqlRequestPacket struct {
	seq  byte
	cmd  byte
	sql  []byte
	stmt sqlparser.Statement
}

func (p *MysqlRequestPacket) Seq() uint8 {
	return uint8(p.seq)
}

func (p *MysqlRequestPacket) Sql() string {
	return string(p.sql)
}

func (p *MysqlRequestPacket) Stmt() sqlparser.Statement {
	return p.stmt
}

func (p *MysqlRequestPacket) Err() error {
	return nil
}

type MysqlResponsePacket struct {
	seq  byte
	flag byte
	body []byte
}

func (p *MysqlResponsePacket) Seq() uint8 {
	return uint8(p.seq)
}

func (p *MysqlResponsePacket) Sql() string {
	return ""
}

func (p *MysqlResponsePacket) Stmt() sqlparser.Statement {
	return nil
}

var ProcessError = errors.New("server process error")
var NotEnouthDataError = errors.New("not enough data")
var StmtParsedError = errors.New("sql statemnet parsed failed")
var NotQueryError = errors.New("not a query")
var PacketError = errors.New("not a mysql packet")

// TODO: full error message.
func (p *MysqlResponsePacket) Err() error {
	if p.flag == iERR {
		return ProcessError
	}
	return nil
}

func (p *MysqlBasePacket) DecodeFromBytes(data []byte) error {
	if len(data) < 4 {
		return NotEnouthDataError
	}

	p.Len = data[0:3]
	p.Seq = data[3]
	length := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)

	dataEnd := length + 4
	if dataEnd > len(data) {
		glog.Warningf("unexpected data length: %v, required: %v, data: %s", len(data), dataEnd, string(data[5:]))
		return PacketError
	}
	p.Data = data[4:]
	return nil
}

func (p *MysqlBasePacket) ParseRequestPacket() (*MysqlRequestPacket, error) {
	if len(p.Data) < 2 {
		return nil, NotEnouthDataError
	}

	switch p.Data[0] {
	case comQuery:
		stmt, err := sqlparser.Parse(string(p.Data[1:]))
		if err != nil || stmt == nil {
			glog.V(8).Infof("possible not a request packet, prase statement failed: %v", err)
			return nil, StmtParsedError
		}
		return &MysqlRequestPacket{seq: p.Seq, cmd: p.Data[0], sql: p.Data[1:], stmt: stmt}, nil
	default:
		glog.V(8).Infof("not a query packet: %s", string(p.Data[1:]))
		return nil, NotQueryError
	}
}

func (p *MysqlBasePacket) ParseResponsePacket() (*MysqlResponsePacket, error) {
	if len(p.Data) < 1 {
		return nil, NotEnouthDataError
	}
	packet := &MysqlResponsePacket{seq: p.Seq, flag: p.Data[0]}
	// packet OK.
	if len(p.Data) > 1 {
		packet.body = p.Data[1:]
	}
	return packet, nil
}
