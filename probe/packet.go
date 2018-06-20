package probe

import (
	"bufio"
	"encoding/binary"
	"errors"

	"github.com/golang/glog"
	"github.com/xwb1989/sqlparser"

	"github.com/yanyu/MysqlProbe/util"
)

// MysqlBasePacket is the complete packet with head and payload
type MysqlBasePacket struct {
	Len  []byte
	Seq  byte
	Data []byte
}

// ReadMysqlBasePacket read and parse mysql base packet from a reader
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

// MysqlPacket is the interface of MysqlRequestPacket and MysqlResponsePacket
type MysqlPacket interface {
	Seq() uint8
	Stmt() sqlparser.Statement
	Sql() string
	Status() *MysqlResponseStatus
	StmtID() uint32
	CMD() byte
}

// MysqlRequestPacket retains the infomation of query packet
type MysqlRequestPacket struct {
	seq    byte
	cmd    byte
	sql    []byte
	stmtID uint32 // statement id of execute
	stmt   sqlparser.Statement
}

// Seq return the sequence id in head
func (p *MysqlRequestPacket) Seq() uint8 {
	return uint8(p.seq)
}

// Sql return the sql in query packet
func (p *MysqlRequestPacket) Sql() string {
	return string(p.sql)
}

// Stmt return the AST of the sql in query packet
func (p *MysqlRequestPacket) Stmt() sqlparser.Statement {
	return p.stmt
}

// Status return the flag of OK packet
func (p *MysqlRequestPacket) Status() *MysqlResponseStatus {
	return nil
}

// StmtID return the statement id of a execution request
func (p *MysqlRequestPacket) StmtID() uint32 {
	return p.stmtID
}

// CMD return the request command flag
func (p *MysqlRequestPacket) CMD() byte {
	return p.cmd
}

// MysqlResponsePacket retains the infomation about the response packet of query
type MysqlResponsePacket struct {
	seq    byte
	status *MysqlResponseStatus
}

// Seq return the sequence id in head
func (p *MysqlResponsePacket) Seq() uint8 {
	return uint8(p.seq)
}

// Sql return empty string just for interface compatiblility
func (p *MysqlResponsePacket) Sql() string {
	return ""
}

// Stmt return nil just for interface compatiblility
func (p *MysqlResponsePacket) Stmt() sqlparser.Statement {
	return nil
}

// Status return the extend infomation of OK and Err
func (p *MysqlResponsePacket) Status() *MysqlResponseStatus {
	return p.status
}

// StmtID return the statement id of a prepare request; return 0 for interface compatiblility
func (p *MysqlResponsePacket) StmtID() uint32 {
	if p.status != nil {
		return p.status.stmtID
	}
	return 0
}

// CMD return '-' just for interface compatiblility
func (p *MysqlResponsePacket) CMD() byte {
	return '-'
}

var errNotEnouthData = errors.New("not enough data")
var errStmtParsedFailed = errors.New("sql statemnet parsed failed")
var errNotQuery = errors.New("not a query")
var errNotMysqlPacket = errors.New("not a mysql packet")

// MysqlResponseStatus retains parts of the query reponse data
type MysqlResponseStatus struct {
	flag         byte
	affectedRows uint64
	insertID     uint64
	status       uint16
	errno        uint16
	message      string
	stmtID       uint32
}

// DecodeFromBytes unmarshal mysql base packet form bytes
func (p *MysqlBasePacket) DecodeFromBytes(data []byte) error {
	if len(data) < 4 {
		return errNotEnouthData
	}

	p.Len = data[0:3]
	p.Seq = data[3]
	length := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)

	dataEnd := length + 4
	if dataEnd > len(data) {
		glog.Warningf("unexpected data length: %v, required: %v, data: %s", len(data), dataEnd, string(data[5:]))
		return errNotMysqlPacket
	}
	p.Data = data[4:]
	return nil
}

// ParseRequestPacket filter out the query packet
func (p *MysqlBasePacket) ParseRequestPacket() (*MysqlRequestPacket, error) {
	if len(p.Data) < 2 {
		return nil, errNotEnouthData
	}

	switch p.Data[0] {
	case comQuery:
		stmt, err := sqlparser.Parse(string(p.Data[1:]))
		if err != nil || stmt == nil {
			glog.V(8).Infof("possible not a request packet, prase statement failed: %v", err)
			return nil, errStmtParsedFailed
		}
		return &MysqlRequestPacket{seq: p.Seq, cmd: comQuery, sql: p.Data[1:], stmt: stmt}, nil
	case comStmtPrepare:
		return &MysqlRequestPacket{seq: p.Seq, cmd: comStmtPrepare, sql: p.Data[1:]}, nil
	case comStmtExecute:
		// we only care about the statement id currently
		if len(p.Data) < 5 {
			return nil, errNotEnouthData
		}
		stmtID := uint32(p.Data[1]) | uint32(p.Data[2])<<8 | uint32(p.Data[3])<<16 | uint32(p.Data[4])<<24
		return &MysqlRequestPacket{seq: p.Seq, cmd: comStmtExecute, stmtID: stmtID}, nil
	default:
		glog.V(8).Infof("not a query packet: %s", string(p.Data[1:]))
		return nil, errNotQuery
	}
}

// ParseResponsePacket distinguish OK packet, Err packet and Result set Packet
func (p *MysqlBasePacket) ParseResponsePacket(reqType byte) (*MysqlResponsePacket, error) {
	if len(p.Data) < 1 {
		return nil, errNotEnouthData
	}

	switch p.Data[0] {
	case iOK:
		switch reqType {
		case comQuery:
			return p.parseResponseOK()
		case comStmtPrepare:
			return p.parsePrepareOK()
		default:
			return nil, nil
		}
	case iERR:
		return p.parseResponseErr()
	default:
		return &MysqlResponsePacket{seq: p.Seq}, nil
	}
}

func (p *MysqlBasePacket) parsePrepareOK() (*MysqlResponsePacket, error) {
	status := &MysqlResponseStatus{flag: p.Data[0]}
	if len(p.Data) < 5 {
		return nil, errNotEnouthData
	}
	status.stmtID = uint32(p.Data[1]) | uint32(p.Data[2])<<8 | uint32(p.Data[3])<<16 | uint32(p.Data[4])<<24
	return &MysqlResponsePacket{seq: p.Seq, status: status}, nil
}

func (p *MysqlBasePacket) parseResponseOK() (*MysqlResponsePacket, error) {
	var n, m int
	status := &MysqlResponseStatus{flag: p.Data[0]}
	if len(p.Data) == 1 {
		// OK packet without extend info
		return &MysqlResponsePacket{seq: p.Seq, status: status}, nil
	}
	// OK packet with extend info
	status.affectedRows, _, n = util.ReadLengthEncodedInteger(p.Data[1:])
	status.insertID, _, m = util.ReadLengthEncodedInteger(p.Data[1+n:])
	status.status = util.ReadStatus(p.Data[1+n+m : 1+n+m+2])
	return &MysqlResponsePacket{seq: p.Seq, status: status}, nil
}

func (p *MysqlBasePacket) parseResponseErr() (*MysqlResponsePacket, error) {
	status := &MysqlResponseStatus{flag: p.Data[0]}
	status.errno = binary.LittleEndian.Uint16(p.Data[1:3])
	pos := 3
	// SQL State [optional: # + 5bytes string]
	if p.Data[3] == 0x23 {
		//sqlstate := string(data[4 : 4+5])
		pos = 9
	}
	status.message = string(p.Data[pos:])
	return &MysqlResponsePacket{seq: p.Seq, status: status}, nil
}
