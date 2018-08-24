package probe

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/golang/glog"
	"github.com/xwb1989/sqlparser"

	"github.com/deatheyes/MysqlProbe/util"
)

var errNotEnouthData = errors.New("not enough data")
var errParsedFailed = errors.New("parsed failed")

// MysqlBasePacket is the complete packet with header and payload
type MysqlBasePacket struct {
	Header []byte // header
	Data   []byte // body
}

// DecodeFromBytes try to decode the first packet from bytes
func (p *MysqlBasePacket) DecodeFromBytes(data []byte) (int, error) {
	if len(data) < 4 {
		return 0, errNotEnouthData
	}

	p.Header = data[0:4]
	length := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)

	if length+4 > len(data) {
		// this packet may not be the first packet of the request or response
		glog.V(8).Infof("unexpected data length: %v, required: %v, data: %s", len(data), length+4, data)
		return 0, errNotEnouthData
	}
	p.Data = data[4 : length+4]
	return length + 4, nil
}

// Seq return the Sequence id
func (p *MysqlBasePacket) Seq() byte {
	return p.Header[3]
}

// Length retrun the body length
func (p *MysqlBasePacket) Length() int {
	return int(uint32(p.Header[0]) | uint32(p.Header[1])<<8 | uint32(p.Header[2])<<16)
}

// MysqlRequestPacket retains the infomation of query packet
type MysqlRequestPacket struct {
	seq    byte
	cmd    byte
	sql    []byte
	stmtID uint32 // statement id of execute
	stmt   sqlparser.Statement
	dbname string
}

// Seq return the sequence id in head
func (p *MysqlRequestPacket) Seq() uint8 {
	return uint8(p.seq)
}

// SQL return the sql in query packet
func (p *MysqlRequestPacket) SQL() string {
	return string(p.sql)
}

// Stmt return the AST of the sql in query packet
func (p *MysqlRequestPacket) Stmt() sqlparser.Statement {
	return p.stmt
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

// Seq return the sequence id in header
func (p *MysqlResponsePacket) Seq() uint8 {
	return uint8(p.seq)
}

// Stmt return nil just for interface compatiblility
func (p *MysqlResponsePacket) Stmt() sqlparser.Statement {
	return nil
}

// Status return the extend infomation of OK and Err
func (p *MysqlResponsePacket) Status() *MysqlResponseStatus {
	return p.status
}

// StmtID return the statement id of a prepare request; return 0 for compatiblility
func (p *MysqlResponsePacket) StmtID() uint32 {
	if p.status != nil {
		return p.status.stmtID
	}
	return 0
}

// MysqlResponseStatus retains parts of the query response data
type MysqlResponseStatus struct {
	flag         byte
	affectedRows uint64
	insertID     uint64
	status       uint16
	errno        uint16
	message      string
	stmtID       uint32
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
			glog.V(6).Infof("possible not a request packet, prase statement failed: %v", err)
			return nil, errParsedFailed
		}
		return &MysqlRequestPacket{seq: p.Seq(), cmd: comQuery, sql: p.Data[1:], stmt: stmt}, nil
	case comStmtPrepare:
		return &MysqlRequestPacket{seq: p.Seq(), cmd: comStmtPrepare, sql: p.Data[1:]}, nil
	case comStmtExecute:
		// we only care about the statement id currently
		if len(p.Data) < 5 {
			return nil, errNotEnouthData
		}
		stmtID := uint32(p.Data[1]) | uint32(p.Data[2])<<8 | uint32(p.Data[3])<<16 | uint32(p.Data[4])<<24
		return &MysqlRequestPacket{seq: p.Seq(), cmd: comStmtExecute, stmtID: stmtID}, nil
	case comInitDB:
		return &MysqlRequestPacket{seq: p.Seq(), cmd: comInitDB, dbname: string(p.Data[1:])}, nil
	default:
		return nil, errParsedFailed
	}
}

// ParseResponsePacket distinguish OK packet, Err packet and Result set Packet
func (p *MysqlBasePacket) ParseResponsePacket(reqType byte) (_ *MysqlResponsePacket, err error) {
	// possible panic while processing length encoding, reover
	defer func() {
		if r := recover(); r != nil {
			glog.Warningf("[recover] parse response failed: %v", r)
			err = r.(error)
		}
	}()

	if len(p.Data) < 1 {
		return nil, errNotEnouthData
	}
	switch reqType {
	case comQuery:
		return p.parseResultSetHeader()
	case comStmtPrepare:
		return p.parsePrepare()
	case comStmtExecute:
		return p.parseResultSetHeader()
	default:
		return nil, errParsedFailed
	}
}

func (p *MysqlBasePacket) parsePrepareOK() (*MysqlResponsePacket, error) {
	status := &MysqlResponseStatus{flag: p.Data[0]}
	if len(p.Data) != 12 {
		return nil, errParsedFailed
	}
	status.stmtID = binary.LittleEndian.Uint32(p.Data[1:5])
	return &MysqlResponsePacket{seq: p.Seq(), status: status}, nil
}

func (p *MysqlBasePacket) parseOK() (*MysqlResponsePacket, error) {
	var n, m int
	status := &MysqlResponseStatus{flag: p.Data[0]}
	// OK packet with extend info
	status.affectedRows, _, n = util.ReadLengthEncodedInteger(p.Data[1:])
	status.insertID, _, m = util.ReadLengthEncodedInteger(p.Data[1+n:])
	status.status = util.ReadStatus(p.Data[1+n+m : 1+n+m+2])
	return &MysqlResponsePacket{seq: p.Seq(), status: status}, nil
}

func (p *MysqlBasePacket) parseErr() (*MysqlResponsePacket, error) {
	status := &MysqlResponseStatus{flag: p.Data[0]}
	status.errno = binary.LittleEndian.Uint16(p.Data[1:3])
	pos := 3
	// SQL State [optional: # + 5bytes string]
	if p.Data[3] == 0x23 {
		//sqlstate := string(data[4 : 4+5])
		pos = 9
	}
	status.message = string(p.Data[pos:])
	return &MysqlResponsePacket{seq: p.Seq(), status: status}, nil
}

func (p *MysqlBasePacket) parseLocalInFile() (*MysqlResponsePacket, error) {
	return &MysqlResponsePacket{seq: p.Seq(), status: &MysqlResponseStatus{flag: p.Data[0]}}, nil
}

func (p *MysqlBasePacket) parseResultSetHeader() (*MysqlResponsePacket, error) {
	switch p.Data[0] {
	case iOK:
		return p.parseOK()
	case iERR:
		return p.parseErr()
	case iLocalInFile:
		return p.parseLocalInFile()
	}

	// column count
	_, _, n := util.ReadLengthEncodedInteger(p.Data)
	if n-len(p.Data) == 0 {
		return &MysqlResponsePacket{seq: p.Seq(), status: &MysqlResponseStatus{flag: p.Data[0]}}, nil
	}
	return nil, errParsedFailed
}

func (p *MysqlBasePacket) parsePrepare() (*MysqlResponsePacket, error) {
	switch p.Data[0] {
	case iOK:
		return p.parsePrepareOK()
	case iERR:
		return p.parseErr()
	default:
		return nil, errParsedFailed
	}
}

// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
func (p *MysqlBasePacket) parseHandShakeResponse320(capabilities uint32) (uname string, dbname string, err error) {
	pos := 2 + 3
	if len(p.Data) < pos {
		err = errNotEnouthData
		glog.Warningf("[handshake response320] unexpected data length: %v", len(p.Data))
		return
	}

	// uname string[0x00]
	unameEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
	if unameEndIndex < 0 {
		err = errNotEnouthData
		glog.Warning("[handshake response320] failed in parsing uname")
		return
	}
	uname = string(p.Data[pos : pos+unameEndIndex])
	pos += unameEndIndex + 1

	if capabilities&clientConnectWithDB != 0 {
		// auth response string[0x00]
		authEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
		if authEndIndex < 0 {
			err = errNotEnouthData
			glog.Warning("[handshake response320] failed in parsing auth response")
			return
		}
		pos += authEndIndex + 1

		// dbname string[0x00]
		dbnameEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
		if dbnameEndIndex < 0 {
			err = errNotEnouthData
			glog.Warning("[handshake response320] failed in parsing dbname")
			return
		}
		dbname = string(p.Data[pos : pos+dbnameEndIndex])
		pos += dbnameEndIndex + 1
	}
	// TODO: parse auth-response[EOF], which we don't need currently
	return
}

// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
func (p *MysqlBasePacket) parseHandShakeResponse41(capabilities uint32) (uname string, dbname string, err error) {
	pos := 4 + 4 + 1 + 23
	if len(p.Data) < pos {
		err = errNotEnouthData
		glog.Warningf("[handshake response41] unexpected data length: %v", len(p.Data))
		return
	}

	// uname string[0x00]
	unameEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
	if unameEndIndex < 0 {
		err = errNotEnouthData
		glog.Warning("[handshake response41] failed in parsing uname")
		return
	}
	uname = string(p.Data[pos : pos+unameEndIndex])
	pos += unameEndIndex + 1

	if capabilities&clientPluginAuthLenEncClientData != 0 {
		// plugin auth length encode client data
		pluginInfoLength, _, m := util.ReadLengthEncodedInteger(p.Data[pos:])
		pos += m + int(pluginInfoLength)
		if pos > len(p.Data) {
			glog.Warning("[handshake response41] failed in parsing clientPluginAuth")
			err = errNotEnouthData
			return
		}
	} else if capabilities&clientSecureConn != 0 {
		// client secure connection
		secureInfoLength, _, m := util.ReadLengthEncodedInteger(p.Data[pos:])
		pos += m + int(secureInfoLength)
		if pos > len(p.Data) {
			glog.Warning("[handshake response41] failed in parsing clientSecureConn")
			err = errNotEnouthData
			return
		}
	} else {
		// auth response string[0x00]
		stringEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
		if stringEndIndex < 0 {
			glog.Warning("[handshake response41] failed in parsing auth-response")
			err = errNotEnouthData
			return
		}
		pos += stringEndIndex + 1
	}

	if capabilities&clientConnectWithDB != 0 {
		// dbname string[0x00]
		dbnameEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
		if dbnameEndIndex < 0 {
			glog.Warning("[handshake response41] failed in parsing dbname")
			err = errNotEnouthData
			return
		}
		dbname = string(p.Data[pos : pos+dbnameEndIndex])
		pos += dbnameEndIndex + 1
	}

	if capabilities&clientPluginAuth != 0 {
		// client plugin auth string[0x00]
		pluginAuthEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
		if pluginAuthEndIndex < 0 {
			glog.Warning("[handshake response41] failed in parsing auth plugin name")
			err = errNotEnouthData
			return
		}
		pos += pluginAuthEndIndex + 1
	}

	// TODO: parse client connect attributes, which we don't need currently
	/*if capabilities&clientConnectAttrs != 0 {
	  }*/
	return
}

func (p *MysqlBasePacket) parseHandShakeResponse() (uname string, dbname string, err error) {
	capabilities := uint32(p.Data[0]) | uint32(p.Data[1])<<8 | uint32(p.Data[2])<<16 | uint32(p.Data[3])<<24
	if capabilities&clientProtocol41 == 0 {
		return p.parseHandShakeResponse320(capabilities)
	}
	return p.parseHandShakeResponse41(capabilities)
}
