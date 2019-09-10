package probe

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/deatheyes/sqlparser"
	"github.com/golang/glog"

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
	seq       byte
	cmd       byte
	sql       []byte
	stmtID    uint32 // statement id of execute
	stmt      sqlparser.Statement
	dbname    string
	queryType byte   // normal | prepare | execute
	queryName string // name of prepare or execute
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
	seq          byte
	flag         byte
	affectedRows uint64
	insertID     uint64
	status       uint16
	errno        uint16
	message      string
	stmtID       uint32
}

// ParseRequestPacket filter out the query packet
func (p *MysqlBasePacket) ParseRequestPacket(packet *MysqlRequestPacket) error {
	if len(p.Data) < 2 {
		return errNotEnouthData
	}
	// clean flag
	packet.queryType = queryNormal
	switch p.Data[0] {
	case comQuery:
		stmt, err := sqlparser.Parse(string(p.Data[1:]))
		if err != nil || stmt == nil {
			glog.V(6).Infof("possible not a request packet, prase statement failed: %v", err)
			return errParsedFailed
		}
		packet.seq = p.Seq()
		if v, ok := stmt.(*sqlparser.Prepare); ok {
			// prepare query
			sql, _ := GenerateSourceQuery(v.Stmt)
			packet.sql = []byte(sql)
			packet.queryType = queryPrepare
			packet.queryName, _ = GenerateSourceQuery(v.Name)
			packet.stmt = stmt
			packet.cmd = comQuery
			return nil
		}
		if v, ok := stmt.(*sqlparser.Execute); ok {
			// execute query
			packet.queryType = queryExecute
			packet.queryName, _ = GenerateSourceQuery(v.Name)
			packet.stmt = stmt
			packet.cmd = comQuery
			return nil
		}
		if v, ok := stmt.(*sqlparser.Use); ok {
			// use dbname
			packet.cmd = comInitDB
			packet.dbname = v.DBName.String()
			return nil
		}
		// normal query
		packet.cmd = comQuery
		packet.sql = p.Data[1:]
		packet.stmt = stmt
		return nil
	case comStmtPrepare:
		packet.seq = p.Seq()
		packet.cmd = comStmtPrepare
		packet.sql = p.Data[1:]
		return nil
	case comStmtExecute:
		// we only care about the statement id currently
		if len(p.Data) < 5 {
			return errNotEnouthData
		}
		packet.seq = p.Seq()
		packet.cmd = comStmtExecute
		packet.stmtID = uint32(p.Data[1]) | uint32(p.Data[2])<<8 | uint32(p.Data[3])<<16 | uint32(p.Data[4])<<24
		return nil
	case comInitDB:
		packet.seq = p.Seq()
		packet.cmd = comInitDB
		packet.dbname = string(p.Data[1:])
		return nil
	default:
		return errParsedFailed
	}
}

// ParseResponsePacket distinguish OK packet, Err packet and Result set Packet
func (p *MysqlBasePacket) ParseResponsePacket(reqType byte, packet *MysqlResponsePacket) (err error) {
	// possible panic while processing length encoding, reover
	defer func() {
		if r := recover(); r != nil {
			glog.Warningf("[recover] parse response failed: %v", r)
			err = r.(error)
		}
	}()

	if len(p.Data) < 1 {
		return errNotEnouthData
	}
	switch reqType {
	case comQuery:
		return p.parseResultSetHeader(packet)
	case comStmtPrepare:
		return p.parsePrepare(packet)
	case comStmtExecute:
		return p.parseResultSetHeader(packet)
	case comInitDB:
		return p.parseResultSetHeader(packet)
	default:
		return errParsedFailed
	}
}

func (p *MysqlBasePacket) parsePrepareOK(packet *MysqlResponsePacket) error {
	packet.flag = p.Data[0]
	if len(p.Data) != 12 {
		return errParsedFailed
	}
	packet.stmtID = binary.LittleEndian.Uint32(p.Data[1:5])
	return nil
}

func (p *MysqlBasePacket) parseOK(packet *MysqlResponsePacket) error {
	var n, m int
	packet.flag = p.Data[0]
	// OK packet with extend info
	packet.affectedRows, _, n = util.ReadLengthEncodedInteger(p.Data[1:])
	packet.insertID, _, m = util.ReadLengthEncodedInteger(p.Data[1+n:])
	packet.status = util.ReadStatus(p.Data[1+n+m : 1+n+m+2])
	return nil
}

func (p *MysqlBasePacket) parseErr(packet *MysqlResponsePacket) error {
	packet.flag = p.Data[0]
	packet.errno = binary.LittleEndian.Uint16(p.Data[1:3])
	pos := 3
	// SQL State [optional: # + 5bytes string]
	if p.Data[3] == 0x23 {
		//sqlstate := string(data[4 : 4+5])
		pos = 9
	}
	packet.message = string(p.Data[pos:])
	return nil
}

func (p *MysqlBasePacket) parseLocalInFile(packet *MysqlResponsePacket) error {
	packet.seq = p.Seq()
	packet.flag = p.Data[0]
	return nil
}

func (p *MysqlBasePacket) parseResultSetHeader(packet *MysqlResponsePacket) error {
	switch p.Data[0] {
	case iOK:
		return p.parseOK(packet)
	case iERR:
		return p.parseErr(packet)
	case iLocalInFile:
		return p.parseLocalInFile(packet)
	}

	// column count
	_, _, n := util.ReadLengthEncodedInteger(p.Data)
	if n-len(p.Data) == 0 {
		packet.seq = p.Seq()
		packet.flag = p.Data[0]
		return nil
	}
	return errParsedFailed
}

func (p *MysqlBasePacket) parsePrepare(packet *MysqlResponsePacket) error {
	switch p.Data[0] {
	case iOK:
		return p.parsePrepareOK(packet)
	case iERR:
		return p.parseErr(packet)
	default:
		return errParsedFailed
	}
}

// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
func (p *MysqlBasePacket) parseHandShakeResponse320(capabilities uint32) (uname string, dbname string, err error) {
	pos := 2 + 3
	// uname string[0x00]
	unameEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
	if unameEndIndex < 0 {
		err = errors.New("[handshake response320] failed in parsing uname")
		return
	}
	uname = string(p.Data[pos : pos+unameEndIndex])
	pos += unameEndIndex + 1

	if capabilities&clientConnectWithDB != 0 {
		// auth response string[0x00]
		authEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
		if authEndIndex < 0 {
			err = errors.New("[handshake response320] failed in parsing auth response")
			return
		}
		pos += authEndIndex + 1

		// dbname string[0x00]
		dbnameEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
		if dbnameEndIndex < 0 {
			err = errors.New("[handshake response320] failed in parsing dbname")
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
		err = fmt.Errorf("[handshake response41] unexpected data length: %v", len(p.Data))
		return
	}

	// uname string[0x00]
	unameEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
	if unameEndIndex < 0 {
		err = errors.New("[handshake response41] failed in parsing uname")
		return
	}
	uname = string(p.Data[pos : pos+unameEndIndex])
	pos += unameEndIndex + 1

	if capabilities&clientPluginAuthLenEncClientData != 0 {
		// plugin auth length encode client data
		pluginInfoLength, _, m := util.ReadLengthEncodedInteger(p.Data[pos:])
		pos += m + int(pluginInfoLength)
		if pos > len(p.Data) {
			err = errors.New("[handshake response41] failed in parsing clientPluginAuth")
			return
		}
	} else if capabilities&clientSecureConn != 0 {
		// client secure connection
		secureInfoLength, _, m := util.ReadLengthEncodedInteger(p.Data[pos:])
		pos += m + int(secureInfoLength)
		if pos > len(p.Data) {
			err = errors.New("[handshake response41] failed in parsing clientSecureConn")
			return
		}
	} else {
		// auth response string[0x00]
		stringEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
		if stringEndIndex < 0 {
			err = errors.New("[handshake response41] failed in parsing auth-response")
			return
		}
		pos += stringEndIndex + 1
	}

	if capabilities&clientConnectWithDB != 0 {
		// dbname string[0x00]
		dbnameEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
		if dbnameEndIndex < 0 {
			err = errors.New("[handshake response41] failed in parsing dbname")
			return
		}
		dbname = string(p.Data[pos : pos+dbnameEndIndex])
		pos += dbnameEndIndex + 1
	}

	if capabilities&clientPluginAuth != 0 {
		// client plugin auth string[0x00]
		pluginAuthEndIndex := bytes.IndexByte(p.Data[pos:], 0x00)
		if pluginAuthEndIndex < 0 {
			err = errors.New("[handshake response41] failed in parsing auth plugin name")
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
	if len(p.Data) < 5 {
		err = errNotEnouthData
		return
	}

	capabilities := uint32(p.Data[0]) | uint32(p.Data[1])<<8
	if capabilities&clientProtocol41 == 0 {
		return p.parseHandShakeResponse320(capabilities)
	}
	capabilities = capabilities | uint32(p.Data[2])<<16 | uint32(p.Data[3])<<24
	return p.parseHandShakeResponse41(capabilities)
}
