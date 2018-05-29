package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// |length(3)|type(1)|body(length)|
type message interface {
	Encode() []byte
	DecodeFromBytes(data []byte) error
	Type() byte
}

const (
	msgNull byte = iota + 1
	msgMeta
)

type NullMessage struct{}

func (m *NullMessage) Type() byte {
	return msgNull
}

func (m *NullMessage) Encode() []byte {
	return []byte{0x01, 0x00, 0x00, 0x00}
}

func (m *NullMessage) DecodeFromByte(data []byte) error {
	return nil
}

const (
	master byte = iota
	standby
	slave
)

type MetaMessage struct {
	Role  byte   // master, standy, probe
	Epic  uint64 // epic for message checking
	Group string // group(cluster) name
}

func (m *MetaMessage) Encode() []byte {
	var length uint32 = uint32(1 + 8 + len(m.Group) + 1)
	// head
	buf := make([]byte, (3 + length))
	buf[0] = byte(length)
	buf[1] = byte(length >> 8)
	buf[2] = byte(length >> 16)
	// role
	buf[3] = msgMeta
	// Epic
	pos := 4
	epic := make([]byte, 8)
	binary.BigEndian.PutUint64(epic, m.Epic)
	pos += copy(buf[pos:], epic)
	// cluster name
	pos += copy(buf[pos:], []byte(m.Group))
	buf[pos] = 0x00 // this is the delimiter for extended data
	return buf
}

var NotEnouthData = errors.New("not enouth data")

func (m *MetaMessage) DecodeFromBytes(data []byte) error {
	if len(data) < 3 {
		return NotEnouthData
	}

	length := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
	if uint32(len(data)) < length+3 {
		return NotEnouthData
	}
	m.Role = data[3]
	m.Epic = binary.BigEndian.Uint64(data[4:])
	// cluster name
	pos := 3 + 1 + 8
	end := pos + bytes.IndexByte(data[pos:], 0x00)
	m.Group = string(data[pos:end])
	return nil
}

func (m *MetaMessage) Type() byte {
	return msgMeta
}
