package message

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/deatheyes/MysqlProbe/util"
)

// reponse status
const (
	ServerStatusInTrans            uint16 = 0x0001
	ServerStatusAutocommit         uint16 = 0x0002
	ServerStatusResultsExists      uint16 = 0x0008
	ServerStatusNoGoodIndexUsed    uint16 = 0x0010
	ServerStatusNoIndexUsed        uint16 = 0x0020
	ServerStatusCursorExists       uint16 = 0x0040
	ServerStatusLastRowSnet        uint16 = 0x0080
	ServerStatusDbDropped          uint16 = 0x0100
	ServerStatusNoBackslashEscapes uint16 = 0x0200
	ServerStatusMetadataChanged    uint16 = 0x0400
	ServerQueryWasSlow             uint16 = 0x0800
	ServerPsOutParams              uint16 = 0x1000
	ServerStatusInTransReadonly    uint16 = 0x2000
	ServerSessionStateChanged      uint16 = 0x4000
)

// Message is the info of a sql query
type Message struct {
	DB           string `json:"a"` // db name
	SQL          string `json:"b"` // templated sql
	Raw          string `json:"c"` // raw sql
	Err          bool   `json:"d"` // sql process error
	ErrMsg       string `json:"e"` // sql process error message
	Errno        uint16 `json:"f"` // sql process error number
	ServerStatus uint16 `json:"g"` // server response status code
	AffectRows   uint64 `json:"h"` // affect rows
	TimestampReq int64  `json:"i"` // timestamp for request package
	TimestampRsp int64  `json:"j"` // timestamp for response package
	Latency      int64  `json:"k"` // latency in microsecond
	ServerIP     string `json:"l"` // server ip
	ServerPort   uint16 `json:"m"` // server port
	ClientIP     string `json:"n"` // client ip
	ClientPort   uint16 `json:"o"` // client port
}

// HashKey hash(sql) for map
func (m *Message) HashKey() string {
	return strconv.FormatInt(int64(util.Hash(m.SQL)), 10)
}

// SummaryHashKey hash(sql+db) for map, same as Summary.HashKey()
func (m *Message) SummaryHashKey() string {
	return strconv.FormatInt(int64(util.Hash(m.SQL+m.DB)), 10)
}

// Summary is a collection of counters and recoreds
type Summary struct {
	SQL               string     `json:"a"` // SQL template
	DB                string     `json:"b"` // DB name
	SuccessCount      int        `json:"c"` // success query number
	FailedCount       int        `json:"d"` // failed query number
	LastSeen          int64      `json:"e"` // the latest timestamp
	SuccCostUsTotal   int64      `json:"f"` // total cost of success query, we don't caculate average info for the sake of performence
	FailedCostUsTotal int64      `json:"g"` // total cost of failed query, we don't caculate average info for the sake of performence
	NoGoodIndexUsed   int64      `json:"h"` // count of SERVER_STATUS_NO_GOOD_INDEX_USED
	NoIndexUsed       int64      `json:"i"` // count of SERVER_STATUS_NO_INDEX_USED
	QueryWasSlow      int64      `json:"j"` // count of SERVER_QUERY_WAS_SLOW
	Slow              []*Message `json:"k"` // slow querys
	QPS               int64      `json:"l"` // current qps
	Latency           int64      `json:"m"` // average latency
	Key               string     `json:"n"` // hash key for speeding up
}

// Merge another summary into this one
func (s *Summary) Merge(as *Summary) bool {
	if as == nil {
		return false
	}

	s.SuccessCount += as.SuccessCount
	s.FailedCount += as.FailedCount
	if s.LastSeen < as.LastSeen {
		s.LastSeen = as.LastSeen
	}
	s.SuccCostUsTotal += as.SuccCostUsTotal
	s.FailedCostUsTotal += as.FailedCostUsTotal
	s.NoIndexUsed += as.NoIndexUsed
	s.NoGoodIndexUsed += as.NoGoodIndexUsed
	s.QueryWasSlow += as.QueryWasSlow

	s.Slow = append(s.Slow, as.Slow[:]...)

	qps := s.QPS + as.QPS
	if qps != 0 {
		s.Latency = (s.Latency*s.QPS + as.Latency*as.QPS) / (s.QPS + as.QPS)
		s.QPS = qps
	} else {
		s.QPS = 0
		s.Latency = 0
	}
	return true
}

// AddMessage asseble a Message to this summary
func (s *Summary) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}
	s.SQL = m.SQL
	s.DB = m.DB
	// init hash key for speed up
	if len(s.Key) == 0 {
		s.Key = m.SummaryHashKey()
	}

	if m.Err {
		s.FailedCount++
		s.FailedCostUsTotal += m.Latency
	} else {
		s.SuccessCount++
		s.SuccCostUsTotal += m.Latency
	}

	if s.LastSeen < m.TimestampReq {
		s.LastSeen = m.TimestampReq
	}
	// status flags
	if m.ServerStatus&ServerStatusNoIndexUsed != 0 {
		s.NoIndexUsed++
	}
	if m.ServerStatus&ServerStatusNoGoodIndexUsed != 0 {
		s.NoGoodIndexUsed++
	}
	if m.ServerStatus&ServerQueryWasSlow != 0 {
		s.QueryWasSlow++
	}
	// slow query
	if slow {
		s.Slow = append(s.Slow, m)
	}
	return true
}

// ClientSummary extend Summary with client ip
type ClientSummary struct {
	Summary map[string]*DBSummary `json:"a"` // counters
}

func newClientSummary() *ClientSummary {
	return &ClientSummary{
		Summary: make(map[string]*DBSummary),
	}
}

// Merge another summary into this one
func (s *ClientSummary) Merge(as *ClientSummary) bool {
	if as == nil {
		return false
	}

	for k, v := range as.Summary {
		if s.Summary[k] != nil {
			s.Summary[k].Merge(v)
		} else {
			s.Summary[k] = v
		}
	}
	return true
}

// AddMessage merge a Message into this summary
func (s *ClientSummary) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}

	key := m.HashKey()
	v := s.Summary[key]
	if v == nil {
		v = newDBSummary()
		s.Summary[key] = v
	}
	return v.AddMessage(m, slow)
}

// DBSummary group summary by dbname
type DBSummary struct {
	Groups map[string]*Summary `json:"a"`
}

func newDBSummary() *DBSummary {
	return &DBSummary{Groups: make(map[string]*Summary)}
}

// Merge another summary into this one
func (s *DBSummary) Merge(as *DBSummary) bool {
	if as == nil {
		return false
	}

	for k, v := range as.Groups {
		if s.Groups[k] != nil {
			s.Groups[k].Merge(v)
		} else {
			s.Groups[k] = v
		}
	}
	return true
}

// AddMessage merge a Message into this summary
func (s *DBSummary) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}

	g := s.Groups[m.DB]
	if g == nil {
		g = &Summary{}
		s.Groups[m.DB] = g
	}
	return g.AddMessage(m, slow)
}

// ServerSummary group client summary by server ip
type ServerSummary struct {
	Overview  map[string]*DBSummary     `json:"a"` // overview
	Timestamp int64                     `json:"b"` // timestamp for this summary
	Clients   map[string]*ClientSummary `json:"c"` // client summary group
}

func newServerSummary(ip string) *ServerSummary {
	return &ServerSummary{
		Overview:  make(map[string]*DBSummary),
		Clients:   make(map[string]*ClientSummary),
		Timestamp: time.Now().UnixNano(),
	}
}

// Merge another summary into this one
func (s *ServerSummary) Merge(as *ServerSummary) bool {
	if as == nil {
		return false
	}

	for k, v := range as.Overview {
		if s.Overview[k] != nil {
			s.Overview[k].Merge(v)
		} else {
			s.Overview[k] = v
		}
	}

	for k, v := range as.Clients {
		if s.Clients[k] != nil {
			s.Clients[k].Merge(v)
		} else {
			s.Clients[k] = v
		}
	}
	s.Timestamp = time.Now().UnixNano()
	return true
}

// AddMessage merge a Message into this summary
func (s *ServerSummary) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}

	key := m.HashKey()
	if s.Overview[key] == nil {
		s.Overview[key] = newDBSummary()
	}
	s.Overview[key].AddMessage(m, false)

	c := s.Clients[m.ClientIP]
	if c == nil {
		c = newClientSummary()
		s.Clients[m.ClientIP] = c
	}
	return c.AddMessage(m, slow)
}

// Report group captured info by server
type Report struct {
	// Overview map[string]*GlobalSummary `json:"overview"` // overview summary group
	Servers map[string]*ServerSummary `json:"a"` // server summary group
}

// NewReport create a Report object
func NewReport() *Report {
	return &Report{
		// Overview: make(map[string]*GlobalSummary),
		Servers: make(map[string]*ServerSummary),
	}
}

// Merge assemble another Report to this one
func (r *Report) Merge(ar *Report) {
	if r == nil {
		return
	}

	for k, v := range ar.Servers {
		if r.Servers[k] != nil {
			r.Servers[k].Merge(v)
		} else {
			r.Servers[k] = v
		}
	}
}

// AddMessage asseble a Message to this Report
func (r *Report) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}

	s := r.Servers[m.ServerIP]
	if s == nil {
		s = newServerSummary(m.ServerIP)
		r.Servers[m.ServerIP] = s
	}
	return s.AddMessage(m, slow)
}

// DecodeReportFromBytes unmarshal bytes to a Report
func DecodeReportFromBytes(data []byte) (*Report, error) {
	r := &Report{}
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}
	return r, nil
}

// EncodeReportToBytes marshal a Report to bytes
func EncodeReportToBytes(r *Report) ([]byte, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return data, nil
}
