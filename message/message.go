package message

import (
	"encoding/json"
	"strconv"

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
	AssemblyKey  string `json:"p"` // hash key for assembly
}

// HashKey hash(sql) for map
func (m *Message) HashKey() string {
	return strconv.FormatInt(int64(util.Hash(m.SQL)), 10)
}

// AssemblyHashKey hash(db+ip+sql) for map, same as AssembleHashKey()
func (m *Message) AssemblyHashKey() string {
	return strconv.FormatInt(int64(util.Hash(m.DB+m.ServerIP+m.SQL)), 10)
}

// SummaryHashKey is the hash key of the corresponding AssemblySummary
func (m *Message) SummaryHashKey() string {
	return m.DB + "|" + m.ServerIP
}

// Summary is a collection of counters and recoreds
type Summary struct {
	Key               string     `json:"a"`           // hash key of SQL
	SQL               string     `json:"b"`           // SQL template
	SuccessCount      int        `json:"c"`           // success query number
	FailedCount       int        `json:"d"`           // failed query number
	LastSeen          int64      `json:"e"`           // the latest timestamp
	SuccCostUsTotal   int64      `json:"f"`           // total cost of success query
	FailedCostUsTotal int64      `json:"g"`           // total cost of failed query
	NoGoodIndexUsed   int64      `json:"h"`           // count of SERVER_STATUS_NO_GOOD_INDEX_USED
	NoIndexUsed       int64      `json:"i"`           // count of SERVER_STATUS_NO_INDEX_USED
	QueryWasSlow      int64      `json:"j"`           // count of SERVER_QUERY_WAS_SLOW
	QPS               *int       `json:"k,omitempty"` // qps
	AverageLatency    *int       `json:"l,omitempty"` // average latency
	MinLatency        *int       `json:"m,omitempty"` // min latency
	MaxLatency        *int       `json:"n,omitempty"` // max latency
	Latency99         *int       `json:"o,omitempty"` // latency of 99 quantile
	Slow              []*Message `json:"p,omitempty"` // slow queries
	AssemblyKey       string     `json:"q,omitempty"` // hash key for assembly
}

// AddMessage asseble a Message to this summary
func (s *Summary) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}
	// init hash key for speed up
	if len(s.Key) == 0 {
		s.Key = m.HashKey()
		s.SQL = m.SQL
		s.AssemblyKey = m.AssemblyKey
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

// SummaryGroup groups summary by sql
type SummaryGroup struct {
	Summary map[string]*Summary
}

func newSummaryGroup() *SummaryGroup {
	return &SummaryGroup{Summary: make(map[string]*Summary)}
}

// MarshalJSON interface
func (s *SummaryGroup) MarshalJSON() ([]byte, error) {
	var list []*Summary
	for _, v := range s.Summary {
		list = append(list, v)
	}
	return json.Marshal(list)
}

// UnmarshalJSON interface
func (s *SummaryGroup) UnmarshalJSON(b []byte) error {
	var list []*Summary
	if err := json.Unmarshal(b, list); err != nil {
		return err
	}

	s.Summary = make(map[string]*Summary)
	for _, v := range list {
		s.Summary[v.Key] = v
	}
	return nil
}

// AddMessage asseble a Message to this summary
func (s *SummaryGroup) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}

	key := m.HashKey()
	v := s.Summary[key]
	if v == nil {
		v = &Summary{}
		s.Summary[key] = v
	}
	return v.AddMessage(m, slow)
}

// ClientSummaryUnit flattens the ClientSummaryGroup
type ClientSummaryUnit struct {
	IP    string        `json:"a"`
	Group *SummaryGroup `json:"b"`
}

// ClientSummaryGroup is a wrapper of map[string]*SummaryGroup
type ClientSummaryGroup struct {
	ClientGroup map[string]*SummaryGroup
}

func newClientSummaryGroup() *ClientSummaryGroup {
	return &ClientSummaryGroup{
		ClientGroup: make(map[string]*SummaryGroup),
	}
}

// MarshalJSON interface
func (g *ClientSummaryGroup) MarshalJSON() ([]byte, error) {
	var list []*ClientSummaryUnit
	for k, v := range g.ClientGroup {
		list = append(list, &ClientSummaryUnit{IP: k, Group: v})
	}
	return json.Marshal(list)
}

// UnmarshalJSON interface
func (g *ClientSummaryGroup) UnmarshalJSON(b []byte) error {
	var list []*ClientSummaryUnit
	if err := json.Unmarshal(b, list); err != nil {
		return err
	}

	g.ClientGroup = make(map[string]*SummaryGroup)
	for _, v := range list {
		if len(v.Group.Summary) == 0 {
			continue
		}
		g.ClientGroup[v.IP] = v.Group
	}
	return nil
}

// AddMessage asseble a Message to this summary
func (g *ClientSummaryGroup) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}

	key := m.ClientIP
	v := g.ClientGroup[key]
	if v == nil {
		v = newSummaryGroup()
		g.ClientGroup[key] = v
	}
	return v.AddMessage(m, slow)
}

// AssemblySummary group Summary by db and server ip
type AssemblySummary struct {
	DB       string              `json:"a"` // DB name
	IP       string              `json:"b"` // server ip
	LastSeen int64               `json:"c"` // timestamp
	Group    *SummaryGroup       `json:"d"` // summary group by query
	Client   *ClientSummaryGroup `json:"e"` // summary group by client
}

func newAssemblySummary() *AssemblySummary {
	return &AssemblySummary{
		Group:  newSummaryGroup(),
		Client: newClientSummaryGroup(),
	}
}

// HashKey for mapping
func (s *AssemblySummary) HashKey() string {
	return s.DB + "|" + s.IP
}

// AddMessage asseble a Message to this summary
func (s *AssemblySummary) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}

	if len(s.IP) == 0 {
		s.DB = m.DB
		s.IP = m.ServerIP
	}

	if s.LastSeen < m.TimestampRsp {
		s.LastSeen = m.TimestampRsp
	}

	s.Group.AddMessage(m, false)
	s.Client.AddMessage(m, slow)

	return true
}

// Report group captured info by DB
type Report struct {
	DB map[string]*AssemblySummary
}

// NewReport create a Report object
func NewReport() *Report {
	return &Report{
		DB: make(map[string]*AssemblySummary),
	}
}

// MarshalJSON interface
func (r *Report) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.DB)
}

// UnmarshalJSON interface
func (r *Report) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &r.DB)
}

// Merge assemble another Report to this one
func (r *Report) Merge(ar *Report) {
	if r == nil {
		return
	}

	// data error if there is a override
	for k, v := range ar.DB {
		r.DB[k] = v
	}
}

// AddMessage asseble a Message to this Report
func (r *Report) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}

	d := r.DB[m.DB]
	if d == nil {
		d = newAssemblySummary()
		r.DB[m.DB] = d
	}
	return d.AddMessage(m, slow)
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
