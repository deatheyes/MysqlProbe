package message

import (
	"encoding/json"
	"time"
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
	SQL          string    `json:"sql"`           // templated sql
	Raw          string    `json:"raw"`           // raw sql
	Err          bool      `json:"error"`         // sql process error
	ErrMsg       string    `json:"error_message"` // sql process error message
	Errno        uint16    `json:"errno"`         // sql process error number
	ServerStatus uint16    `json:"server_status"` // server response status code
	AffectRows   uint64    `json:"affect_rows"`   // affect rows
	TimestampReq time.Time `json:"request_time"`  // timestamp for request package
	TimestampRsp time.Time `json:"rsponse_time"`  // timestamp for response package
	Latency      int64     `json:"latency"`       // latency in microsecond
	ServerIP     string    `json:"server_ip"`     // server ip
	ServerPort   uint16    `json:"server_port"`   // server port
	ClientIP     string    `json:"client_ip"`     // client ip
	ClientPort   uint16    `json:"client_port"`   // client port
}

// HashKey for map
func (m *Message) HashKey() string {
	return m.SQL
}

// Summary is a collection of counters and recoreds
type Summary struct {
	SuccessCount      int        `json:"success"`                   // success query number
	FailedCount       int        `json:"failed"`                    // failed query number
	LastSeen          time.Time  `json:"last_seen"`                 // the latest timestamp
	SuccCostUsTotal   int64      `json:"success_total_cost"`        // total cost of success query, we don't caculate average info for the sake of performence
	FailedCostUsTotal int64      `json:"failed_total_cost"`         // total cost of failed query, we don't caculate average info for the sake of performence
	NoGoodIndexUsed   int64      `json:"status_no_good_index_used"` // count of SERVER_STATUS_NO_GOOD_INDEX_USED
	NoIndexUsed       int64      `json:"status_no_index_used"`      // count of SERVER_STATUS_NO_INDEX_USED
	QueryWasSlow      int64      `json:"status_query_was_slow"`     // count of SERVER_QUERY_WAS_SLOW
	Slow              []*Message `json:"slow"`                      // slow querys
}

// Merge another summary into this one
func (s *Summary) Merge(as *Summary) bool {
	if as == nil {
		return false
	}

	s.SuccessCount += as.SuccessCount
	s.FailedCount += as.FailedCount
	if s.LastSeen.Before(as.LastSeen) {
		s.LastSeen = as.LastSeen
	}
	s.SuccCostUsTotal += as.SuccCostUsTotal
	s.FailedCostUsTotal += as.FailedCostUsTotal
	s.NoIndexUsed += as.NoIndexUsed
	s.NoGoodIndexUsed += as.NoGoodIndexUsed
	s.QueryWasSlow += as.QueryWasSlow

	s.Slow = append(s.Slow, as.Slow[:]...)
	return true
}

// AddMessage asseble a Message to this summary
func (s *Summary) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}

	if m.Err {
		s.FailedCount++
		s.FailedCostUsTotal += m.Latency
	} else {
		s.SuccessCount++
		s.SuccCostUsTotal += m.Latency
	}

	if s.LastSeen.Before(m.TimestampReq) {
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
	Summary map[string]*Summary `json:"summary"` // counters
}

func newClientSummary() *ClientSummary {
	return &ClientSummary{
		Summary: make(map[string]*Summary),
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

	v := s.Summary[m.SQL]
	if v == nil {
		v = &Summary{}
		s.Summary[m.SQL] = v
	}
	return v.AddMessage(m, slow)
}

// SQLSummary extends Summary with average values, sunch as QPS and Latency
type SQLSummary struct {
	QPS     int64    `json:"qps"`     // current qps
	Latency int64    `json:"latency"` // average latency
	Summary *Summary `json:"summary"`
}

// Merge another summary into this one
func (s *SQLSummary) Merge(as *SQLSummary) bool {
	if as == nil {
		return false
	}

	qps := s.QPS + as.QPS
	if qps != 0 {
		s.Latency = (s.Latency*s.QPS + as.Latency*as.QPS) / (s.QPS + as.QPS)
		s.QPS = qps
	} else {
		s.QPS = 0
		s.Latency = 0
	}

	return s.Summary.Merge(as.Summary)
}

// AddMessage merge a Message into this summary
func (s *SQLSummary) AddMessage(m *Message) bool {
	if m == nil {
		return false
	}
	return s.Summary.AddMessage(m, false)
}

// ServerSummary group client summary by server ip
type ServerSummary struct {
	Overview  map[string]*SQLSummary    `json:"overview"`  // overview
	Timestamp time.Time                 `json:"timestamp"` // timestamp for this summary
	IP        string                    `json:"ip"`        // server ip
	Clients   map[string]*ClientSummary `json:"clients"`   // client summary group
}

func newServerSummary(ip string) *ServerSummary {
	return &ServerSummary{
		Clients:   make(map[string]*ClientSummary),
		IP:        ip,
		Timestamp: time.Now(),
	}
}

// Merge another summary into this one
func (s *ServerSummary) Merge(as *ServerSummary) bool {
	if as == nil || s.IP != as.IP {
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
	s.Timestamp = time.Now()
	return true
}

// AddMessage merge a Message into this summary
func (s *ServerSummary) AddMessage(m *Message, slow bool) bool {
	if m == nil {
		return false
	}

	if s.Overview[m.SQL] == nil {
		s.Overview[m.SQL] = &SQLSummary{}
	}
	s.Overview[m.SQL].AddMessage(m)

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
	Servers map[string]*ServerSummary `json:"servers"` // server summary group
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
