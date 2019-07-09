package message

import (
	"bytes"
	"encoding/json"
	"sync"
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

const sampleInterval = 10 * time.Second

// Message is the info of a sql query
type Message struct {
	DB           string  `json:"-"`           // db name
	SQL          string  `json:"-"`           // templated sql
	Raw          string  `json:"c,omitempty"` // raw sql
	Err          bool    `json:"d"`           // sql process error
	ErrMsg       string  `json:"e,omitempty"` // sql process error message
	Errno        uint16  `json:"f,omitempty"` // sql process error number
	ServerStatus uint16  `json:"g"`           // server response status code
	AffectRows   uint64  `json:"h"`           // affect rows
	TimestampReq int64   `json:"i"`           // timestamp for request package
	TimestampRsp int64   `json:"j"`           // timestamp for response package
	Latency      float32 `json:"k"`           // latency in millisecond
	ServerIP     string  `json:"-"`           // server ip
	ServerPort   uint16  `json:"-"`           // server port
	ClientIP     string  `json:"n"`           // client ip
	ClientPort   uint16  `json:"-"`           // client port
	AssemblyKey  string  `json:"-"`           // hash key for assembly
	UnknownExec  bool    `json:"-"`           // execute command without crossponding sql
}

// HashKey hash(sql) for map
func (m *Message) HashKey() (hash string, lastreport time.Time, lastsample time.Time) {
	var ok bool
	if hash, lastreport, lastsample, ok = GlobalSQLHashCache.get(m.SQL); ok {
		return
	}
	return util.Hash(m.SQL), lastreport, lastsample
}

// AssemblyHashKey hash(db+ip+sql) for map, same as AssembleHashKey()
func (m *Message) AssemblyHashKey() (hash string, lastreport time.Time, lastsample time.Time) {
	var ok bool
	key := m.DB + m.ServerIP + m.SQL
	if hash, lastreport, lastsample, ok = GlobalSQLHashCache.get(key); ok {
		return
	}
	return util.Hash(key), lastreport, lastsample
}

// SummaryHashKey is the hash key of the corresponding AssemblySummary
func (m *Message) SummaryHashKey() string {
	return m.DB + "|" + m.ServerIP
}

// Summary is a collection of counters and recoreds
type Summary struct {
	Key             string     `json:"a"`           // hash key of SQL
	SQL             string     `json:"b,omitempty"` // SQL template
	SuccessCount    int        `json:"c"`           // success query number
	FailedCount     int        `json:"d"`           // failed query number
	LastSeen        int64      `json:"e"`           // the latest timestamp
	SuccCostTotal   float32    `json:"f"`           // total cost of success query in millisecond
	FailedCostTotal float32    `json:"g"`           // total cost of failed query in millisecond
	NoGoodIndexUsed int64      `json:"h"`           // count of SERVER_STATUS_NO_GOOD_INDEX_USED
	NoIndexUsed     int64      `json:"i"`           // count of SERVER_STATUS_NO_INDEX_USED
	QueryWasSlow    int64      `json:"j"`           // count of SERVER_QUERY_WAS_SLOW
	QPS             *int       `json:"k,omitempty"` // qps
	AverageLatency  *float32   `json:"l,omitempty"` // average latency in millisecond
	MinLatency      *float32   `json:"m,omitempty"` // min latency in millisecond
	MaxLatency      *float32   `json:"n,omitempty"` // max latency in millisecond
	Latency99       *float32   `json:"o,omitempty"` // latency of 99 quantile in milliseconds
	Slow            []*Message `json:"p,omitempty"` // slow queries
	AssemblyKey     string     `json:"-"`           // hash key for assembly
	Sample          *Message   `json:"r,omitempty"` // one normal query sample
}

// AddMessage assemble a Message to this summary
func (s *Summary) AddMessage(m *Message, slow bool, client bool) bool {
	if m == nil {
		return false
	}

	// init hash key for speed up
	if len(s.Key) == 0 {
		now := time.Now()
		key, lastreport, lastsample := m.HashKey()
		s.Key = key
		if !client {
			if now.Sub(lastreport) > cacheExpiration {
				// report SQL as less as possible
				s.SQL = m.SQL
				lastreport = now
			}

			// take the first message as a sample
			s.Sample = m
			if now.Sub(lastsample) < sampleInterval {
				// Caution: Here modify the 'message'
				s.Sample.Raw = ""
			} else {
				lastsample = now
			}
			// Caution: s.SQL maybe empty here, use m.SQL instead.
			GlobalSQLHashCache.set(m.SQL, key, lastreport, lastsample)
		}
		s.AssemblyKey = m.AssemblyKey
	}

	if m.Err {
		s.FailedCount++
		s.FailedCostTotal += m.Latency
	} else {
		s.SuccessCount++
		s.SuccCostTotal += m.Latency
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

	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	jsonEncoder.Encode(list)
	return bf.Bytes(), nil
}

// UnmarshalJSON interface
func (s *SummaryGroup) UnmarshalJSON(b []byte) error {
	var list []*Summary
	if err := json.Unmarshal(b, &list); err != nil {
		return err
	}

	s.Summary = make(map[string]*Summary)
	for _, v := range list {
		s.Summary[v.Key] = v
	}
	return nil
}

// AddMessage asseble a Message to this summary
func (s *SummaryGroup) AddMessage(m *Message, slow bool, client bool) bool {
	if m == nil {
		return false
	}

	key, _, _ := m.HashKey()
	v := s.Summary[key]
	if v == nil {
		v = &Summary{}
		s.Summary[key] = v
	}
	return v.AddMessage(m, slow, client)
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

	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	jsonEncoder.Encode(list)
	return bf.Bytes(), nil
}

// UnmarshalJSON interface
func (g *ClientSummaryGroup) UnmarshalJSON(b []byte) error {
	var list []*ClientSummaryUnit
	if err := json.Unmarshal(b, &list); err != nil {
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
	return v.AddMessage(m, slow, true)
}

// AssemblySummary group Summary by db and server ip
type AssemblySummary struct {
	DB          string              `json:"a"` // DB name
	IP          string              `json:"b"` // server ip
	LastSeen    int64               `json:"c"` // timestamp
	Group       *SummaryGroup       `json:"d"` // summary group by query
	Client      *ClientSummaryGroup `json:"e"` // summary group by client
	UnknownExec int64               `json:"f"` // counter of execute commands without sql
}

func newAssemblySummary() *AssemblySummary {
	return &AssemblySummary{
		Group:       newSummaryGroup(),
		Client:      newClientSummaryGroup(),
		UnknownExec: 0,
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

	if m.UnknownExec {
		s.UnknownExec++
		return true
	}

	s.Group.AddMessage(m, false, false)
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
	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	jsonEncoder.Encode(r.DB)
	return bf.Bytes(), nil
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

// Reset collect Messages to reuse
func (r *Report) Reset() {
	r.DB = make(map[string]*AssemblySummary)
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
	return r.MarshalJSON()
}

const cacheExpiration = time.Minute * 5
const cacheCleanInterval = time.Minute * 30

// GlobalSQLHashCache is cache the SQL=>Hash(SQL) pairs
var GlobalSQLHashCache = newSQLHashCache()

// SQLHashCache cache the SQL=>MD5(SQL) pairs
type SQLHashCache struct {
	cache map[string]*cacheItem
	sync.RWMutex
}

type cacheItem struct {
	value      string
	lastreport time.Time
	lastsample time.Time
}

func newSQLHashCache() *SQLHashCache {
	c := &SQLHashCache{
		cache: make(map[string]*cacheItem),
	}
	go c.run()
	return c
}

func (c *SQLHashCache) get(key string) (value string, lastreport time.Time, lastsample time.Time, ok bool) {
	c.RLock()
	defer c.RUnlock()

	item, ok := c.cache[key]
	if ok {
		return item.value, item.lastreport, item.lastsample, ok
	}
	return
}

func (c *SQLHashCache) set(key string, value string, report time.Time, sample time.Time) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.cache[key]; !ok {
		c.cache[key] = &cacheItem{value, report, sample}
	} else {
		c.cache[key].value = value
		c.cache[key].lastreport = report
		c.cache[key].lastsample = sample
	}
}

func (c *SQLHashCache) setValue(key string, value string) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.cache[key]; !ok {
		c.cache[key] = &cacheItem{value: value}
	} else {
		c.cache[key].value = value
	}
}

func (c *SQLHashCache) setLastReport(key string, lastreport time.Time) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.cache[key]; !ok {
		c.cache[key] = &cacheItem{lastreport: lastreport}
	} else {
		c.cache[key].lastreport = lastreport
	}
}

func (c *SQLHashCache) setLastSample(key string, lastsample time.Time) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.cache[key]; !ok {
		c.cache[key] = &cacheItem{lastsample: lastsample}
	} else {
		c.cache[key].lastsample = lastsample
	}
}

func (c *SQLHashCache) run() {
	ticker := time.NewTicker(cacheCleanInterval)
	for {
		<-ticker.C

		c.Lock()
		now := time.Now()
		for k, v := range c.cache {
			if now.Sub(v.lastreport) > cacheCleanInterval && now.Sub(v.lastsample) > cacheCleanInterval {
				delete(c.cache, k)
			}
		}
		c.Unlock()
	}
}
