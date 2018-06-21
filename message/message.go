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
	SQL          string    `json:"sql"`           // templated sql.
	Err          bool      `json:"error"`         // sql process error.
	ErrMsg       string    `json:"error_message"` // sql process error message.
	Errno        uint16    `json:"errno"`         // sql process error number.
	ServerStatus uint16    `json:"server_status"` // server response status code.
	AffectRows   uint64    `json:"affect_rows"`   // affect rows.
	TimestampReq time.Time `json:"request_time"`  // timestamp for request package.
	TimestampRsp time.Time `json:"rsponse_time"`  // timestamp for response package.
}

// MessageGroup is the assembled info of a sql template
type MessageGroup struct {
	// summary
	QPS               int64     `json:"qps"`                       // current qps
	AverageLatency    int64     `json:"avg_latency"`               // average latency
	SuccessCount      int       `json:"success"`                   // success query number
	FailedCount       int       `json:"failed"`                    // failed query number
	LastSeen          time.Time `json:"last_seen"`                 // the latest timestamp
	SuccCostMsTotal   int64     `json:"success_total_cost"`        // total cost of success query, we don't caculate average info for the sake of performence
	FailedCostMsTotal int64     `json:"failed_total_cost"`         // total cost of failed query, we don't caculate average info for the sake of performence
	NoGoodIndexUsed   int64     `json:"status_no_good_index_used"` // count of SERVER_STATUS_NO_GOOD_INDEX_USED
	NoIndexUsed       int64     `json:"status_no_index_used"`      // count of SERVER_STATUS_NO_INDEX_USED
	QueryWasSlow      int64     `json:"status_query_was_slow"`     // count of SERVER_QUERY_WAS_SLOW

	// detail
	//Messages []*Message `json:"messages"` // detail info of the query
}

// Merge assemlbe another message group to this one
func (g *MessageGroup) Merge(ag *MessageGroup) {
	if ag == nil {
		return
	}

	//if len(ag.Messages) == 0 {
	//	return
	//}

	if g.SuccessCount == 0 && g.FailedCount == 0 {
		g.SuccessCount = ag.SuccessCount
		g.FailedCount = ag.FailedCount
		g.LastSeen = ag.LastSeen
		g.SuccCostMsTotal = ag.SuccCostMsTotal
		g.FailedCostMsTotal = ag.FailedCostMsTotal
		g.NoIndexUsed = ag.NoIndexUsed
		g.NoGoodIndexUsed = ag.NoGoodIndexUsed
		g.QueryWasSlow = ag.QueryWasSlow
	} else {
		g.SuccessCount += ag.SuccessCount
		g.FailedCount += ag.FailedCount
		if g.LastSeen.Before(ag.LastSeen) {
			g.LastSeen = ag.LastSeen
		}
		g.SuccCostMsTotal += ag.SuccCostMsTotal
		g.FailedCostMsTotal += ag.FailedCostMsTotal
		g.NoIndexUsed += ag.NoIndexUsed
		g.NoGoodIndexUsed += ag.NoGoodIndexUsed
		g.QueryWasSlow += ag.QueryWasSlow
	}
	//g.Messages = append(g.Messages, ag.Messages[:]...)
}

// Report is the data sent to master or user
type Report struct {
	Groups map[string]*MessageGroup `json:"groups"`
}

// NewReport create a Report
func NewReport() *Report {
	return &Report{Groups: make(map[string]*MessageGroup)}
}

// AddMessage asseble a Message to this Report
func (r *Report) AddMessage(m *Message) {
	g := r.Groups[m.SQL]
	cost := m.TimestampRsp.Sub(m.TimestampReq).Nanoseconds() / 1000000
	if g == nil {
		g = &MessageGroup{}
		if m.Err {
			g.FailedCount = 1
			g.FailedCostMsTotal = cost
		} else {
			g.SuccessCount = 1
			g.SuccCostMsTotal = cost
		}

		g.LastSeen = m.TimestampReq
		//g.Messages = append(g.Messages, m)
		r.Groups[m.SQL] = g
	} else {
		if m.Err {
			g.FailedCostMsTotal = cost
			g.FailedCount++
		} else {
			g.SuccCostMsTotal += cost
			g.SuccessCount++
		}

		if g.LastSeen.Before(m.TimestampReq) {
			g.LastSeen = m.TimestampReq
		}
		//g.Messages = append(g.Messages, m)
	}

	switch m.ServerStatus {
	case ServerStatusNoGoodIndexUsed:
		g.NoGoodIndexUsed++
	case ServerQueryWasSlow:
		g.QueryWasSlow++
	case ServerStatusNoIndexUsed:
		g.NoIndexUsed++
	}
}

// Merge assemble another Report to this one
func (r *Report) Merge(ar *Report) {
	if ar == nil {
		return
	}

	for k, g := range ar.Groups {
		if r.Groups[k] != nil {
			r.Groups[k].Merge(g)
		} else {
			r.Groups[k] = g
		}
	}
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
