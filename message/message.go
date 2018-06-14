package message

import (
	"encoding/json"
	"time"
)

// Message is the info of a sql query
type Message struct {
	SQL          string    `json:"sql"`           // templated sql.
	Err          bool      `json:"error"`         // sql process error.
	ErrMsg       string    `json:"error_message"` // sql process error message.
	TimestampReq time.Time `json:"request_time"`  // timestamp for request package.
	TimestampRsp time.Time `json:"rsponse_time"`  // timestamp for response package.
}

// MessageGroup is the assembled info of a sql template
type MessageGroup struct {
	// summary
	QPS               int64     `json:"qps"`                // current qps
	Overhead          int64     `json:"overhead"`           // average overhead
	SuccessCount      int       `json:"success"`            // success query number
	FailedCount       int       `json:"failed"`             // failed query number
	LastSeen          time.Time `json:"last_seen"`          // the latest timestamp
	SuccCostMsTotal   int64     `json:"success_total_cost"` // total cost of success query, we don't caculate average info for the sake of performence
	FailedCostMsTotal int64     `json:"failed_total_cost"`  // total cost of failed query, we don't caculate average info for the sake of performence
	// detail
	Messages []*Message `json:"messages"` // detail info of the query
}

// Merge assemlbe another message group to this one
func (g *MessageGroup) Merge(ag *MessageGroup) {
	if ag == nil {
		return
	}

	if len(ag.Messages) == 0 {
		return
	}

	if len(g.Messages) == 0 {
		g.SuccessCount = ag.SuccessCount
		g.FailedCount = ag.FailedCount
		g.LastSeen = ag.LastSeen
		g.SuccCostMsTotal = ag.SuccCostMsTotal
		g.FailedCostMsTotal = ag.FailedCostMsTotal
	} else {
		g.SuccessCount += ag.SuccessCount
		g.FailedCount += ag.FailedCount
		if g.LastSeen.Before(ag.LastSeen) {
			g.LastSeen = ag.LastSeen
		}
		g.SuccCostMsTotal += ag.SuccCostMsTotal
		g.FailedCostMsTotal += ag.FailedCostMsTotal
	}
	g.Messages = append(g.Messages, ag.Messages[:]...)
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
		g.Messages = append(g.Messages, m)
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
		g.Messages = append(g.Messages, m)
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
