package server

import (
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/yanyu/MysqlProbe/message"
)

const (
	updatePeriod = 10 * time.Second
)

// master collector gathers info from slaves
// slave collector gathers info from probes
type Collector struct {
	clients      map[*Client]bool // connection to slaves
	report       chan<- []byte      // channel to report
	reportIn     chan *message.Report  // channel to gather report
	messageIn    chan *message.Message // channel to gather message
	stop         chan struct{}    // channel to stop collector
	register     chan *Client     // client register channel
	unregister   chan *Client     // client unregister channel
	reportPeriod time.Duration    // period to report and flush merged message
	shutdown     bool             // ture if already stoppted
	sync.Mutex
}

func NewCollector(report chan<- []byte, reportPeriod time.Duration) *Collector {
	return &Collector{
		report:       report,
		reportIn:     make(chan *message.Report),
		messageIn:    make(chan *message.Message),
		register:     make(chan *Client),
		unregister:   make(chan *Client),
		stop:         make(chan struct{}),
		reportPeriod: reportPeriod,
		shutdown:     false,
	}
}

// periodicily update cluster info in case of missed out node
func (c *Collector) update() {
	// TODO: diff the cluster nodes, register the new connection and reactive to the stop channel
}

func (c *Collector) Stop() {
	c.Lock()
	defer c.Unlock()
	if !c.shutdown {
		close(c.stop)
		c.shutdown = true
	}
}

func (c *Collector) ReportIn() chan<- *message.Report {
	return c.reportIn
}

func (c *Collector) MessageIn() chan<- *message.Message {
	return c.messageIn
}

func (c *Collector) Register() chan<- *Client {
	return c.register
}

func (c *Collector) Unregister() chan<- *Client {
	return c.unregister
}

func (c *Collector) ProcessData(data []byte) {
	r, err := message.DecodeReportFromBytes(data);
	if err != nil {
		glog.Warningf("decode report failed: %v", err)
		return
	}
	// gather reports from remote slaves, this is only avaiable on master
	c.reportIn <- r
}

func (c *Collector) Run() {
	glog.V(8).Info("collector start...")
	// TODO: go update()

	report := &message.Report{}
	ticker := time.NewTicker(c.reportPeriod)
	defer ticker.Stop()
	for {
		select {
		case client := <-c.register:
			c.clients[client] = true
		case client := <-c.unregister:
			if _, ok := c.clients[client]; ok {
				delete(c.clients, client)
				close(client.send)
			}
		case r := <-c.reportIn:
			// merge collected reports
			report.Merge(r)
		case m := <-c.messageIn:
			// merge collected messages
			report.AddMessage(m)
		case <-ticker.C:
			// report and flush merged message
			if len(report.Groups) > 0 {
				if data, err := message.EncodeReportToBytes(report); err != nil {
					glog.Warningf("encode report failed: %v", err)
				} else {
					c.report <- data
				}
			}
		case <-c.stop:
			// stop the collector
			for client := range c.clients {
				close(client.send)
			}
			return
		}
	}
}
