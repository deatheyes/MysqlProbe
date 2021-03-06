package server

import (
	"net/url"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/golang/snappy"
	"github.com/gorilla/websocket"

	"github.com/deatheyes/MysqlProbe/message"
	"github.com/deatheyes/MysqlProbe/util"
)

const (
	updatePeriod = 10 * time.Second
)

// Collector is responsable for assembling data
// master collector gathers info from slaves
// slave collector gathers info from probes
type Collector struct {
	clients           map[*Client]bool      // connection to slaves
	clientAddrs       map[string]*Client    // connection addr to slaves
	report            chan<- []byte         // channel to report
	reportIn          chan *message.Report  // channel to gather report
	messageIn         chan *message.Message // channel to gather message
	stop              chan struct{}         // channel to stop collector
	register          chan *Client          // client register channel
	unregister        chan *Client          // client unregister channel
	registerAddr      chan string           // cluster node register channel
	unregisterAddr    chan string           // cluster node unregister channel
	rejectConnection  chan bool             // notify to unregister all the connection
	reportPeriod      time.Duration         // period to report and flush merged message
	shutdown          bool                  // ture if already stoppted
	disableConnection bool                  // true if disable to accept connection
	configChanged     bool                  // reload flag
	qps               *util.RollingNumber   // qps caculator
	latency           *util.RollingNumber   // latency caculator
	latencyRange      *util.QuantileGroup   // latency range caculator
	slowThreshold     int64                 // threshold to record slow querys

	sync.Mutex
}

// NewCollector create a collecotr
func NewCollector(report chan<- []byte, reportPeriod time.Duration, slowThreshold int64, disableConnection bool) *Collector {
	qps, _ := util.NewRollingNumber(10000, 100)
	latency, _ := util.NewRollingNumber(10000, 100)
	latencyRange := util.NewQuantileGroup(time.Minute, 1000)
	return &Collector{
		clients:           make(map[*Client]bool),
		clientAddrs:       make(map[string]*Client),
		report:            report,
		reportIn:          make(chan *message.Report, 1000),
		messageIn:         make(chan *message.Message, 1000),
		register:          make(chan *Client),
		unregister:        make(chan *Client),
		registerAddr:      make(chan string),
		unregisterAddr:    make(chan string),
		stop:              make(chan struct{}),
		reportPeriod:      reportPeriod,
		shutdown:          false,
		disableConnection: disableConnection,
		configChanged:     false,
		qps:               qps,
		latency:           latency,
		latencyRange:      latencyRange,
		slowThreshold:     slowThreshold,
	}
}

// UpdateReportPeriod reload the reportPeriod
func (c *Collector) UpdateReportPeriod(reportPeriod time.Duration) {
	if c.reportPeriod != reportPeriod {
		c.reportPeriod = reportPeriod
		c.configChanged = true
	}
}

// DisableConnection clean all client connections
func (c *Collector) DisableConnection() {
	c.rejectConnection <- true
}

// EnableConnection enable and refresh client connections
func (c *Collector) EnableConnection() {
	c.rejectConnection <- false
}

// AddNode add a cluster node specified by addr
func (c *Collector) AddNode(addr string) {
	c.registerAddr <- addr
}

// RemoveNode delete a cluster node specified by addr
func (c *Collector) RemoveNode(addr string) {
	c.unregisterAddr <- addr
}

// node level control
// this function run as fake server
// we need to care about the retry if the client has been unregister but the node not
func (c *Collector) innerupdate() {
	ticker := time.NewTicker(retryPeriod)
	defer ticker.Stop()

	dialer := &websocket.Dialer{HandshakeTimeout: connectTimeout}
	glog.Info("collect innerupdate run...")
	for {
		select {
		case addr := <-c.registerAddr:
			// if this isn't a master, do nothing
			if c.disableConnection {
				glog.Warning("collector has disabled connection")
				continue
			}

			// add a new node
			if _, ok := c.clientAddrs[addr]; !ok {
				glog.V(5).Infof("collector adds node: %v", addr)
				// create client
				u := url.URL{Scheme: "ws", Host: addr, Path: "/collector"}
				conn, _, err := dialer.Dial(u.String(), nil)
				if err != nil {
					glog.Warningf("collector add node %v failed: %v", addr, err)
				} else {
					client := &Client{hub: c, conn: conn, send: make(chan []byte, 256), dead: false, retry: 0, ping: false}
					c.clientAddrs[addr] = client
					// register client
					c.register <- client
					glog.V(5).Infof("collector add node %v done", addr)
					go client.writePump()
					go client.readPump()
				}
			} else {
				glog.Warningf("collector has added node: %v", addr)
			}
		case addr := <-c.unregisterAddr:
			// remove a node if exists
			if client := c.clientAddrs[addr]; client != nil {
				glog.V(5).Infof("collector removes node: %v", addr)
				c.unregister <- client
				delete(c.clientAddrs, addr)
			} else {
				glog.V(5).Infof("collector cannot remove node %v as it is not in the cluster", addr)
			}
		case flag := <-c.rejectConnection:
			if flag {
				// possible node role changed: master -> standby, stop colloect data from nodes
				for addr, client := range c.clientAddrs {
					glog.V(5).Infof("clean client: %v", addr)
					c.unregister <- client
					delete(c.clientAddrs, addr)
				}
				c.disableConnection = true
			} else {
				c.disableConnection = false
			}
		case <-ticker.C:
			// see if any node need an retry
			for k, v := range c.clientAddrs {
				if v.dead {
					glog.V(5).Infof("collector reconnect to node %v retry: %v", k, v.retry)
					u := url.URL{Scheme: "ws", Host: k, Path: "/collector"}
					conn, _, err := dialer.Dial(u.String(), nil)
					if err != nil {
						glog.Warningf("collector reconnect to node %v failed: %v", k, err)
						v.retry++
					} else {
						glog.V(5).Infof("collector reconnect to node %v success", k)
						client := &Client{hub: c, conn: conn, send: make(chan []byte, 256), dead: false, retry: 0, ping: false}
						c.clientAddrs[k] = client
						// register client
						c.register <- client
						glog.V(5).Infof("collector reconnect to node %v done", k)
						go client.writePump()
						go client.readPump()
					}
				}
			}
		case <-c.stop:
			return
		}
	}
}

// Stop shutdown the collector
func (c *Collector) Stop() {
	c.Lock()
	defer c.Unlock()
	if !c.shutdown {
		close(c.stop)
		c.shutdown = true
	}
}

// ReportIn return the input channel of 'Report'
func (c *Collector) ReportIn() chan<- *message.Report {
	return c.reportIn
}

// MessageIn return the input channel of single 'Message'
func (c *Collector) MessageIn() chan<- *message.Message {
	return c.messageIn
}

// Register submit a client to the client pool
func (c *Collector) Register() chan<- *Client {
	return c.register
}

// Unregister remove a client from client pool
func (c *Collector) Unregister() chan<- *Client {
	return c.unregister
}

// ProcessData decode the report received from slaves
func (c *Collector) ProcessData(data []byte) {
	// snappy decode
	dst := make([]byte, len(data)*10)
	if buf, err := snappy.Decode(dst, data); err != nil {
		glog.Warningf("snappy decode failed: %v", err)
	} else {
		r, err := message.DecodeReportFromBytes(buf)
		if err != nil {
			glog.Warningf("decode report failed: %v", err)
			return
		}
		// gather reports from remote slaves, this is only avaiable on master
		c.reportIn <- r
	}
}

// merge collected reports, used by master and standby master
func (c *Collector) assembleReport(target, slice *message.Report) {
	glog.V(8).Info("[collector] merge report")
	// merge report
	target.Merge(slice)
}

// merge collected messages, used by slave
func (c *Collector) assembleMessage(target *message.Report, slice *message.Message) {
	glog.V(7).Infof("[collector] merge message: %v", slice.SQL)
	// merge message
	slow := slice.Latency > float32(c.slowThreshold)
	target.AddMessage(slice, slow)
	// caculate qps
	key := slice.AssemblyKey
	c.qps.Add(key, 1)
	// caculate latency us
	c.latency.Add(key, int64(slice.Latency*1000))
	c.latencyRange.Add(key, int64(slice.Latency*1000))
}

// Run start the main assembling process on message and report level
func (c *Collector) Run() {
	glog.Info("collector start...")
	go c.innerupdate()

	report := message.NewReport()
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
			c.assembleReport(report, r)
			// try to receive more reports
			for i := 0; i < len(c.reportIn); i++ {
				r = <-c.reportIn
				c.assembleReport(report, r)
			}
		case m := <-c.messageIn:
			c.assembleMessage(report, m)
			// try to receive more messages
			for i := 0; i < len(c.messageIn); i++ {
				m = <-c.messageIn
				c.assembleMessage(report, m)
			}
		case <-ticker.C:
			glog.V(7).Info("collector flush report")
			// report and flush merged data
			if c.disableConnection {
				// slave need to caculate the average values
				for _, db := range report.DB {
					for _, s := range db.Group.Summary {
						s.QPS = new(int)
						*s.QPS = int(c.qps.AverageInSecond(s.AssemblyKey))
						sum := c.qps.Sum(s.AssemblyKey)
						if sum != 0 {
							s.AverageLatency = new(float32)
							*s.AverageLatency = float32(c.latency.Sum(s.AssemblyKey)/sum) / 1000
						}
						s.MinLatency = new(float32)
						s.MaxLatency = new(float32)
						s.Latency99 = new(float32)
						min, max, q99 := c.latencyRange.Get(s.AssemblyKey)
						*s.MinLatency = float32(min) / 1000
						*s.MaxLatency = float32(max) / 1000
						*s.Latency99 = float32(q99) / 1000
					}
				}
			}

			// report
			if len(report.DB) > 0 {
				if data, err := message.EncodeReportToBytes(report); err != nil {
					glog.Warningf("[collector] encode report failed: %v", err)
				} else {
					glog.V(8).Infof("[collector] send report %s", string(data))
					// compress and report
					dst := make([]byte, len(data))
					c.report <- snappy.Encode(dst, data)
				}
				report.Reset()
			}

			// see if need to refresh the ticker
			if c.configChanged {
				glog.V(7).Infof("[collector] ticker update: %v", c.reportPeriod)
				ticker.Stop()
				ticker = time.NewTicker(c.reportPeriod)
				c.configChanged = false
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
