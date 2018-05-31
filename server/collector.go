package server

import (
	"net/url"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/gorilla/websocket"

	"github.com/yanyu/MysqlProbe/message"
)

const (
	updatePeriod = 10 * time.Second
)

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

	sync.Mutex
}

func NewCollector(report chan<- []byte, reportPeriod time.Duration, disableConnection bool) *Collector {
	return &Collector{
		clients:           make(map[*Client]bool),
		clientAddrs:       make(map[string]*Client),
		report:            report,
		reportIn:          make(chan *message.Report),
		messageIn:         make(chan *message.Message),
		register:          make(chan *Client),
		unregister:        make(chan *Client),
		stop:              make(chan struct{}),
		reportPeriod:      reportPeriod,
		shutdown:          false,
		disableConnection: disableConnection,
	}
}

func (c *Collector) DisableConnection() {
	c.rejectConnection <- true
}

func (c *Collector) EnableConnection() {
	c.rejectConnection <- false
}

func (c *Collector) AddNode(addr string) {
	c.registerAddr <- addr
}

func (c *Collector) RemoveNode(addr string) {
	c.unregisterAddr <- addr
}

// node level controll
// this function run as fake server
// we need to care about the retry if the client has been unregister but the node not
func (c *Collector) innerupdate() {
	ticker := time.NewTicker(retryPerid)
	defer ticker.Stop()

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
				conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
				if err != nil {
					glog.Warningf("collector add node %v failed: %v", addr, err)
				} else {
					client := &Client{hub: c, conn: conn, send: make(chan []byte, 256), dead: false, retry: 0}
					c.clientAddrs[addr] = client
					// register client
					c.register <- client
					glog.V(5).Infof("collector add node %v done", addr)
					go client.writePump()
					go client.readPump()
				}
			} else {
				glog.Warningf("collector has added noed: %v", addr)
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
					conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
					if err != nil {
						glog.Warningf("collector reconnect to node %v failed: %v", k, err)
						v.retry++
					} else {
						glog.V(5).Infof("collector reconnect to node %v success", k)
						client := &Client{hub: c, conn: conn, send: make(chan []byte, 256), dead: false, retry: 0}
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
	r, err := message.DecodeReportFromBytes(data)
	if err != nil {
		glog.Warningf("decode report failed: %v", err)
		return
	}
	// gather reports from remote slaves, this is only avaiable on master
	c.reportIn <- r
}

func (c *Collector) Run() {
	glog.V(8).Info("collector start...")
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
			// merge collected reports, used by master and standby master
			report.Merge(r)
		case m := <-c.messageIn:
			// merge collected messages, used by slave
			report.AddMessage(m)
		case <-ticker.C:
			// report and flush merged message
			if len(report.Groups) > 0 {
				if data, err := message.EncodeReportToBytes(report); err != nil {
					glog.Warningf("encode report failed: %v", err)
				} else {
					c.report <- data
				}
				report = message.NewReport()
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
