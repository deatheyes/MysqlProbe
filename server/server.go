package server

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"

	"github.com/deatheyes/MysqlProbe/config"
)

// InitWebsocketEnv set global config for websocket client and server
func InitWebsocketEnv(config *config.Config) {
	if config.Websocket.WriteTimeoutMs > 0 {
		writeTimeout = time.Duration(config.Websocket.WriteTimeoutMs) * time.Millisecond
	}

	if config.Websocket.PingPeriodS > 0 {
		pingPeriod = time.Duration(config.Websocket.PingPeriodS) * time.Second
	}

	if config.Websocket.ReconnectPeriodS > 0 {
		retryPeriod = time.Duration(config.Websocket.ReconnectPeriodS) * time.Second
	}

	if config.Websocket.MaxMessageSize > 0 {
		maxMessageSize = config.Websocket.MaxMessageSize * 1024
	}

	glog.Infof("intialize websocket env done, connect timeout: %v, write timeout: %vms, ping period: %vs, retry period: %vs, max message size: %vk",
		connectTimeout, writeTimeout, pingPeriod, retryPeriod, maxMessageSize)
}

// Server manage all network input and output
type Server struct {
	dispatcher        *Dispatcher       // dispatcher to serve the client
	collector         *Collector        // collector to gather message
	distributedSystem DistributedSystem // distributed system handling topo
	config            *config.Config    // config loaded form file
}

// NewServer create a server by config
func NewServer(config *config.Config) *Server {
	s := &Server{
		dispatcher: NewDispatcher(nil),
		config:     config,
	}

	var pusher *Pusher
	if len(config.Pusher.Servers) == 0 {
		pusher = nil
	} else {
		servers := strings.Split(config.Pusher.Servers, ",")
		pusher = newPusher(servers, config.Pusher.Path, config.Pusher.Preconnect)
	}
	s.dispatcher = NewDispatcher(pusher)

	flag := true
	if config.Role != NodeRoleSlave {
		flag = false
	}
	s.collector = NewCollector(s.dispatcher.In(), time.Duration(config.Interval)*time.Second, config.SlowThresholdMs, flag)
	if config.Cluster.Gossip {
		s.distributedSystem = NewGossipSystem(s, config.Role, config.Cluster.Group, config.Cluster.Port)
	} else {
		s.distributedSystem = NewStaticSystem(s, config.Role, config.Cluster.Group)
	}
	return s
}

func (s *Server) Collector() *Collector {
	return s.collector
}

func (s *Server) Dispatcher() *Dispatcher {
	return s.dispatcher
}

func (s *Server) Run() {
	s.distributedSystem.Run()
	go s.dispatcher.Run()
	go s.collector.Run()

	// websocket
	http.HandleFunc("/collector", func(w http.ResponseWriter, r *http.Request) {
		serveWs(s.dispatcher, w, r)
	})
	// cluster control
	http.HandleFunc("/cluster/listnodes", func(w http.ResponseWriter, r *http.Request) {
		serveListNodes(s.distributedSystem, w, r)
	})
	http.HandleFunc("/cluster/join", func(w http.ResponseWriter, r *http.Request) {
		serveJoin(s.distributedSystem, w, r)
	})
	http.HandleFunc("/cluster/leave", func(w http.ResponseWriter, r *http.Request) {
		serveLeave(s.distributedSystem, w, r)
	})
	http.HandleFunc("/config/update", func(w http.ResponseWriter, r *http.Request) {
		serveConfigUpdate(s.distributedSystem, w, r)
	})
	err := http.ListenAndServe(fmt.Sprintf(":%d", s.config.Port), nil)
	if err != nil {
		glog.Fatalf("listen and serve failed: %v", err)
	}
}
