package server

import (
	"fmt"
	"net/http"
	"time"

	"github.com/golang/glog"

	"github.com/yanyu/MysqlProbe/config"
)

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
		dispatcher: NewDispatcher(),
		config:     config,
	}

	flag := true
	if config.Role != NodeRoleSlave {
		flag = false
	}
	s.collector = NewCollector(s.dispatcher.In(), time.Duration(config.Interval)*time.Second, flag)
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
