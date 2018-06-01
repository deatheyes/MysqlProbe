package server

import (
	"fmt"
	"net/http"
	"time"

	"github.com/golang/glog"
)

type Server struct {
	dispatcher        *Dispatcher       // dispatcher to serve the client
	collector         *Collector        // collector to gather message
	distributedSystem DistributedSystem // distributed system handling topo
	port              uint16            // server port
}

func NewServer(port uint16, role string, interval uint16, cluster bool, group string) *Server {
	s := &Server{
		dispatcher: NewDispatcher(),
		port:       port,
	}

	flag := true
	if role != "slave" {
		flag = false
	}
	s.collector = NewCollector(s.dispatcher.In(), time.Duration(interval)*time.Second, flag)
	if cluster {
		s.distributedSystem = NewGossipSystem(s, role, group)
	} else {
		s.distributedSystem = NewStaticSystem(s, role, group)
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

	// start websocket server
	http.HandleFunc("/collector", func(w http.ResponseWriter, r *http.Request) {
		serveWs(s.dispatcher, w, r)
	})
	http.HandleFunc("/cluster/listnodes", func(w http.ResponseWriter, r *http.Request) {
		serveListNodes(s.distributedSystem, w, r)
	})
	http.HandleFunc("/cluster/join", func(w http.ResponseWriter, r *http.Request) {
		serveJoin(s.distributedSystem, w, r)
	})
	http.HandleFunc("/cluster/leave", func(w http.ResponseWriter, r *http.Request) {
		serveLeave(s.distributedSystem, w, r)
	})
	err := http.ListenAndServe(fmt.Sprintf(":%d", s.port), nil)
	if err != nil {
		glog.Fatalf("listen and serve failed: %v", err)
	}
}
