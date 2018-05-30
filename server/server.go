package server

import (
	"net/http"
	"time"

	"github.com/golang/glog"
)

type Server struct {
	dispatcher *Dispatcher // dispatcher to serve the client
	collector  *Collector  // collector to gather message
	addr       string      // server addr
}

func NewServer(addr string, slave bool, interval uint16) *Server {
	s := &Server{
		dispatcher: NewDispatcher(),
		addr:       addr,
	}
	s.collector = NewCollector(s.dispatcher.In(), time.Duration(interval)*time.Second, slave)
	return s
}

func (s *Server) Collector() *Collector {
	return s.collector
}

func (s *Server) Dispatcher() *Dispatcher {
	return s.dispatcher
}

func (s *Server) Run() {
	go s.dispatcher.Run()
	go s.collector.Run()

	// start websocket server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serveWs(s.dispatcher, w, r)
	})
	err := http.ListenAndServe(s.addr, nil)
	if err != nil {
		glog.Fatalf("listen and serve failed: %v", err)
	}
}
