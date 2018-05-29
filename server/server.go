package server

import (
	"net/http"
	"time"

	"github.com/golang/glog"
)

const (
	modeMaster = iota
	modeStandby
	modeSlave
)

type Server struct {
	mode int                // server mode
	dispatcher *Dispatcher  // dispatcher to serve the client
	collector *Collector    // collector to gather message
	control chan int        // control channel
	addr string             // server addr
}

// TODO: load config
func NewServer(mode int, addr string) *Server {
	s := &Server{
		mode: mode,
		control: make(chan int),
		dispatcher: NewDispatcher(),
		addr: addr,
	}
	// TODO: load flush period from config
	s.collector = NewCollector(s.dispatcher.In(), 5 * time.Second)
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
