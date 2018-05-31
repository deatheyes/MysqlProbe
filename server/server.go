package server

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/yanyu/MysqlProbe/cluster"
)

// cluster info reported to user
type Topo struct {
	Role  string                   `json:"role"`  // role of this node
	Nodes map[string]*cluster.Node `json:"nodes"` // cluster Node
}

type Server struct {
	dispatcher *Dispatcher // dispatcher to serve the client
	collector  *Collector  // collector to gather message
	addr       string      // server addr
	topo       *Topo       // cluster topology
	sync.RWMutex
}

func NewServer(addr string, slave bool, interval uint16) *Server {
	t := &Topo{Nodes: make(map[string]*cluster.Node)}
	if slave {
		t.Role = "master"
	} else {
		t.Role = "slave"
	}

	s := &Server{
		dispatcher: NewDispatcher(),
		addr:       addr,
		topo:       t,
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
	http.HandleFunc("/collector", func(w http.ResponseWriter, r *http.Request) {
		serveWs(s.dispatcher, w, r)
	})
	http.HandleFunc("/cluster", func(w http.ResponseWriter, r *http.Request) {
		s.handleCluster(w, r)
	})
	err := http.ListenAndServe(s.addr, nil)
	if err != nil {
		glog.Fatalf("listen and serve failed: %v", err)
	}
}

func (s Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	s.RLock()
	defer s.RUnlock()

	data, err := json.Marshal(s.topo)
	if err != nil {
		glog.Warningf("server handle cluster info failed: %v", err)
		return
	}
	io.WriteString(w, string(data))
}

// cluster delegate
func (s *Server) NotifyMyRoleChanged(oldRole string, newRole string) {
	glog.V(5).Infof("my role change form %v to %v", oldRole, newRole)

	s.Lock()
	defer s.Unlock()

	s.topo.Role = newRole
	switch newRole {
	case "master":
		// start collect data from nodes
		s.collector.EnableConnection()
		// create connection for all slaves
		for _, n := range s.topo.Nodes {
			if n.Role == "slave" {
				s.collector.AddNode(n.Addr)
			}
		}
	case "standby":
		// stop collect data from nodes
		s.collector.DisableConnection()
	default:
		s.collector.DisableConnection()
	}
}

func (s *Server) NotifyJoin(node *cluster.Node) {
	glog.V(5).Infof("new node join: %s", node)

	s.Lock()
	defer s.Unlock()

	if node.Role == "slave" && s.topo.Role == "master" {
		s.collector.AddNode(node.Addr)
	}

	// update topo
	// TODO: what if node exists
	s.topo.Nodes[node.Addr] = node
}

func (s *Server) NotifyUpdate(node *cluster.Node) {
	glog.V(5).Infof("node update: %s", node)

	s.Lock()
	defer s.Unlock()

	// update topo
	// TODO: what if node not exists
	s.topo.Nodes[node.Addr] = node
}

func (s *Server) NotifyLeave(node *cluster.Node) {
	glog.V(5).Infof("node leave: %s", node)

	s.Lock()
	defer s.Unlock()

	if node.Role == "slave" && s.topo.Role == "master" {
		s.collector.RemoveNode(node.Addr)
	}

	// update topo
	delete(s.topo.Nodes, node.Addr)
}

/*func (s *Server) NotifyRefresh(nodes []*cluster.Node) {
	// periodicity diff the cluster
}*/
