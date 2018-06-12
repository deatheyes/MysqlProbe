package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/hashicorp/memberlist"
	"github.com/pborman/uuid"

	localConfig "github.com/yanyu/MysqlProbe/config"
)

const (
	NodeRoleSlave   = "slave"
	NodeRoleMaster  = "master"
	NodeRoleStandby = "standby"
)

const (
	SeedsFileName = "seeds.yaml"
)

type Broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

func (b *Broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *Broadcast) Message() []byte {
	return b.msg
}

func (b *Broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

type MetaData struct {
	Role       string `json:"role"`        // master, standy, probe
	Epic       uint64 `json:"epic"`        // epic for message checking
	Group      string `json:"group"`       // group(cluster) name
	ServerPort uint16 `json:"server_port"` // dispatcher port
}

type Node struct {
	Name       string    `json:"name"`
	IP         string    `json:"ip"`
	GossipPort uint16    `json:"gossip_port"`
	Meta       *MetaData `json:"meta"`
}

type DistributedSystem interface {
	Run()
	Join(addr string) error
	Remove(addr string) error
	Leave() error
	ListNodes() ([]byte, error)
}

// auto failure dectect distributed system
type GossipSystem struct {
	server        *Server   // owner
	meta          *MetaData // local meta
	master        string    // master of current cluster
	seeds         []string  // seeds to join
	list          *memberlist.Memberlist
	broadcasts    *memberlist.TransmitLimitedQueue
	localIp       string
	localPort     uint16
	port          uint16
	name          string // node name in the cluster
	seedsFilePath string // seeds file for boot, usually used when restart

	sync.Mutex
}

func NewGossipSystem(server *Server, role string, group string, port uint16) *GossipSystem {
	dir := path.Dir(server.config.Path)
	seedsFilePath := path.Join(dir, SeedsFileName)
	return &GossipSystem{
		server:        server,
		meta:          &MetaData{Role: role, Epic: 0, Group: group, ServerPort: server.config.Port},
		port:          port,
		seedsFilePath: seedsFilePath,
	}
}

func (d *GossipSystem) writeSeeds() {
	if d.list == nil || d.list.Members() == nil {
		// list is not inited
		return
	}

	var addrs []string
	for _, m := range d.list.Members() {
		addrs = append(addrs, fmt.Sprintf("%v:%v", m.Addr.String(), m.Port))
	}

	seeds := &localConfig.Seeds{Addrs: addrs, Epic: d.meta.Epic, Name: d.name, Role: d.meta.Role}

	if err := localConfig.SeedsToFile(seeds, d.seedsFilePath); err != nil {
		glog.Warningf("write seeds failed: %v", err)
	}
	glog.V(7).Infof("write seeds to %v done", d.seedsFilePath)
}

func (d *GossipSystem) Run() {
	hostname, _ := os.Hostname()
	config := memberlist.DefaultWANConfig()
	config.Delegate = d
	config.Events = d
	config.Alive = d
	config.BindPort = int(d.port)
	config.Name = hostname + "-" + uuid.NewUUID().String()

	if d.meta.Role == NodeRoleMaster {
		d.master = config.Name
	} else {
		d.master = ""
	}

	// try load seeds from file
	if _, err := os.Stat(d.seedsFilePath); os.IsNotExist(err) {
		glog.Info("no seeds to start cluster")
		d.name = config.Name
		list, err := memberlist.Create(config)
		if err != nil {
			glog.Fatalf("init distributed system failed: %v", err)
		}
		d.list = list
		n := d.list.LocalNode()
		d.localIp = n.Addr.String()
		d.localPort = uint16(n.Port)
	} else {
		// override the config with seeds
		seeds, err := localConfig.SeedsFromFile(d.seedsFilePath)
		if err != nil {
			glog.Fatalf("load seeds failed: %v", err)
			return
		}

		switch seeds.Role {
		case NodeRoleMaster, NodeRoleSlave, NodeRoleStandby:
			d.meta.Role = seeds.Role
		default:
			glog.Warningf("unkonwn role: %v", seeds.Role)
		}

		d.meta.Epic = seeds.Epic

		if len(seeds.Name) != 0 {
			config.Name = seeds.Name
			d.name = seeds.Name
		}

		list, err := memberlist.Create(config)
		if err != nil {
			glog.Fatalf("init distributed system with seeds %v failed: %v", seeds, err)
		}
		d.list = list
		n := d.list.LocalNode()
		d.localIp = n.Addr.String()
		d.localPort = uint16(n.Port)

		// try to join the cluster recorded by seeds
		if len(seeds.Addrs) > 0 {
			glog.Infof("init distributed system by addresses: %v", seeds.Addrs)
			if _, err := d.list.Join(seeds.Addrs); err != nil {
				glog.Fatalf("join seeds failed: %v", err)
				return
			}
		}
	}

	d.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return d.list.NumMembers()
		},
		RetransmitMult: 3,
	}
	glog.Infof("init distributed system done, local member %s:%d, name: %v", d.localIp, d.localPort, d.name)
}

var UnexpectedGroupError = errors.New("unexpected group")

// delegate
func (d *GossipSystem) NodeMeta(limit int) []byte {
	data, err := json.Marshal(d.meta)
	if err != nil {
		glog.Warningf("marshal meta failed: %v", err)
		return nil
	}
	return data
}

func (d *GossipSystem) NotifyMsg(b []byte) {}

func (d *GossipSystem) GetBroadcasts(overhead, limit int) [][]byte {
	return d.broadcasts.GetBroadcasts(overhead, limit)
}

func (d *GossipSystem) LocalState(join bool) []byte {
	return nil
}

func (d *GossipSystem) MergeRemoteState(buf []byte, join bool) {

}

func (d *GossipSystem) OnRoleChanged(oldRole, newRole string) {
	glog.V(5).Infof("my role change form %v to %v", oldRole, newRole)
	switch newRole {
	case NodeRoleMaster:
		// start collect data from nodes
		d.server.collector.EnableConnection()
		// create connection for all slaves
		for _, m := range d.list.Members() {
			meta := &MetaData{}
			if err := json.Unmarshal(m.Meta, meta); err != nil {
				glog.Warningf("unmarshal meta failed: %v", err)
				continue
			}
			if meta.Role == NodeRoleSlave {
				d.server.collector.AddNode(fmt.Sprintf("%s:%d", m.Addr.String(), meta.ServerPort))
			}
		}
	default:
		// clean all node in collector
		d.server.collector.DisableConnection()
	}
}

func (d *GossipSystem) checkPromotion(meta *MetaData, node *memberlist.Node) {
	if meta.Role == NodeRoleMaster {
		// check if need to update status
		if d.meta.Epic < meta.Epic || (d.meta.Epic == meta.Epic && strings.Compare(d.master, node.Name) < 0) {
			d.meta.Epic = meta.Epic
			d.master = node.Name
			if d.meta.Role == NodeRoleMaster {
				// switch to standby
				d.meta.Role = NodeRoleStandby
				d.OnRoleChanged(NodeRoleMaster, NodeRoleStandby)
			}
		}
	}
}

// event delegate
func (d *GossipSystem) NotifyJoin(node *memberlist.Node) {
	if node.Name == d.name {
		return
	}

	meta := &MetaData{}
	if err := json.Unmarshal(node.Meta, meta); err != nil {
		glog.Warningf("unmarshal meta failed: %v", err)
		return
	}

	d.Lock()
	defer d.Unlock()

	d.checkPromotion(meta, node)

	// see if need to add node to collector
	if d.meta.Role == NodeRoleMaster && meta.Role == NodeRoleSlave {
		// we are the master, and found a new slave
		d.server.collector.AddNode(fmt.Sprintf("%s:%d", node.Addr.String(), meta.ServerPort))
	}
	// update seeds
	go d.writeSeeds()
}

func (d *GossipSystem) NotifyUpdate(node *memberlist.Node) {
	meta := &MetaData{}
	if err := json.Unmarshal(node.Meta, meta); err != nil {
		glog.Warningf("unmarshal meta failed: %v", err)
		return
	}

	d.Lock()
	defer d.Unlock()

	d.checkPromotion(meta, node)
	// currently, role could not be switched between slave and master|standby.
}

func (d *GossipSystem) NotifyLeave(node *memberlist.Node) {
	meta := &MetaData{}
	if err := json.Unmarshal(node.Meta, meta); err != nil {
		glog.Warningf("unmarshal meta failed: %v", err)
		return
	}

	d.Lock()
	defer d.Unlock()

	if meta.Role == NodeRoleMaster {
		// check if need an election, only standby could start an election.
		if d.master == node.Name {
			// master left
			if d.meta.Role == NodeRoleStandby {
				// start an election
				d.meta.Epic++
				d.meta.Role = NodeRoleMaster
				d.OnRoleChanged(NodeRoleStandby, NodeRoleMaster)
			}
		}
	}

	if d.meta.Role == NodeRoleMaster && meta.Role == NodeRoleSlave {
		// remove the left slave node from collector
		d.server.collector.RemoveNode(fmt.Sprintf("%s:%d", node.Addr.String(), meta.ServerPort))
	}
	go d.writeSeeds()
}

func (d *GossipSystem) NotifyAlive(peer *memberlist.Node) error {
	meta := &MetaData{}
	if err := json.Unmarshal(peer.Meta, meta); err != nil {
		glog.Warningf("unmarshal meta failed: %v", err)
		return fmt.Errorf("unmarshal meta failed: %v", err)
	}

	d.Lock()
	defer d.Unlock()

	// reject those who is not the same group as us.
	if meta.Group != d.meta.Group {
		return fmt.Errorf("unexpected gorup: %v", meta.Group)
	}
	return nil
}

func (d *GossipSystem) Join(addr string) error {
	if _, err := d.list.Join([]string{addr}); err != nil {
		return err
	}
	return nil
}

var leavetimeout = 10 * time.Second

func (d *GossipSystem) Leave() error {
	if err := d.list.Leave(leavetimeout); err != nil {
		return err
	}
	return nil
}

func (d *GossipSystem) Remove(addr string) error {
	return errors.New("gossip system don't support this interface")
}

func (d *GossipSystem) ListNodes() ([]byte, error) {
	nodes := []*Node{}
	for _, m := range d.list.Members() {
		meta := &MetaData{}
		if err := json.Unmarshal(m.Meta, meta); err != nil {
			glog.Warningf("unmarshal meta failed: %v")
			continue
		}
		nodes = append(nodes, &Node{Name: m.Name, IP: m.Addr.String(), GossipPort: uint16(m.Port), Meta: meta})
	}

	data, err := json.Marshal(nodes)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// mannually control distributed system
type StaticSystem struct {
	server        *Server          // owner
	role          string           // role of this node
	nodes         map[string]*Node // slaves' info
	group         string           // cluster group
	seedsFilePath string           // cluster nodes file, usually used when restart

	sync.Mutex
}

// there is no standby static system
// slave can only added by master
func NewStaticSystem(server *Server, role string, group string) *StaticSystem {
	dir := path.Dir(server.config.Path)
	seedsFilePath := path.Join(dir, SeedsFileName)
	return &StaticSystem{
		server:        server,
		role:          role,
		group:         group,
		nodes:         make(map[string]*Node),
		seedsFilePath: seedsFilePath,
	}
}

func (d *StaticSystem) addNodeByAddr(addr string) error {
	if _, ok := d.nodes[addr]; !ok {
		ss := strings.Split(addr, ":")
		if len(ss) != 2 {
			return fmt.Errorf("unexpected address: %v", addr)
		}

		port, err := strconv.ParseUint(ss[1], 10, 16)
		if err != nil {
			return err
		}
		d.server.collector.AddNode(addr)
		d.nodes[addr] = &Node{
			Name:       addr,
			IP:         ss[0],
			GossipPort: 0,
			Meta: &MetaData{
				Role:       NodeRoleSlave,
				Epic:       0,
				ServerPort: uint16(port),
				Group:      d.group,
			},
		}
	}
	// update cluster file
	d.writeSeeds()
	return nil
}

func (d *StaticSystem) Run() {
	// slaves don't need the seeds
	if d.role == NodeRoleSlave {
		return
	}

	// try load cluster from seeds
	if _, err := os.Stat(d.seedsFilePath); os.IsNotExist(err) {
		glog.Info("no seeds to start cluster")
	} else {
		seeds, err := localConfig.SeedsFromFile(d.seedsFilePath)
		if err != nil {
			glog.Fatalf("load seeds failed: %v", err)
		}

		for _, addr := range seeds.Addrs {
			if err := d.addNodeByAddr(addr); err != nil {
				glog.Warningf("load node %v failed: %v", addr, err)
			}
		}
	}
}

func (d *StaticSystem) writeSeeds() {
	var addrs []string
	for k := range d.nodes {
		addrs = append(addrs, k)
	}

	// we must sure not to miss any nodes, so block and update cluster nodes file
	seeds := &localConfig.Seeds{Addrs: addrs}
	if err := localConfig.SeedsToFile(seeds, d.seedsFilePath); err != nil {
		glog.Warningf("write seed to %v failed: %v", d.seedsFilePath, err)
	}
	glog.V(7).Infof("write seed to %v done", d.seedsFilePath)
}

var NotMasterError = errors.New("not master")

func (d *StaticSystem) Join(addr string) error {
	// only master could join slave
	if d.role != NodeRoleMaster {
		return NotMasterError
	}

	d.Lock()
	defer d.Unlock()

	return d.addNodeByAddr(addr)
}

func (d *StaticSystem) Leave() error {
	if d.role != NodeRoleMaster {
		return NotMasterError
	}

	d.Lock()
	defer d.Unlock()

	d.server.collector.DisableConnection()
	// update cluster file
	d.writeSeeds()
	return nil
}

func (d *StaticSystem) Remove(addr string) error {
	// only master could remove slave
	if d.role != NodeRoleMaster {
		return NotMasterError
	}

	d.Lock()
	defer d.Unlock()

	if _, ok := d.nodes[addr]; ok {
		d.server.collector.RemoveNode(addr)
		delete(d.nodes, addr)
	}
	// update cluster file
	d.writeSeeds()
	return nil
}

func (d *StaticSystem) ListNodes() ([]byte, error) {
	if d.role != NodeRoleMaster {
		return nil, NotMasterError
	}

	d.Lock()
	defer d.Unlock()

	nodes := []*Node{}
	for _, n := range d.nodes {
		nodes = append(nodes, n)
	}

	data, err := json.Marshal(nodes)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// http handle function
func serveJoin(d DistributedSystem, w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	addr := r.Form.Get("addr")

	if err := d.Join(addr); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	io.WriteString(w, "OK")
}

func serveLeave(d DistributedSystem, w http.ResponseWriter, r *http.Request) {
	if err := d.Leave(); err != nil {
		glog.Warningf("leave cluster failed: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	io.WriteString(w, "OK")
}

func serveListNodes(d DistributedSystem, w http.ResponseWriter, r *http.Request) {
	data, err := d.ListNodes()
	if err != nil {
		glog.Warningf("list cluster nodes failed: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	io.WriteString(w, string(data))
}
