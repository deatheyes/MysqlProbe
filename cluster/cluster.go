package cluster

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/hashicorp/memberlist"
	"sync"
)

// outbound node info
type Node struct {
	Addr string
	Role string
}

func NewNode(addr string, role byte) *Node {
	n := &Node{Addr: addr}
	switch role {
	case slave:
		n.Role = "slave"
	case master:
		n.Role = "master"
	case standby:
		n.Role = "standby"
	default:
		n.Role = "unknown"
	}
	return n
}

func (n *Node) String() string {
	return fmt.Sprintf("Addr: %s, Role: %s", n.Addr, n.Role)
}

type ClusterDelegate interface {
	NotifyMyRoleChanged(oldRole string, newRole string)
	NotifyJoin(node *Node)
	NotifyUpdate(node *Node)
	NotifyLeave(node *Node)
	NotifyRefresh(nodes []*Node)
}

type Cluster struct {
	inited     bool // flag true if cluster could run
	list       *memberlist.Memberlist
	seed       []string           // cluster seed nodes
	meta       *MetaMessage       // meta info
	isMaster   bool               // if start as a master
	masterName string             // master name
	config     *memberlist.Config // memberlist config
	group      string             // cluster name
	Delegate   ClusterDelegate    // callbacks for cluster action
	sync.Mutex
}

func NewCluster(isMaster bool, seed []string, group string, d ClusterDelegate) *Cluster {
	return &Cluster{
		inited:   false,
		seed:     seed,
		isMaster: isMaster,
		group:    group,
		Delegate: d,
	}
}

var UnexpectedGroupError = errors.New("unexpected group")

func (c *Cluster) CheckMeta(meta *MetaMessage) error {
	if meta.Group != c.meta.Group {
		glog.Warningf("unexpected group: %v", meta.Group)
		return UnexpectedGroupError
	}
	return nil
}

func (c *Cluster) Init() error {
	c.config = memberlist.DefaultWANConfig()
	c.config.Events = NewEventDelegate(c)
	c.config.Alive = NewAliveDelegate(c)

	// create meta
	c.meta = &MetaMessage{Epic: 0, Group: c.group}
	if c.isMaster {
		c.meta.Role = master
		c.masterName = c.config.Name
	} else {
		c.meta.Role = slave
		c.masterName = ""
	}

	// join the cluster.
	var err error
	c.list, err = memberlist.Create(c.config)
	if err != nil {
		return err
	}
	c.inited = true
	return nil
}

func (c *Cluster) Run() {
	if !c.inited {
		glog.Fatal("cluster has not been inited")
		return
	}

	// TODO: flush and reload meta and seed
	// join
	ticker := time.Tick(joinRetryInterval)
	for {
		<-ticker
		glog.Infof("try to join cluster:%v seed:%v", c.group, c.seed)
		_, err := c.list.Join(c.seed)
		if err != nil {
			glog.Warningf("join cluster failed: %v", err)
		} else {
			glog.Info("cluster join success")
		}
	}

	ticker = time.Tick(refreshSeedInterval)
	for {
		<-ticker
		glog.V(8).Infof("refresh seed")
		var newNodes []*Node
		for _, member := range c.list.Members() {
			meta := &MetaMessage{}
			if err := meta.DecodeFromBytes(member.Meta); err != nil {
				glog.Warningf("decode node meta failed: %v", err)
				continue
			}
			n := NewNode(member.Addr.String(), meta.Role)
			newNodes = append(newNodes, n)
		}
		if c.Delegate != nil {
			c.Delegate.NotifyRefresh(newNodes)
		}
		if len(newNodes) != 0 {
			// TODO: diff and flush to disk
		}
	}
}

func (c *Cluster) processStatusChange(node *memberlist.Node, join bool) {
	meta := &MetaMessage{}
	if err := meta.DecodeFromBytes(node.Meta); err != nil {
		return
	}

	c.Lock()
	defer c.Unlock()

	n := NewNode(node.Addr.String(), meta.Role)
	if c.Delegate != nil {
		if join {
			c.Delegate.NotifyJoin(n)
		} else {
			c.Delegate.NotifyUpdate(n)
		}
	}

	if meta.Role == master {
		// check if need to update status
		if c.meta.Epic < meta.Epic || (c.meta.Epic == meta.Epic && strings.Compare(c.masterName, node.Name) < 0) {
			c.meta.Epic = meta.Epic
			c.masterName = node.Name
			if c.meta.Role == master {
				// switch to standby
				c.meta.Role = standby
				if c.Delegate != nil {
					c.Delegate.NotifyMyRoleChanged("master", "standby")
				}
			}
		}
	}
}

func (c *Cluster) processLeave(node *memberlist.Node) {
	meta := &MetaMessage{}
	if err := meta.DecodeFromBytes(node.Meta); err != nil {
		return
	}

	c.Lock()
	defer c.Unlock()

	n := NewNode(node.Addr.String(), meta.Role)
	if c.Delegate != nil {
		c.Delegate.NotifyLeave(n)
	}

	if meta.Role == master {
		// check if need an election, only standby could start an election.
		if c.masterName == node.Name {
			// master left.
			if c.meta.Role == standby {
				// start an election.
				c.meta.Epic++
				c.meta.Role = master
				// TODO: start the colloctor.
				if c.Delegate != nil {
					c.Delegate.NotifyMyRoleChanged("standby", "master")
				}
			}
		}
	}
}

func (c *Cluster) processAlive(peer *memberlist.Node) error {
	meta := &MetaMessage{}
	if err := meta.DecodeFromBytes(peer.Meta); err != nil {
		glog.Warningf("decode meta failed: %v", err)
		return fmt.Errorf("decode meta failed: %v", err)
	}

	c.Lock()
	defer c.Unlock()

	// reject those who is not the same group as us.
	if meta.Group != c.meta.Group {
		return fmt.Errorf("unexpected gorup: %v", meta.Group)
	}
	return nil
}
