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

type Cluster struct {
	inited     bool // flag true if cluster could run
	list       *memberlist.Memberlist
	seed       []string           // cluster seed nodes
	meta       *MetaMessage       // meta info
	isMaster   bool               // if start as a master
	masterName string             // master name
	config     *memberlist.Config // memberlist config
	group      string             // cluster name
	sync.Mutex
}

func NewCluster(isMaster bool, seed []string, group string) *Cluster {
	return &Cluster{
		inited:   false,
		seed:     seed,
		isMaster: isMaster,
		group:    group,
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
		var newSeed []string
		for _, member := range c.list.Members() {
			newSeed = append(newSeed, member.Addr.String())
		}
		if len(newSeed) != 0 {
			// TODO: diff and flush to disk
			c.seed = newSeed
		}
	}
}

func (c *Cluster) processStatusChange(node *memberlist.Node) {
	meta := &MetaMessage{}
	if err := meta.DecodeFromBytes(node.Meta); err != nil {
		return
	}

	c.Lock()
	defer c.Unlock()

	if meta.Role == master {
		// check if need to update status
		if c.meta.Epic < meta.Epic || (c.meta.Epic == meta.Epic && strings.Compare(c.masterName, node.Name) < 0) {
			c.meta.Epic = meta.Epic
			c.masterName = node.Name
			if c.meta.Role == master {
				// switch to standby
				c.meta.Role = standby
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

	if meta.Role == master {
		// check if need an election, only standby could start an election.
		if c.masterName == node.Name {
			// master left.
			if c.meta.Role == standby {
				// start an election.
				c.meta.Epic++
				c.meta.Role = master
				// TODO: start the colloctor.
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
