package server

import (
	"github.com/golang/glog"
	"github.com/yanyu/MysqlProbe/cluster"
)

type CollectorClusterDelegate struct {
	collector *Collector
	nodes     map[string]*cluster.Node
	role      string
}

func NewCollectorClusterDelegate(c *Collector, role string) *CollectorClusterDelegate {
	return &CollectorClusterDelegate{collector: c, role: role}
}

func (d *CollectorClusterDelegate) NotifyMyRoleChanged(oldRole string, newRole string) {
	glog.V(5).Infof("my role change form %v to %v", oldRole, newRole)
	d.role = newRole
	switch newRole {
	case "master":
		// start collect data from nodes
		d.collector.EnableConnection()
	case "standby":
		// stop collect data from nodes
		d.collector.DisableConnection()
	default:
		d.collector.DisableConnection()
	}
}

func (d *CollectorClusterDelegate) NotifyJoin(node *cluster.Node) {
	// we are only concerned about the slave join, and only master will react to it
	glog.V(5).Infof("new node join: %s", node)
	if node.Role == "slave" && d.role == "master" {
		d.collector.AddNode(node.Addr)
	}
}

func (d *CollectorClusterDelegate) NotifyUpdate(node *cluster.Node) {
	// do nothing currently.
}

func (d *CollectorClusterDelegate) NotifyLeave(node *cluster.Node) {
	// we are only concerned about the slave join, and only master will react to it
	glog.V(5).Infof("node leave: %s", node)
	if node.Role == "slave" && d.role == "master" {
		d.collector.RemoveNode(node.Addr)
	}
}

func (d *CollectorClusterDelegate) NotifyRefresh(nodes []*cluster.Node) {
	// diff the cluster, used by role change from standby to master
	// node will add to collector after enable connection
	for _, n := range nodes {
		if old := d.nodes[n.Addr]; old != nil {
			// only master concerned about the role change
			if old.Role != n.Role && d.role == "master" && n.Role != "slave" {
				d.collector.RemoveNode(n.Addr)
				delete(d.nodes, n.Addr)
			}
		} else {
			// new slave
			if d.role == "master" && n.Role == "slave" {
				d.collector.AddNode(n.Addr)
				d.nodes[n.Addr] = n
			}
		}
	}
}
