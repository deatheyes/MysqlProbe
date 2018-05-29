package cluster

import (
	"github.com/hashicorp/memberlist"
)

type eventDelegate struct {
	cluster *Cluster
}

func NewEventDelegate(cluster *Cluster) *eventDelegate {
	return &eventDelegate{cluster: cluster}
}

func (d *eventDelegate) NotifyJoin(node *memberlist.Node) {
	d.cluster.processStatusChange(node)
}

func (d *eventDelegate) NotifyLeave(node *memberlist.Node) {
	d.cluster.processLeave(node)
}

func (d *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	d.cluster.processStatusChange(node)
}

type aliveDelegate struct {
	cluster *Cluster
}

func NewAliveDelegate(cluster *Cluster) *aliveDelegate {
	return &aliveDelegate{cluster: cluster}
}

func (d *aliveDelegate) NotifyAlive(peer *memberlist.Node) error {
	return d.cluster.processAlive(peer)
}
