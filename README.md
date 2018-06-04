# Mysql Probe
A distributed mysql packets capture and report system

## Modules
There is only one component which could run as three mode:
* slave
* master
* standby

### Slave
Slave run at the same machine with mysql. A probe will be started to capture the mysql query infos, such as sql, error and execution overhead.

### Master
Master is responsible for collecting infos from slaves. Aggregated data will be reported by websocket.

### Standby
Standby is a special master that runs as the backup of the master. It is only available in gossip cluster mode.

## Cluster
There are two cluster modes, gossip and static. 

### Gossip Cluster
In gossip mode, nodes are aware of each other, auto failover could be taken by the system.

#### Interface
* collector("/collector"): A websocket interface for caller to get assembled data from master or slave.
* join("/cluster/join?addr="): A http interface to join current node to a cluster, 'addr' is one of the cluster node's gossip address.
* leave("/cluster/leave"): A http interface to make current node left from its cluster.
* listnodes("/cluster/listnodes") : A http interface to list the topology of current node.

### Static Cluster
There are only masters and slaves in static mode. Manual intervention is needed when nodes down.

#### Interface
Interfaces are only availiable on master when cluster runs as static mode.

* collector("/collector"): A websocket interface for caller to get assembled data from master or slave.
* join("/cluster/join?addr="): A http interface to add a slave to current node.
* leave("/cluster/leave"): A http interface to make the node left from its cluster. All the slave of the node would be removed.
* remove("cluster/remove?addr="): A http interface to remove a slave from a master. 'addr' is the server address of the slave.
* listnodes("/cluster/listnodes") : A http interface to list the topology of the node.

## Configuration
The configuration is a yaml file as below:

	slave: true      # true if run as slave. In gossip mode, those nodes not slave are initialized as master. 
	serverport: 8667 # websocket address the node listen
	interval: 10     # report interval, slaves and master(s) will report assembled data period by websocket
	cluster:
	  gossip: true   # true if run as gossip mode
  	  group: test    # cluster name
  	  port: 0        # gossip bind port
	probe:
	  device: lo0      # device to probe, slave only
	  port: 3306       # port to probe, slave only
	  snappylength: 0  # snappy buffer length of the probe, slave only
	  workers: 2       # number of workers to process probe data, slave only
