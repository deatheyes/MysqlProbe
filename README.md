# Mysql Probe
A distributed mysql packets capture and report system inspired by [vividcortex](https://www.vividcortex.com/) and [linkedin blog](https://engineering.linkedin.com/blog/2017/09/query-analyzer--a-tool-for-analyzing-mysql-queries-without-overh)

## Modules
There is only one component which could run as three mode:
* slave
* master
* standby

### Slave
Slave run at the same machine with mysql. A probe will be started to capture the mysql query infos, such as sql, error and execution latency.

### Master
Master is responsible for collecting infos from slaves. Aggregated data will be reported by websocket.

### Standby
Standby is a special master that runs as the backup of the master. It is only available in gossip cluster mode.

## Cluster
There are two cluster modes, **gossip** and **static**. 

### Gossip Cluster
In gossip mode, nodes are aware of each other, auto failover could be taken by the system.

#### Interface
* collector("/collector"): A websocket interface for caller to get assembled data from master or slave.
* join("/cluster/join?addr="): A http interface to join current node to a cluster, 'addr' is one of the cluster node's gossip address.
* leave("/cluster/leave"): A http interface to make current node left from its cluster.
* listnodes("/cluster/listnodes") : A http interface to list the topology of current node.
* config-update("/config/update?{key}={value}"): A http interface to config the node dynamiclly. Only 'report\_period\_ms', the sampling freuency of this node, supported currently.

### Static Cluster
There are only masters and slaves in static mode. Manual intervention is needed when nodes down.

#### Interface

Interfaces both availiable on master and slave:

* collector("/collector"): A websocket interface for caller to get assembled data from master or slave.
* config-update("/config/update?{key}={value}"): A http interface to config the node dynamiclly. Only 'report\_period\_ms', the sampling freuency of this node, supported currently. 

Interfaces only availiable on master:

* join("/cluster/join?addr="): A http interface to add a slave to current node.
* leave("/cluster/leave"): A http interface to make the node left from its cluster. All the slave of the node would be removed.
* remove("/cluster/remove?addr="): A http interface to remove a slave from a master. 'addr' is the server address of the slave.
* listnodes("/cluster/listnodes"): A http interface to list the topology of the node.

## Configuration
The configuration is a yaml file:

	slave: true      # true if run as slave. In gossip mode, those nodes not slave are initialized as master. 
	serverport: 8667 # websocket address the node listen
	interval: 10     # report interval, slaves and master(s) will report assembled data periodically by websocket
	cluster:
	  gossip: true   # true if run as gossip mode
  	  group: test    # cluster name
  	  port: 0        # gossip bind port
	probe:
	  device: lo0,en0  # devices to probe, splited by ',', slave only
	  port: 3306       # port to probe, slave only
	  snappylength: 0  # snappy buffer length of the probe, slave only
	  workers: 2       # number of workers to process probe data, slave only

## Output
Data collected from slave or master will be reported in form of json. The report contains statistical items:

* sql template: A sql template is a sql like text without constant condition value. eg. "select * from user where name=?".
* latency: The execution latency.
* timestamp: Request and response timestamps.
* status: Wether sueecssed or not.

This is the current struct of the Report:

	// MessageGroup is the assembled info of a sql template
	type MessageGroup struct {
		QPS               int64      `json:"qps"`                       // current qps
		AverageLatency    int64      `json:"avg_latency"`               // average latency
		SuccessCount      int        `json:"success"`                   // success query number
		FailedCount       int        `json:"failed"`                    // failed query number
		LastSeen          time.Time  `json:"last_seen"`                 // the latest timestamp
		SuccCostMsTotal   int64      `json:"success_total_cost"`        // total cost of success query, we don't caculate average info for the sake of performence
		FailedCostMsTotal int64      `json:"failed_total_cost"`         // total cost of failed query, we don't caculate average info for the sake of performence
		NoGoodIndexUsed   int64      `json:"status_no_good_index_used"` // count of SERVER_STATUS_NO_GOOD_INDEX_USED
		NoIndexUsed       int64      `json:"status_no_index_used"`      // count of SERVER_STATUS_NO_INDEX_USED
		QueryWasSlow      int64      `json:"status_query_was_slow"`     // count of SERVER_QUERY_WAS_SLOW
	}
	// data reported to master or user
	type Report struct {
		Groups map[string]*MessageGroup `json:"groups"`
	}

## Note
* On Linux, users may come up with an error 'Activate: can't mmap rx ring: Invalid argument', please refer [here](https://stackoverflow.com/questions/11397367/issue-in-pcap-set-buffer-size) for more detail
