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
Standby is a special mater that runs as the backup of the master. It is only available in cluster mode.

## Cluster
In cluster mode, all nodes will be connected by gossip. Standbys will elect a new master if the old one dead.
