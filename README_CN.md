#### README: [English](README.md)

# Mysql Probe
受 [vividcortex](https://www.vividcortex.com/) 及 [linkedin blog](https://engineering.linkedin.com/blog/2017/09/query-analyzer--a-tool-for-analyzing-mysql-queries-without-overh) 启发而开发的 MySQL 抓包监控系统。

## 模块
只有一个二进制文件，允许以三种模式运行:
* slave
* master
* standby

### Slave
与 MySQL 同机部署的探针，主要用于抓取 MySQL 请求，并获取 SQL、错误、响应行等基准数据，同时基于这些数据做一些初级计算，例如请求耗时、平均响应时间、99分位数等等。它是整个系统最基础的模块，可以提供除集群外的所有功能，能够独立于其它两种模块运行。

### Master
负责从下层 slave 收集信息，并通过 websocket 汇报聚合后的数据。

### Standby
用于容灾的 master，仅用于 gossip 集群模式。

## 集群
目前集成了两种集群模式：**gossip**、**static**

### Gossip 集群
节点间通过 gossip 互联，从而实现自动容灾。

#### 接口
* collector("/collector"): 从 master 或者 slave 获取聚合或采集到的数据，websocket 协议。
* join("/cluster/join?addr="): 向集群中加入节点, 'addr' 为已在集群中的其中一个节点的地址，http 协议。
* leave("/cluster/leave"): 从集群中移除当前节点，http 协议。
* listnodes("/cluster/listnodes") : 列出集群拓扑中的所有节点， http 协议。
* config-update("/config/update?{key}={value}"): 动态配置接口，http 协议。目前只有采样频率 'report\_period\_ms' 能够配置。

### Static 集群
此集群模式中只会有 master 和 slave 节点。如果出现节点宕机，需要人工介入。

#### 接口

master 及 slave 上均存在的接口:
* collector("/collector"): 从 master 或者 slave 获取聚合或采集到的数据，websocket 协议。
* config-update("/config/update?{key}={value}"): 动态配置接口，http 协议。目前只有采样频率 'report\_period\_ms' 能够配置。

仅存在于 master 的接口:
* join("/cluster/join?addr="): 将某一从节点加入集群，http 协议。
* leave("/cluster/leave"): 从集群中移除当前节点，其所有从节点均会被移除，http 协议。
* remove("/cluster/remove?addr="): 删除某一从节点，地址以 'addr' 指示，http 协议。
* listnodes("/cluster/listnodes"): 列出当前集群拓扑，http 协议。

### 制定自己的集群

上边两种树形结构的集群更多的是实验性质，而大多数生产环境中都需要平滑且无状态的数据处理层。可以基于 slave 制定生产环境，其携带的 **pusher** 模块可以将数据推送至一组用于数据处理服务器中的某一节点。

## 配置
yaml 格式的配置文件:

        slave: true           # 是否以 slave 模式运行。在 gossip 集群中所有非 slave 节点最初都会以 master 模式运行。
        serverport: 8667      # websocket 端口
        interval: 1           # 数据汇报周期(s)， slave 和 master 会以该周期通过 websocket 端口以及 pusher 汇报数据。
        slowthresholdms: 100  # 慢查询阈值，响应时间超过该值的请求都会标记为慢查询。
        cluster:
          gossip: true   # 是否以 gossip 模式启动。
          group: test    # 集群名
          port: 0        # gossip 端口，0 为自动选择。
        probe:
          device: lo0,en0  # 需要监控的设备以 ',' 分隔，仅对 slave 生效。
          port: 3306       # 抓包端口， 仅对 slave 生效。
          snappylength: 0  # 快照缓冲区长度，仅对 slave 生效。
          workers: 2       # 处理抓包数据的协程数，仅对 slave 生效。
        pusher:
          servers: 127.0.0.1:8668,127.0.0.1:8669 # 以 ',' 分隔的服务节点，pusher 会从中选择一个推送数据，websocket 协议。
          path: websocket                        # 服务节点的 websocket 路径。
          preconnect: true                       # 是否提前创建连接。
        watcher:                     # watcher 用于缓存、更新数据库名与连接信息，通过 127.0.0.1 连接本地MySQL。
          uname: test                # MySQL 用户名。
          passward: test             # MySQL 用户密码。
        websocket:              # webscoket 配置
          writetimeoutms: 1000  # websocket 写超时(ms)
          pingtimeouts: 30      # webscoket ping 超时(s)
          reconnectperiods: 10  # websocket 连接重建等待时间(s)
          maxmessagesize: 16384 # websocket 最大消息体(k)

### 全局配置

* slave: 节点角色。
* serverport: 服务端口。数据会通过 '/collector' 接口推送给所有连接的客户端。
* interval: 数据推送周期(s).
* slowthresholdms: 慢查询阈值(ms).

### 集群配置(cluster)

此部分配置为可选项，默认会初始化为 gossip 集群。如果需要制定集群，不要通过相关接口填加 gossip 节点即可。

* gossip: 集群模式。
* group: 集群名，用于避免不同集群节点的错误引入。
* port: gossip 端口，设置为 0 时会自动选择，日志中会打印出实际使用端口。

### 采集配置(probe)

此部分的大多数配置都与 **libpcap** 相关。只有 slave 会创建 probe，需要与 MySQL 同机部署。

* device: 需要监控的设备，多个设备以 ',' 分隔。
* port: Mysql 端口，目前仅支持配置一个。
* snappylength: libpcap 的快照缓冲区大小。如果不确定操作系统对 libpcap 的支持程度，建议设置为 0。参考 **Note** 获取更多相关信息。
* workers: probe 中用于处理采集数据的协程数目。

### 推送器(pusher)

**pusher** 是一个可选模块，相较于全局配置中的 '/collector' 接口, 它会将数据推送给其中一个下游节点。利用它可以制定集群, 例如将其下游设置为数据处理的 proxy 集群，proxy 集群完成数据清洗后再导入存储层、数据挖掘、机器学习等系统。

* servers: 下游服务组，多个节点间以 ',' 分隔。
* path: websocket 路径。
* preconnect: 是否预先创建对所有下游节点的连接。

### 监视器(watcher)

用于创建、缓存库与连接的映射表，以 127.0.0.1 方式连接 MySQL，须拥有中执行 'show processlist' 的权限。

## 输出
slave 或 master 的汇报数据为经过 snappy 压缩过的 json 数据，主要包含以下信息:

* SQL 模板: 经过格式化的 SQL，如 "select * from user where name=?"。
* 延迟: 毫秒维度的请求响应时间。
* 时间戳: 请求、响应包被抓取的时间戳。
* 状态: 成功、失败、响应行等。

更具体的信息可以参考 **message.go**

## 注意事项
* 在 Linux 环境中，可能会遇到 'Activate: can't mmap rx ring: Invalid argument' 错误， 可以参考 [这里](https://stackoverflow.com/questions/11397367/issue-in-pcap-set-buffer-size) 获取相关信息。