package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/golang/glog"
	"github.com/yanyu/MysqlProbe/config"
	"github.com/yanyu/MysqlProbe/probe"
	"github.com/yanyu/MysqlProbe/server"
)

/*
// probe test
func main() {
	flag.Parse()

	var snapLen int32
	snapLen = 16 << 20
	p := probe.NewProbe("lo0", snapLen, 3306, 2)

	if err := p.Init(); err != nil {
		glog.Fatalf("probe init failed: %v", err)
		return
	}
	go p.Run()
	for msg := range p.Out() {
		glog.Infof("get message: %v", msg.Sql)
	}
}
*/

var (
	configfile string
	version    bool
)

func showVersion() {
	// TODO: read version from file
	fmt.Println("mysql probe 0.0.0.1 - author yanyu")
}

func init() {
	flag.StringVar(&configfile, "c", "./conf/config.yaml", "yaml `config` file path")
	flag.BoolVar(&version, "version", false, "show version")
}

func main() {
	flag.Parse()

	if version {
		showVersion()
	}

	conf, err := config.ConfigFromFile(configfile)
	if err != nil {
		glog.Fatalf("load config failed: %v", err)
		return
	}

	glog.Infof("load config done: %s", string(config.ConfigToBytes(conf)))
	conf.Path = configfile
	// set default report interval if necessary
	if conf.Interval == 0 {
		conf.Interval = 5
	}

	conf.Role = "master"
	if conf.Slave {
		conf.Role = "slave"
	}

	// start server
	glog.Infof("run server, role: %v port: %v report period: %v s gossip: %v group: %v", conf.Role, conf.Port, conf.Interval, conf.Cluster.Gossip, conf.Cluster.Group)
	s := server.NewServer(conf)
	go s.Run()

	// check if need to start probe
	if conf.Slave {
		glog.Info("start probe...")
		if len(conf.Probe.Device) == 0 {
			glog.Fatal("start probe failed, no device specified")
			return
		}

		// probe all ports is prohibited
		if conf.Probe.Port == 0 {
			glog.Fatal("start probe failed, no probe port specified")
		}
		// set default snappy buffer length if needed
		if conf.Probe.SnapLen <= 0 {
			conf.Probe.SnapLen = int32(65535)
		}
		// set default woker number 1 if needed
		if conf.Probe.Workers <= 0 {
			conf.Probe.Workers = 1
		}

		// multi devices support
		devices := strings.Split(conf.Probe.Device, ",")
		for _, device := range devices {
			if len(device) != 0 {
				// start probe
				glog.Infof("run probe, device: %v snappylength: %v port: %v workers: %v", device, conf.Probe.SnapLen, conf.Probe.Port, conf.Probe.Workers)
				p := probe.NewProbe(device, conf.Probe.SnapLen, conf.Probe.Port, conf.Probe.Workers, s.Collector().MessageIn())
				if err := p.Init(); err != nil {
					glog.Fatalf("init probe failed: %v", err)
					return
				}
				go p.Run()
			}
		}
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
}
