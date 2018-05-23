package main

import (
	"flag"

	"github.com/golang/glog"
	"github.com/yanyu/MysqlProbe/probe"
)

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
