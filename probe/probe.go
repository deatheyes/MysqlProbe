// Package probe classifies the packets into stream according to network flow and transport flow.
//                                                     stream(network1:transport1)\
//                                                   /                             \
//                               / hash(network flow)- stream(network2:transport1)- \
// packet-> hash(transport flow)                                                     messages
//                               \ hash(network flow)- stream(network3:transport2)- /
//                                                   \                             /
//                                                     stream(network4:transport2)/
//                                |               tcp assembly                   |
package probe

import (
	"fmt"
	"strings"

	"github.com/golang/glog"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"

	"github.com/deatheyes/MysqlProbe/message"
	"github.com/deatheyes/MysqlProbe/util"
)

// Probe need to deloyed at server side.
type Probe struct {
	device    string
	snapLen   int32
	localIPs  []string
	port      uint16                  // probe port.
	filter    string                  // bpf filter.
	inited    bool                    // flag if could be run.
	workers   []*Worker               // probe worker group processing packet.
	workerNum int                     // worker number.
	out       chan<- *message.Message // data collect channel.
	watcher   *util.ConnectionWatcher // db connection watcher.
}

// NewProbe create a probe to collect and parse packets
func NewProbe(device string, snapLen int32, port uint16, workerNum int, out chan<- *message.Message, watcher *util.ConnectionWatcher) *Probe {
	p := &Probe{
		device:    device,
		snapLen:   snapLen,
		port:      port,
		inited:    false,
		workerNum: workerNum,
		out:       out,
		watcher:   watcher,
	}
	return p
}

// Init is the preprocess before the probe starts
func (p *Probe) Init() error {
	IPs, err := util.GetLocalIPs()
	if err != nil {
		return err
	}
	p.localIPs = IPs
	slice := []string{}
	for _, h := range p.localIPs {
		item := fmt.Sprintf("(src host %v and src port %v) or (dst host %v and dst port %v)", h, p.port, h, p.port)
		slice = append(slice, item)
	}
	p.filter = fmt.Sprintf("tcp and (%v)", strings.Join(slice, " or "))
	if p.workerNum <= 0 {
		p.workerNum = 1
	}
	p.inited = true
	return nil
}

// IsRequest distinguish if is a inbound request
func (p *Probe) IsRequest(dstIP string, dstPort uint16) bool {
	if dstPort != p.port {
		return false
	}
	for _, ip := range p.localIPs {
		if ip == dstIP {
			return true
		}
	}
	return false
}

func (p *Probe) String() string {
	return fmt.Sprintf("device: %v, snapshot length: %v, probe port: %v, bpf filter: %v, local IPs: %v, inited: %v, workers: %v",
		p.device, p.snapLen, p.port, p.filter, p.localIPs, p.inited, p.workerNum)
}

// Run starts the probe after inited
func (p *Probe) Run() {
	if !p.inited {
		glog.Fatal("probe not inited")
		return
	}

	glog.Infof("probe run - %s", p)
	for id := 0; id < p.workerNum; id++ {
		p.workers = append(p.workers, NewProbeWorker(p, id))
	}

	// run probe.
	handle, err := pcap.OpenLive(p.device, p.snapLen, true, pcap.BlockForever)
	if err != nil {
		glog.Fatalf("pcap open live failed: %v", err)
		return
	}
	if err := handle.SetBPFFilter(p.filter); err != nil {
		glog.Fatalf("set bpf filter failed: %v", err)
		return
	}
	defer handle.Close()

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packetSource.NoCopy = true
	for packet := range packetSource.Packets() {
		if packet.NetworkLayer() == nil || packet.TransportLayer() == nil || packet.TransportLayer().LayerType() != layers.LayerTypeTCP {
			glog.Warning("unexpected packet")
			continue
		}
		// dispatch packet by stream transport flow.
		id := int(packet.TransportLayer().TransportFlow().FastHash() % uint64(p.workerNum))
		p.workers[id].in <- packet
	}
}
