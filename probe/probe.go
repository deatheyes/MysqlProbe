package probe

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"

	"github.com/yanyu/MysqlProbe/message"
	"github.com/yanyu/MysqlProbe/util"
)

// Probe need to deloyed at server side.
type Probe struct {
	device    string
	snapLen   int32
	localIPs  []string
	port      uint16                // probe port.
	filter    string                // bpf filter.
	inited    bool                  // flag if could be run.
	workers   []*ProbeWorker        // probe worker group processing packet.
	workerNum int                   // worker number.
	out       chan *message.Message // data collect channel.
}

func NewProbe(device string, snapLen int32, port uint16, workerNum int) *Probe {
	p := &Probe{
		device:    device,
		snapLen:   snapLen,
		port:      port,
		inited:    false,
		workerNum: workerNum,
		out:       make(chan *message.Message),
	}
	return p
}

func (p *Probe) Out() <-chan *message.Message {
	return p.out
}

func (p *Probe) Init() error {
	IPs, err := util.GetLocalIPs()
	if err != nil {
		return err
	}
	p.localIPs = IPs
	p.filter = fmt.Sprintf("tcp and port %v", p.port)
	if p.workerNum <= 0 {
		p.workerNum = 1
	}
	p.inited = true
	return nil
}

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

func (p *Probe) Run() {
	// create workers.
	if !p.inited {
		glog.Fatal("probe not inited")
		return
	}

	glog.Infof("prbe run - %s", p)
	for i := 0; i < p.workerNum; i++ {
		w := NewProbeWorker(p, p.out, i, time.Second, false)
		p.workers = append(p.workers, w)
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

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	for packet := range packetSource.Packets() {
		if packet.NetworkLayer() == nil || packet.TransportLayer() == nil || packet.TransportLayer().LayerType() != layers.LayerTypeTCP {
			glog.Warning("unexpected packet")
			continue
		}
		// dispatch packet by stream network flow.
		id := int(packet.NetworkLayer().NetworkFlow().FastHash() % uint64(p.workerNum))
		p.workers[id].in <- packet
	}
}
