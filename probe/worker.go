package probe

import (
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/golang/glog"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/tcpassembly"

	"github.com/yanyu/MysqlProbe/message"
)

type ProbeWorker struct {
	owner        *Probe                // owner.
	in           chan gopacket.Packet  // input channel.
	out          chan *message.Message // output channel.
	id           int                   // worker id.
	logAllPacket bool                  // wether to log the paocket.
	interval     time.Duration         // flush interval.
}

func NewProbeWorker(prbe *Probe, out chan *message.Message, id int, interval time.Duration, logAllPacket bool) *ProbeWorker {
	p := &ProbeWorker{
		owner:        prbe,
		in:           make(chan gopacket.Packet),
		out:          out,
		id:           id,
		interval:     interval,
		logAllPacket: logAllPacket,
	}
	go p.Run()
	return p
}

func (w *ProbeWorker) Run() {
	f := func(netFlow, tcpFlow gopacket.Flow) bool {
		ip := netFlow.Dst()
		port := tcpFlow.Dst()
		return w.owner.IsRequest(ip.String(), binary.BigEndian.Uint16(port.Raw()))
	}

	streamFactory := &BidiFactory{bidiMap: make(map[Key]*bidi), out: w.out, isRequest: f, wid: w.id}
	streamPool := tcpassembly.NewStreamPool(streamFactory)
	assembler := tcpassembly.NewAssembler(streamPool)
	assembler.MaxBufferedPagesTotal = 100000
	assembler.MaxBufferedPagesPerConnection = 1000

	// padding for broken symmetry.
	padding := time.Millisecond * time.Duration(rand.Int()*w.id%500)
	ticker := time.Tick(w.interval + padding)
	count := 0
	glog.Infof("[worker %v] init done, ticker: %v", w.id, w.interval+padding)
	for {
		select {
		case packet := <-w.in:
			if w.logAllPacket {
				glog.V(8).Infof("[worker %v] parse packet: %v", w.id, packet)
			}
			tcp := packet.TransportLayer().(*layers.TCP)
			assembler.AssembleWithTimestamp(packet.NetworkLayer().NetworkFlow(), tcp, packet.Metadata().Timestamp)
		case <-ticker:
			// flush data every interval for process, not close connection.
			// flush data every 10 * interval for clean up, close expired connection.
			count++
			if count%10 == 0 {
				count = 0
				glog.V(8).Infof("[worker %v] flush for clean up", w.id)
				assembler.FlushOlderThan(time.Now().Add(-(10 * w.interval)))
				streamFactory.collectOldStreams((10 * w.interval))
			} else {
				glog.V(8).Infof("[worker %v] flush for parse", w.id)
				assembler.FlushWithOptions(tcpassembly.FlushOptions{CloseAll: false, T: time.Now().Add(-w.interval)})
			}
		}
	}
}
