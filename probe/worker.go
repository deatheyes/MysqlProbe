package probe

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/tcpassembly"

	"github.com/deatheyes/MysqlProbe/message"
	"github.com/deatheyes/MysqlProbe/util"
)

// Worker assembles the data from tcp connection distributed by Probe
type Worker struct {
	owner        *Probe                  // owner.
	in           chan gopacket.Packet    // input channel.
	out          chan<- *message.Message // output channel.
	flush        chan *FlushContext      // flush cotrol channel.
	flushMap     map[Key]bool            // record stream flushing.
	id           int                     // worker id.
	logAllPacket bool                    // wether to log the paocket.
	interval     time.Duration           // flush interval.
	name         string                  // worker name for logging.
}

// NewProbeWorker create a new woker to assemble tcp data
func NewProbeWorker(probe *Probe, out chan<- *message.Message, id int, interval time.Duration, logAllPacket bool) *Worker {
	p := &Worker{
		owner:        probe,
		in:           make(chan gopacket.Packet),
		flush:        make(chan *FlushContext),
		out:          out,
		interval:     interval,
		logAllPacket: logAllPacket,
		id:           id,
		name:         fmt.Sprintf("%v-%v", probe.device, id),
		flushMap:     make(map[Key]bool),
	}
	go p.Run()
	return p
}

// Run initilize and start the assembling process
func (w *Worker) Run() {
	f := func(netFlow, tcpFlow gopacket.Flow) bool {
		ip := netFlow.Dst()
		port := tcpFlow.Dst()
		return w.owner.IsRequest(ip.String(), binary.BigEndian.Uint16(port.Raw()))
	}

	streamFactory := &BidiFactory{bidiMap: make(map[Key]*bidi), out: w.out, isRequest: f, wname: w.name, flush: w.flush}
	streamPool := tcpassembly.NewStreamPool(streamFactory)
	assembler := tcpassembly.NewAssembler(streamPool)
	assembler.MaxBufferedPagesTotal = 100000
	assembler.MaxBufferedPagesPerConnection = 1000

	// padding for breaking symmetry.
	padding := time.Millisecond * time.Duration((util.Hash(w.name)%20)*10)
	ticker := time.Tick(w.interval + padding)
	count := 0
	glog.Infof("[%v] init done, ticker: %v", w.name, w.interval+padding)
	for {
		select {
		case packet := <-w.in:
			if w.logAllPacket {
				glog.V(8).Infof("[%v] parse packet: %v", w.name, packet)
			}
			if len(w.flushMap) != 0 {
				// filter flushing
				key := Key{packet.NetworkLayer().NetworkFlow(), packet.TransportLayer().TransportFlow()}
				if ok := w.flushMap[key]; ok {
					glog.V(8).Info("[%v] stream is flushing", w.name)
					continue
				}
			}
			tcp := packet.TransportLayer().(*layers.TCP)
			assembler.AssembleWithTimestamp(packet.NetworkLayer().NetworkFlow(), tcp, packet.Metadata().Timestamp)
		case ctx := <-w.flush:
			reverse := Key{ctx.key.net.Reverse(), ctx.key.transport.Reverse()}
			if ctx.flag {
				// set flag and flush all buffered packet for alignment.
				// we cannot close connection and rebuild the stream:
				// 1. prepare cache will be lost.
				// 2. those bidis don't need flush will be suffered.
				glog.Warningf("[%v] stream %v is require flushing", w.name, ctx.key)
				w.flushMap[ctx.key] = true
				w.flushMap[reverse] = true
				assembler.FlushWithOptions(tcpassembly.FlushOptions{CloseAll: false, T: time.Now()})
			} else {
				// clean flush flag
				delete(w.flushMap, ctx.key)
				delete(w.flushMap, reverse)
			}
		case <-ticker:
			// flush data every interval for process, not close connection.
			// flush data every 10 * interval for clean up, close expired connection.
			count++
			if count%10 == 0 {
				count = 0
				glog.V(8).Infof("[%v] flush for clean up", w.name)
				assembler.FlushOlderThan(time.Now().Add(-(10 * w.interval)))
			} else {
				glog.V(8).Infof("[%v] flush for parse", w.name)
				assembler.FlushWithOptions(tcpassembly.FlushOptions{CloseAll: false, T: time.Now().Add(-w.interval)})
			}
		}
	}
}
