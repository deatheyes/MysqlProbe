package probe

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/google/gopacket"

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

	assembly := &Assembly{
		streamMap: make(map[Key]*MysqlStream),
		out:       w.out,
		isRequest: f,
		wname:     w.name,
		flush:     w.flush,
	}

	// padding for breaking symmetry.
	padding := time.Millisecond * time.Duration((util.Hash(w.name)%20)*10)
	ticker := time.Tick(w.interval + padding)
	count := 0
	glog.Infof("[%v] init done, ticker: %v", w.name, w.interval+padding)

	for {
		select {
		case packet := <-w.in:
			assembly.Assemble(packet)
		case <-ticker:
			count++
			if count%10 == 0 {
				// close expired stream
				glog.V(8).Infof("[%v] try to close expired stream", w.name)
				assembly.CloseOlderThan(time.Now().Add(-w.interval))
			}
		}
	}

}
