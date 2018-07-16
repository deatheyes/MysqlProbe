package probe

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/google/gopacket"

	"github.com/deatheyes/MysqlProbe/message"
)

// Worker assembles the data from tcp connection dispatched by Probe
type Worker struct {
	owner *Probe                  // owner.
	in    chan gopacket.Packet    // input channel.
	out   chan<- *message.Message // output channel.
	id    int                     // worker id.
	name  string                  // worker name for logging.
}

// NewProbeWorker create a new woker to assemble tcp packets
func NewProbeWorker(probe *Probe, id int) *Worker {
	p := &Worker{
		owner: probe,
		in:    make(chan gopacket.Packet),
		out:   probe.out,
		id:    id,
		name:  fmt.Sprintf("%v-%v", probe.device, id),
	}
	go p.Run()
	return p
}

// Run initilizes and starts the assembly
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
	}

	ticker := time.NewTicker(streamExpiration)
	defer ticker.Stop()

	glog.Infof("[%v] initilization done, stream expiration: %v", w.name, streamExpiration)

	for {
		select {
		case packet := <-w.in:
			assembly.Assemble(packet)
		case <-ticker.C:
			// close expired stream
			glog.V(8).Infof("[%v] try to close expired streams", w.name)
			assembly.CloseOlderThan(time.Now().Add(-streamExpiration))
		}
	}
}
