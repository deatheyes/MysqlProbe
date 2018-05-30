package probe

import (
	"bufio"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"

	"github.com/yanyu/MysqlProbe/message"
)

type Key struct {
	net, transport gopacket.Flow
}

func (k Key) String() string {
	return fmt.Sprintf("%v:%v", k.net, k.transport)
}

type MysqlStream struct {
	bidi   *bidi            // maps to the bidirectional twin.
	r      ReaderStream     // low level stream for tcpassembly
	c      chan MysqlPacket // output channel.
	stop   chan struct{}    // channel to stop stream.
	done   bool             // flag parsed success.
	client bool             // ture if it is a requeset stream.
}

func NewMysqlStream(bidi *bidi, client bool) *MysqlStream {
	s := &MysqlStream{
		bidi:   bidi,
		stop:   bidi.stop,
		done:   false,
		client: client,
		r:      NewReaderStream(),
	}
	if client {
		s.c = bidi.req
		bidi.a = s
	} else {
		s.c = bidi.rsp
		bidi.b = s
	}
	go s.run()
	return s
}

func (s MysqlStream) parse(data []byte) (MysqlPacket, error) {
	base := &MysqlBasePacket{}
	if err := base.DecodeFromBytes(data); err != nil {
		return nil, err
	}

	if s.client {
		return base.ParseRequestPacket()
	} else {
		return base.ParseResponsePacket()
	}
}

func (s *MysqlStream) run() {
	buf := bufio.NewReader(&s.r)
	for {
		base, err := ReadMysqlBasePacket(buf)
		if err == io.EOF {
			// We must read until we see an EOF... very important!
			return
		} else if err != nil {
			// not mysql protocal.
			glog.Warningf("[worker %v] stream parse mysql packet failed: %v", s.bidi.wid, err)
			return
		} else {
			var packet MysqlPacket
			var err error
			if s.client {
				packet, err = base.ParseRequestPacket()
			} else {
				packet, err = base.ParseResponsePacket()
			}
			if err != nil {
				// not a concerned packet.
				glog.V(5).Infof("[worker %v] parse packet error: %v, ignored packet: %v", s.bidi.wid, err, base.Data)
			} else {
				glog.V(8).Infof("[woker %v] parse packet done, client: %v, data: %v", s.bidi.wid, s.client, base.Data)
				// report to bidi or wait to exit.
				select {
				case <-s.stop:
					return
				case s.c <- packet:
				}
			}
		}
	}
}

type bidi struct {
	key            Key                     // Key of the first stream, mostly for logging.
	a, b           *MysqlStream            // the two bidirectional streams.
	lastPacketSeen time.Time               // last time we saw a packet from either stream.
	out            chan<- *message.Message // channel to report message, copy from bidi factory.
	req            chan MysqlPacket        // channel to receive request packet.
	rsp            chan MysqlPacket        // channel to receive response packet.
	stop           chan struct{}           // channel to stop stream a, b.
	stopped        bool                    // if is shutdown.
	wid            int                     // worker id for log.
	sync.Mutex
}

func Newbidi(key Key, out chan<- *message.Message, wid int) *bidi {
	b := &bidi{
		key:     key,
		req:     make(chan MysqlPacket),
		rsp:     make(chan MysqlPacket),
		stop:    make(chan struct{}),
		out:     out,
		stopped: false,
		wid:     wid,
	}
	go b.run()
	return b
}

func (b *bidi) shutdown() {
	b.Lock()
	defer b.Unlock()

	if !b.stopped {
		b.stopped = true
		// b.a shuld not be null, just in case.
		if b.a != nil {
			close(b.a.c)
		}
		if b.b != nil {
			close(b.b.c)
		}
		close(b.stop)
	}
}

func (b *bidi) run() {
	var msg *message.Message
	waitting := false // flag true if there is a request waitting for response.
	// compare the timestamp of request and response.
	// TODO: wrap packet with timestamp.
	for {
		select {
		case reqPacket := <-b.req:
			// set expireation timestamp.
			glog.V(8).Infof("[worker %v] request packet received", b.wid)
			if b.lastPacketSeen.Before(b.a.r.Seen()) {
				b.lastPacketSeen = b.a.r.Seen()
			}
			// create report data.
			waitting = true
			msg = &message.Message{
				Sql:          generateQuery(reqPacket.Stmt(), true),
				TimestampReq: b.a.r.Seen(),
			}

		case rspPacket := <-b.rsp:
			glog.V(8).Infof("[worker %v] response packet received", b.wid)
			// recevice response packet.
			// update timestamp.
			if ok := b.lastPacketSeen.Before(b.b.r.Seen()); !ok {
				// an expired or sub response packet.
				glog.V(5).Infof("[worker %v] found a useless packet", b.wid)
			} else {
				b.lastPacketSeen = b.b.r.Seen()
			}
			// if there is a request waitting, this packet is possible the first packet of response.
			if waitting {
				// fill report data.
				msg.TimestampRsp = b.b.r.Seen()
				if rspPacket.Err() != nil {
					msg.Err = true
				} else {
					msg.Err = false
				}
				// report.
				b.out <- msg
				waitting = false
				glog.V(5).Infof("[worker %v] mysql query parsed done: %v", msg, b.wid)
			}
		case <-b.stop:
			return
		}
	}
}

type IsRequest func(netFlow, tcpFlow gopacket.Flow) bool

type BidiFactory struct {
	bidiMap   map[Key]*bidi           // bidiMap maps keys to bidirectional stream pairs.
	out       chan<- *message.Message // channle to report message.
	isRequest IsRequest               // check if it is a request stream.
	wid       int                     // worker id for log.
}

// New handles creating a new tcpassembly.Stream. Must be sure the bidi.a is a client stream.
func (f *BidiFactory) New(netFlow, tcpFlow gopacket.Flow) tcpassembly.Stream {
	// Create a new stream.
	var s *MysqlStream

	// Find the bidi bidirectional struct for this stream, creating a new one if
	// request doesn't already exist in the map.
	k := Key{netFlow, tcpFlow}
	bd := f.bidiMap[k]
	if bd == nil {
		bd = Newbidi(k, f.out, f.wid)
		s = NewMysqlStream(bd, f.isRequest(netFlow, tcpFlow))
		glog.V(8).Infof("[worker %v][%v] created request side of bidirectional stream", f.wid, bd.key)
		kReverse := Key{netFlow.Reverse(), tcpFlow.Reverse()}
		if v := f.bidiMap[kReverse]; v != nil {
			// shutdown the Orphan bidi.
			v.shutdown()
		}
		// Register bidirectional with the reverse key, so the matching stream going
		// the other direction will find it.
		f.bidiMap[Key{netFlow.Reverse(), tcpFlow.Reverse()}] = bd
	} else {
		glog.V(8).Infof("[worker %v][%v] found response side of bidirectional stream", f.wid, bd.key)
		s = NewMysqlStream(bd, f.isRequest(netFlow, tcpFlow))
		// Clear out the bidi we're using from the map, just in case.
		delete(f.bidiMap, k)
	}
	return &s.r
}

func (f *BidiFactory) collectOldStreams(timeout time.Duration) {
	cutoff := time.Now().Add(-timeout)
	for k, bd := range f.bidiMap {
		if bd.lastPacketSeen.Before(cutoff) {
			glog.Infof("[worker %v][%v] timing out old stream", f.wid, bd.key)
			delete(f.bidiMap, k) // remove it from our map.
			bd.shutdown()        // if b was the last stream we were waiting for, finish up.
		}
	}
}
