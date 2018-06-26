package probe

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"

	"github.com/yanyu/MysqlProbe/message"
)

// Key is the pair of networker and transport Flow
type Key struct {
	net, transport gopacket.Flow
}

func (k Key) String() string {
	return fmt.Sprintf("%v:%v", k.net, k.transport)
}

// MysqlStream is a tcp assemble stream wrapper of ReaderStream
type MysqlStream struct {
	bidi   *bidi                 // maps to the bidirectional twin.
	r      ReaderStream          // low level stream for tcpassembly
	c      chan *MysqlBasePacket // output channel.
	stop   chan struct{}         // channel to stop stream.
	done   bool                  // flag parsed success.
	client bool                  // ture if it is a requeset stream.
	name   string                // stream name for logging.
}

// NewMysqlStream create a bi-directional tcp assembly stream
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
		s.name = fmt.Sprintf("%v-client", bidi.name)
	} else {
		s.c = bidi.rsp
		bidi.b = s
		s.name = fmt.Sprintf("%v-server", bidi.name)
	}
	go s.run()
	return s
}

func (s *MysqlStream) run() {
	//buf := bufio.NewReader(&s.r)
	count := 0
	for {
		//base, err := ReadMysqlBasePacket(buf)
		base, err := ReadMysqlBasePacket(&s.r)
		if err == io.EOF {
			// We must read until we see an EOF... very important!
			return
		} else if err != nil {
			// not mysql protocal.
			glog.Warningf("[%v] stream parse mysql packet failed: %v", s.name, err)
			return
		}

		// filter the packets not concerned, skip ASAP
		if s.client {
			if base.Data[0] != comQuery && base.Data[0] != comStmtPrepare && base.Data[0] != comStmtExecute {
				// not the packet concerned, skip ASAP
				continue
			}
		}

		// Warning for possible blocking
		if len(s.c) > 200 && count%500 == 0 {
			count++
			glog.Warningf("[%v] stream has %v watting packets, watch out for possible blocking", s.name, len(s.c))
		}
		select {
		case <-s.stop:
			return
		case s.c <- base:
		}
	}
}

// bidi is a bi-direction wapper of tcp assembly stream
type bidi struct {
	key            Key                     // Key of the first stream, mostly for logging.
	a, b           *MysqlStream            // the two bidirectional streams.
	lastPacketSeen time.Time               // last time we saw a packet from either stream.
	out            chan<- *message.Message // channel to report message, copy from bidi factory.
	req            chan *MysqlBasePacket   // channel to receive request packet.
	rsp            chan *MysqlBasePacket   // channel to receive response packet.
	stop           chan struct{}           // channel to stop stream a, b.
	stopped        bool                    // if is shutdown.
	name           string                  // bidi name for logging.
	sync.Mutex
}

func newbidi(key Key, out chan<- *message.Message, wname string) *bidi {
	b := &bidi{
		key:     key,
		req:     make(chan *MysqlBasePacket, 10000),
		rsp:     make(chan *MysqlBasePacket, 10000),
		stop:    make(chan struct{}),
		out:     out,
		stopped: false,
		name:    fmt.Sprintf("%s-%s", wname, key),
	}
	go b.run()
	return b
}

func (b *bidi) shutdown() {
	b.Lock()
	defer b.Unlock()

	if !b.stopped {
		b.stopped = true
		close(b.stop)
	}
}

func (b *bidi) close() {
	if b.a != nil {
		glog.V(5).Infof("[%v] input stream shutdown", b.name)
		b.a.r.Close()
	}
	if b.b != nil {
		glog.V(5).Infof("[%v] output stream shutdown", b.name)
		b.b.r.Close()
	}
	glog.V(5).Infof("[%v] shutdown", b.name)
}

func (b *bidi) updateTimestamp(t time.Time) bool {
	if b.lastPacketSeen.Before(t) {
		b.lastPacketSeen = t
		return true
	}
	return false
}

func (b *bidi) run() {
	var msg *message.Message           // message to report
	var waitting MysqlPacket           // request waitting for response
	stmtmap := make(map[uint32]string) // map to register the statement

	for {
		// get request packet
		select {
		case reqPacket := <-b.req:
			glog.V(8).Infof("[%v] request packet received", b.name)
			// update expireation timestamp.
			b.updateTimestamp(reqPacket.Timestamp)

			// parse request packet
			packet, err := reqPacket.ParseRequestPacket()
			if err != nil {
				glog.V(5).Infof("[%v] parse packet error: %v, ignored packet: %v", b.name, err, reqPacket.Data)
				continue
			}

			// build message
			msg = &message.Message{TimestampReq: reqPacket.Timestamp}
			switch packet.CMD() {
			case comQuery:
				// this is a raw sql query
				waitting = packet
				msg.SQL = generateQuery(packet.Stmt(), true)
			case comStmtPrepare:
				// the statement will be registered if processed OK
				waitting = packet
				glog.V(6).Infof("[%v] [prepare] sql: %v", b.name, waitting.Sql())
			case comStmtExecute:
				waitting = packet
				stmtID := packet.StmtID()
				if _, ok := stmtmap[stmtID]; !ok {
					// no stmt possible query error or sequence error。
					glog.V(5).Infof("[%v] no corresponding local statement found, stmtID: %v", b.name, stmtID)
				} else {
					msg.SQL = stmtmap[stmtID]
					glog.V(6).Infof("[%v] [execute] stmtID: %v, sql: %v", b.name, stmtID, stmtmap[stmtID])
				}
			default:
				// not the packet concerned, continue
				glog.V(8).Infof("[%v] request packet received unconcerned packet", b.name)
				continue
			}
		case <-b.stop:
			b.close()
			return
		}

		// find response
	findResponse:
		for {
			select {
			case rspPacket := <-b.rsp:
				glog.V(8).Infof("[%v] response packet received", b.name)
				// update expireation timestamp.
				b.updateTimestamp(rspPacket.Timestamp)

				if msg.TimestampReq.After(rspPacket.Timestamp) {
					// this is an expired packet or a sub packet
					glog.V(8).Infof("[%v] found a useless or expired packet", b.name)
					continue
				}

				// parse response packet
				packet, err := rspPacket.ParseResponsePacket(waitting.CMD())
				if err != nil {
					glog.V(5).Infof("[%v] parse packet error: %v, ignored packet: %v", b.name, err, rspPacket.Data)
					continue
				}

				msg.TimestampRsp = rspPacket.Timestamp
				status := packet.Status()
				switch status.flag {
				case iOK:
					msg.Err = false
					msg.AffectRows = status.affectedRows
					msg.ServerStatus = status.status
					// if is a prepare request, register the sql and continue.
					if waitting.CMD() == comStmtPrepare {
						glog.V(6).Infof("[%v] [prepare] response OK, stmtID: %v, sql: %v", b.name, packet.StmtID(), waitting.Sql())
						stmtmap[packet.StmtID()] = waitting.Sql()
					}
				case iERR:
					msg.Err = true
					msg.ErrMsg = status.message
					msg.Errno = status.errno
				default:
					// response for SELECT
					msg.Err = false
				}

				// don't report those message without SQL.
				// there is no SQL in prepare message.
				// need more precise filter about control command such as START, END
				if len(msg.SQL) > 5 {
					// report
					glog.V(6).Infof("[%v] mysql query parsed done: %v", b.name, msg.SQL)
					b.out <- msg
				}
				break findResponse
			case <-b.stop:
				b.close()
				return
			}
		}
	}
}

/*func (b *bidi) run() {
	var msg *message.Message           // message to report
	var waitting MysqlPacket           // request waiting for response
	stmtmap := make(map[uint32]string) // map to register the statement

	for {
		select {
		case reqPacket := <-b.req:
			glog.V(8).Infof("[worker %v] request packet received", b.wid)
			// update expireation timestamp.
			b.updateTimestamp(reqPacket.Timestamp)

			if reqPacket.Seq != 0 {
				glog.V(6).Infof("[worker %v] not the fist packet of request: %v", b.wid, reqPacket.Data)
			}

			// TODO: parse transaction
			packet, err := reqPacket.ParseRequestPacket()
			if err != nil {
				glog.V(5).Infof("[worker %v] parse packet error: %v, ignored packet: %v", b.wid, err, reqPacket.Data)
				continue
			}
			msg = &message.Message{TimestampReq: reqPacket.Timestamp}
			switch packet.CMD() {
			case comQuery:
				// this is an raw sql query
				waitting = packet
				msg.SQL = generateQuery(packet.Stmt(), true)
			case comStmtPrepare:
				// the statement will be registered if processed OK
				waitting = packet
				glog.V(6).Infof("[worker %v] [prepare] sql: %v", b.wid, waitting.Sql())
			case comStmtExecute:
				waitting = packet
				stmtID := packet.StmtID()
				if _, ok := stmtmap[stmtID]; !ok {
					// no stmt possible query error or sequence error。
					glog.V(5).Infof("[worker %v] no corresponding local statement found, stmtID: %v", b.wid, stmtID)
				} else {
					glog.V(6).Infof("[worker %v] [execute] stmtID: %v, sql: %v", b.wid, stmtID, stmtmap[stmtID])
					msg.SQL = stmtmap[stmtID]
				}
			}
		case rspPacket := <-b.rsp:
			glog.V(8).Infof("[worker %v] response packet received", b.wid)
			// update expireation timestamp.
			b.updateTimestamp(rspPacket.Timestamp)
			if b.lastPacketSeen.After(rspPacket.Timestamp) {
				// this is an expired packet or a sub packet
				glog.V(8).Infof("[worker %v] found a useless or expired packet", b.wid)
				continue
			}

			// if there is a request waitting, this packet is possible the first packet of response.
			if waitting != nil {
				packet, err := rspPacket.ParseResponsePacket(waitting.CMD())
				if err != nil {
					glog.V(5).Infof("[worker %v] parse packet error: %v, ignored packet: %v", b.wid, err, rspPacket.Data)
					// we don't need to reset the waitting packet here as the new request packet will replace it.
					continue
				}

				msg.TimestampRsp = rspPacket.Timestamp
				status := packet.Status()
				if status != nil {
					switch status.flag {
					case iOK:
						msg.Err = false
						msg.AffectRows = status.affectedRows
						msg.ServerStatus = status.status
						// if is a prepare request, register the sql and continue.
						if waitting.CMD() == comStmtPrepare {
							glog.V(6).Infof("[worker %v] [prepare] response OK, stmtID: %v, sql: %v", b.wid, packet.StmtID, waitting.Sql())
							stmtmap[packet.StmtID()] = waitting.Sql()
						}
					case iERR:
						msg.Err = true
						msg.ErrMsg = status.message
						msg.Errno = status.errno
					default:
						// not the reponse concerned.
						continue
					}
				}
				waitting = nil
				// don't report those message without SQL.
				// there is no SQL in prepare message.
				if len(msg.SQL) == 0 {
					continue
				}
				// report.
				glog.V(7).Infof("[worker %v] mysql query parsed done: %v, bidi: %v", b.wid, msg.SQL, b.key)
				b.out <- msg
			}
		case <-b.stop:
			b.close()
			return
		}
	}
}*/

// IsRequest is a callback set by user to distinguish flow direction.
type IsRequest func(netFlow, tcpFlow gopacket.Flow) bool

// BidiFactory retains the basic data to create a bidi.
type BidiFactory struct {
	bidiMap   map[Key]*bidi           // bidiMap maps keys to bidirectional stream pairs.
	out       chan<- *message.Message // channle to report message.
	isRequest IsRequest               // check if it is a request stream.
	wname     string                  // worker name for log.
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
		bd = newbidi(k, f.out, f.wname)
		s = NewMysqlStream(bd, f.isRequest(netFlow, tcpFlow))
		reverse := Key{netFlow.Reverse(), tcpFlow.Reverse()}
		glog.Infof("[%s] created request side of bidirectional stream %s", f.wname, bd.key)
		if v := f.bidiMap[reverse]; v != nil {
			// shutdown the Orphan bidi.
			v.shutdown()
		}
		// Register bidirectional with the reverse key, so the matching stream going
		// the other direction will find it.
		f.bidiMap[reverse] = bd
	} else {
		glog.Infof("[%v] found response side of bidirectional stream %v", f.wname, bd.key)
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
			glog.Infof("[%v] timing out old stream %v", f.wname, bd.key)
			delete(f.bidiMap, k) // remove it from our map.
			bd.shutdown()        // if b was the last stream we were waiting for, finish up.
		}
	}
}
