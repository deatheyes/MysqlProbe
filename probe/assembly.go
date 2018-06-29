package probe

import (
	"fmt"
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

const (
	readHead byte = iota
	readBody
	skipBody
)

// MysqlStream is a tcp assembly stream wrapper of ReaderStream
type MysqlStream struct {
	bidi   *bidi                 // maps to the bidirectional twin.
	c      chan *MysqlBasePacket // output channel.
	client bool                  // ture if it is a requeset stream.
	name   string                // stream name for logging.
	closed bool                  // if this stream closed.
	count  int                   // current packet data length.
	length int                   // packet body length.
	header []byte                // packet header.
	body   []byte                // packet body.
	seq    byte                  // packet seq.
	status byte                  // parse status.
}

// NewMysqlStream create a bi-directional tcp assembly stream
func NewMysqlStream(bidi *bidi, client bool) *MysqlStream {
	s := &MysqlStream{
		bidi:   bidi,
		client: client,
		closed: false,
		count:  0,
		status: readHead,
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
	return s
}

// Reassembled implements tcpassembly.Stream's Reassembled function.
func (s *MysqlStream) Reassembled(reassembly []tcpassembly.Reassembly) {
	// parse as many packets as possible
	var pos int
	for _, r := range reassembly {
		if len(r.Bytes) == 0 {
			continue
		}
		pos = 0 // parsing position of current reassembly
		for {
			switch s.status {
			case readHead:
				if s.count == 0 {
					// parse start
					s.header = make([]byte, 4)
				}
				n := copy(s.header[s.count:], r.Bytes[pos:])
				s.count += n
				pos += n
				if s.count == 4 {
					// parse header done
					s.length = int(uint32(s.header[0]) | uint32(s.header[1])<<8 | uint32(s.header[2])<<16)
					s.seq = s.header[3]
					// check if is a packet concerned
					s.status = readBody
					if s.client {
						if s.seq != 0 {
							s.status = skipBody
						}
					} else {
						if s.seq != 1 {
							s.status = skipBody
						}
					}
				}
			case readBody:
				if s.count == 4 {
					// parse start
					s.body = make([]byte, s.length)
				}

				if s.length == 0 {
					// empty packet, reset status
					s.count = 0
					s.status = readHead
					break
				}

				// check if is a packet concerned
				if s.client {
					cmd := r.Bytes[pos]
					if cmd != comQuery && cmd != comStmtPrepare && cmd != comStmtExecute {
						s.status = skipBody
						continue
					}
				}

				n := copy(s.body[s.count-4:], r.Bytes[pos:])
				s.count += n
				pos += n

				if s.count == s.length+4 {
					// packet parsed done, reset status
					s.c <- &MysqlBasePacket{Header: s.header, Data: s.body, Timestamp: r.Seen}
					s.count = 0
					s.status = readHead
				}
			case skipBody:
				if s.length == 0 {
					s.count = 0
					s.status = readHead
					break
				}

				need := s.length + 4 - s.count
				left := len(r.Bytes) - pos
				if need < left {
					pos += need
					// skip the packet body done
					s.count = 0
					s.status = readHead
				} else {
					// not enougth data
					pos += left
					s.count += left
				}
			}

			if pos == len(r.Bytes) {
				// reassembly parsed done
				break
			}
		}
	}
}

// ReassemblyComplete implements tcpassembly.Stream's ReassemblyComplete function.
func (s *MysqlStream) ReassemblyComplete() {
	glog.Infof("[%v] stream closed", s.name)
	// we cannot close s.c, as another may replace this stream
	s.closed = true
}

// bidi is a bi-direction wapper of tcp assembly stream
type bidi struct {
	key            Key                     // Key of the first stream, mostly for logging.
	a, b           *MysqlStream            // the two bidirectional streams.
	lastPacketSeen time.Time               // last time we saw a packet from either stream.
	out            chan<- *message.Message // channel to report message, copy from bidi factory.
	req            chan *MysqlBasePacket   // channel to receive request packet.
	rsp            chan *MysqlBasePacket   // channel to receive response packet.
	stop           chan struct{}           // channel to stop.
	stopped        bool                    // if is stopped.
	name           string                  // bidi name for logging.
	factory        *BidiFactory            // owner.
}

func newbidi(key Key, out chan<- *message.Message, wname string, factory *BidiFactory) *bidi {
	b := &bidi{
		key:     key,
		req:     make(chan *MysqlBasePacket, 10000),
		rsp:     make(chan *MysqlBasePacket, 10000),
		stop:    make(chan struct{}),
		stopped: false,
		out:     out,
		name:    fmt.Sprintf("%s-%s", wname, key),
		factory: factory,
	}
	go b.run()
	go b.monitor()
	return b
}

func (b *bidi) monitor() {
	ticker := time.NewTicker(time.Second)
	for {
		<-ticker.C
		// handle shutdown
		if b.factory.tryRemove(b) {
			close(b.stop)
			return
		}
		// handle blocking
		reqlen := len(b.req)
		rsplen := len(b.rsp)
		if reqlen > 1000 || rsplen > 1000 {
			// flush the current data
			glog.Warningf("flush blocking packet, data lost, request: %v, response: %v", reqlen, rsplen)
			go func(length int) {
				for i := 0; i < length; i++ {
					<-b.req
				}
			}(reqlen)
			go func(length int) {
				for i := 0; i < length; i++ {
					<-b.rsp
				}
			}(rsplen)
		}
	}
}

func (b *bidi) updateTimestamp(t time.Time) bool {
	if b.lastPacketSeen.Before(t) {
		b.lastPacketSeen = t
		return true
	}
	return false
}

func (b *bidi) run() {
	defer glog.V(3).Infof("[%v] bidi shutdown", b.name)

	var msg *message.Message           // message to report
	var waitting MysqlPacket           // request waitting for response
	var reqPacket *MysqlBasePacket     // request packet
	var rspPacket *MysqlBasePacket     // response packet
	stmtmap := make(map[uint32]string) // map to register the statement

	for {
		// get request packet
		select {
		case reqPacket = <-b.req:
		case <-b.stop:
			return
		}

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
			glog.V(6).Infof("[%v] [query] sql: %v", b.name, waitting.Sql())
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

		// find response
		for {
			select {
			case rspPacket = <-b.rsp:
			case <-b.stop:
				return
			}

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

			if waitting.Seq()+1 != packet.Seq() {
				// not the peer
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
			break
		}
	}
}

// IsRequest is a callback set by user to distinguish flow direction.
type IsRequest func(netFlow, tcpFlow gopacket.Flow) bool

// BidiFactory retains the basic data to create a bidi.
type BidiFactory struct {
	bidiMap   map[Key]*bidi           // bidiMap maps keys to bidirectional stream pairs.
	out       chan<- *message.Message // channle to report message.
	isRequest IsRequest               // check if it is a request stream.
	wname     string                  // worker name for log.
	sync.RWMutex
}

// New handles creating a new tcpassembly.Stream. Must be sure the bidi.a is a client stream.
func (f *BidiFactory) New(netFlow, tcpFlow gopacket.Flow) tcpassembly.Stream {
	f.Lock()
	defer f.Unlock()
	// Create a new stream.
	var s *MysqlStream

	// Find the bidi bidirectional struct for this stream, creating a new one if
	// request doesn't already exist in the map.
	k := Key{netFlow, tcpFlow}
	bd := f.bidiMap[k]
	if bd == nil {
		bd = newbidi(k, f.out, f.wname, f)
		s = NewMysqlStream(bd, f.isRequest(netFlow, tcpFlow))
		reverse := Key{netFlow.Reverse(), tcpFlow.Reverse()}
		glog.Infof("[%s] created request side of bidirectional stream %s", f.wname, bd.key)
		// Register bidirectional with the reverse key, so the matching stream going
		// the other direction will find it.
		f.bidiMap[reverse] = bd
	} else {
		glog.Infof("[%v] found response side of bidirectional stream %v", f.wname, bd.key)
		s = NewMysqlStream(bd, f.isRequest(netFlow, tcpFlow))
	}
	return s
}

func (f *BidiFactory) tryRemove(b *bidi) bool {
	f.RLock()
	defer f.RUnlock()

	clientClosed := (b.a == nil) || (b.a.closed)
	serverClosed := (b.b == nil) || (b.b.closed)
	if clientClosed && serverClosed {
		delete(f.bidiMap, b.key)
		return true
	}
	return false
}
