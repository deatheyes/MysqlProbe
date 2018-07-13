package probe

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"

	"github.com/deatheyes/MysqlProbe/message"
)

// Key is the pair of networker and transport Flow
type Key struct {
	net, transport gopacket.Flow
}

func (k Key) String() string {
	return fmt.Sprintf("%v:%v", k.net, k.transport)
}

// FlushContext specify which stream to enable or disable flushing
type FlushContext struct {
	key  Key  // stream key to flush
	flag bool // enable or disable
}

// IsRequest is a callback set by user to distinguish flow direction.
type IsRequest func(netFlow, tcpFlow gopacket.Flow) bool

// MysqlStream is a tcp assembly stream wrapper of ReaderStream
type MysqlStream struct {
	assembly *Assembly            // owner
	in       chan gopacket.Packet // input channel
	localIP  string               // server ip
	key      Key                  // hash key
	name     string               // stream name for log
	lastSeen time.Time            // timestamp of the lastpacket processed
	stop     chan struct{}        // notify close
	closed   bool                 // close flag
}

func newMysqlStream(assembly *Assembly, localIP string, key Key) *MysqlStream {
	s := &MysqlStream{
		assembly: assembly,
		in:       make(chan gopacket.Packet, 2000),
		localIP:  localIP,
		key:      key,
		name:     fmt.Sprintf("%v - %v", assembly.wname, key),
		stop:     make(chan struct{}),
		closed:   false,
	}
	go s.run()
	return s
}

func (s *MysqlStream) close() {
	if !s.closed {
		s.closed = true
		close(s.stop)
	}
}

func (s *MysqlStream) run() {
	var reqPacket *MysqlRequestPacket  // request packet
	var rspPacket *MysqlResponsePacket // response packet
	stmtmap := make(map[uint32]string) // map to register the statement
	var msg *message.Message
	var err error
	for {
		select {
		case packet := <-s.in:
			tcp := packet.TransportLayer().(*layers.TCP)
			// Ignore empty TCP packets
			if !tcp.SYN && !tcp.FIN && !tcp.RST && len(tcp.Payload) == 0 {
				continue
			}

			key := Key{packet.NetworkLayer().NetworkFlow(), packet.TransportLayer().TransportFlow()}
			if s.assembly.isRequest(key.net, key.transport) {
				// parse request packet
				// Note: there may be many mysql packets in one tcp packet.
				// we only care about the first mysql packet,
				// which should only be the first part of tcp payload
				basePacket := &MysqlBasePacket{}
				if _, err = basePacket.DecodeFromBytes(tcp.Payload); err != nil {
					glog.V(6).Infof("[%v] parse request base packet failed: %v", s.name, err)
					continue
				}

				// filter
				if basePacket.Seq() != 0 {
					glog.V(8).Infof("[%v] skip unconcerned packet %v", s.name, tcp.Payload)
					continue
				}

				if reqPacket, err = basePacket.ParseRequestPacket(); err != nil {
					glog.V(6).Infof("[%v] parse request packet failed: %v", s.name, err)
					continue
				}
				s.lastSeen = packet.Metadata().Timestamp

				// parse request and build message
				msg = &message.Message{TimestampReq: s.lastSeen, IP: s.localIP}
				switch reqPacket.CMD() {
				case comQuery:
					// this is a raw sql query
					msg.SQL = generateQuery(reqPacket.Stmt(), true)
					glog.V(6).Infof("[%v] [query] sql: %v", s.name, reqPacket.SQL())
				case comStmtPrepare:
					// the statement will be registered if processed OK
					glog.V(6).Infof("[%v] [prepare] sql: %v", s.name, reqPacket.SQL())
				case comStmtExecute:
					stmtID := reqPacket.StmtID()
					if _, ok := stmtmap[stmtID]; !ok {
						// no stmt possible query error or sequence error。
						glog.V(5).Infof("[%v] no corresponding local statement found, stmtID: %v", s.name, stmtID)
					} else {
						msg.SQL = stmtmap[stmtID]
						glog.V(6).Infof("[%v] [execute] stmtID: %v, sql: %v", s.name, stmtID, stmtmap[stmtID])
					}
				default:
					// not the packet concerned, continue
					glog.V(8).Infof("[%v] request packet received unconcerned packet", s.name)
					reqPacket = nil
					continue
				}
			} else {
				// parse response packet
				if reqPacket == nil {
					// if there is no request, skip this packet ASAP
					continue
				}

				// Note: there may be many mysql packets in one tcp packet.
				// we only care about the first mysql packet,
				// which should only be the first part of tcp payload
				basePacket := &MysqlBasePacket{}
				if _, err = basePacket.DecodeFromBytes(tcp.Payload); err != nil {
					glog.V(6).Infof("[%v] parse response base packet failed: %v", s.name, err)
					continue
				}

				// filter
				if basePacket.Seq() != 1 {
					glog.V(8).Infof("[%v] skip unconcerned packet %v", s.name, tcp.Payload)
					continue
				}

				if rspPacket, err = basePacket.ParseResponsePacket(reqPacket.CMD()); err != nil {
					glog.V(6).Infof("[%v] parse request packet failed: %v", s.name, err)
					continue
				}
				s.lastSeen = packet.Metadata().Timestamp
				msg.TimestampRsp = s.lastSeen

				// parse reponse and fill message
				status := rspPacket.Status()
				switch status.flag {
				case iOK:
					msg.Err = false
					msg.AffectRows = status.affectedRows
					msg.ServerStatus = status.status
					// if is a prepare request, register the sql and continue.
					if reqPacket.CMD() == comStmtPrepare {
						glog.V(6).Infof("[%v] [prepare] response OK, stmtID: %v, sql: %v", s.name, rspPacket.StmtID(), reqPacket.SQL())
						stmtmap[rspPacket.StmtID()] = reqPacket.SQL()
					}
				case iERR:
					msg.Err = true
					msg.ErrMsg = status.message
					msg.Errno = status.errno
				default:
					// response for SELECT
					msg.Err = false
				}

				// report
				// don't report those message without SQL.
				// there is no SQL in prepare message.
				// need more precise filter about control command such as START, END
				if len(msg.SQL) > 5 {
					glog.V(6).Infof("[%v] mysql query parsed done: %v", s.name, msg.SQL)
					s.assembly.out <- msg
				}
				reqPacket = nil
				rspPacket = nil
			}
		case <-s.stop:
			glog.Infof("[%v] close stream", s.name)
			return
		}
	}
}

// Assembly dispatchs packet according to net flow and tcp flow
type Assembly struct {
	streamMap map[Key]*MysqlStream    // allocated stream
	out       chan<- *message.Message // channle to report message.
	isRequest IsRequest               // check if it is a request stream.
	wname     string                  // worker name for log.
	flush     chan<- *FlushContext    // flush control.
	sync.RWMutex
}

// Assemble send the packet to specify stream
func (a *Assembly) Assemble(packet gopacket.Packet) {
	key := Key{packet.NetworkLayer().NetworkFlow(), packet.TransportLayer().TransportFlow()}
	var s *MysqlStream
	s = a.streamMap[key]
	if s == nil {
		var serverIP string
		if a.isRequest(key.net, key.transport) {
			serverIP = key.net.Dst().String()
		} else {
			serverIP = key.net.Src().String()
		}

		reverse := Key{key.net.Reverse(), key.transport.Reverse()}
		s = newMysqlStream(a, serverIP, key)
		a.streamMap[key] = s
		a.streamMap[reverse] = s
	}
	s.in <- packet
}

// CloseOlderThan remove those streams expired and return the number of them
func (a *Assembly) CloseOlderThan(t time.Time) int {
	count := 0
	for k, v := range a.streamMap {
		if v.lastSeen.Before(t) {
			count++
			v.close()
			delete(a.streamMap, k)
		}
	}
	return count / 2
}
