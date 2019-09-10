package probe

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	LRUCache "github.com/hashicorp/golang-lru"

	"github.com/deatheyes/MysqlProbe/message"
	"github.com/deatheyes/MysqlProbe/util"
)

// Key is the pair of networker and transport Flow
type Key struct {
	net, transport gopacket.Flow
}

func (k Key) String() string {
	return fmt.Sprintf("%v:%v", k.net, k.transport)
}

// IsRequest is a callback set by user to distinguish flow direction.
type IsRequest func(netFlow, tcpFlow gopacket.Flow) bool

// MysqlStream is a tcp assembly stream wrapper of ReaderStream
type MysqlStream struct {
	assembly   *Assembly            // owner
	key        Key                  // hash key
	localIP    string               // server ip
	localPort  uint16               // server port
	clientIP   string               // client ip
	clientPort uint16               // client port
	name       string               // stream name for log
	lastSeen   time.Time            // timestamp of the lastpacket processed
	closed     bool                 // close flag
	stop       chan struct{}        // notify close
	in         chan gopacket.Packet // input channel
	dbname     string               // dbname get from handshake response
	uname      string               // uname get from handshake response
	cache      *LRUCache.Cache      // lru cache of prepare statment
}

func newMysqlStream(assembly *Assembly, localIP string, localPort uint16, clientIP string, clientPort uint16, key Key) *MysqlStream {
	c, _ := LRUCache.New(lruCacheSize)
	s := &MysqlStream{
		assembly:   assembly,
		key:        key,
		localIP:    localIP,
		localPort:  localPort,
		clientIP:   clientIP,
		clientPort: clientPort,
		name:       fmt.Sprintf("%v-%v", assembly.wname, key),
		closed:     false,
		stop:       make(chan struct{}),
		in:         make(chan gopacket.Packet, inputQueueLength),
		cache:      c,
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
	basePacket := &MysqlBasePacket{}
	reqPacket := &MysqlRequestPacket{}
	rspPacket := &MysqlResponsePacket{}
	waitting := false // if there is a request packet parsed
	var msg *message.Message
	var err error
	handshake := false
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
				// parse client packet
				// Note: there may be many mysql packets in one tcp packet.
				// we only care about the first mysql packet,
				// which should only be the first part of tcp payload regardless of what the tcp packet seq is.
				if _, err = basePacket.DecodeFromBytes(tcp.Payload); err != nil {
					glog.V(6).Infof("[%v] parse request base packet failed: %v", s.name, err)
					continue
				}

				// parse handshake response
				if handshake && basePacket.Seq() == mysqlRspSeq {
					// this packet should be a handshake response
					uname, dbname, err := basePacket.parseHandShakeResponse()
					if err != nil {
						// maybe not a handshake response
						glog.Warningf("[%v] parse handshake response failed: %v", s.name, err)
					} else {
						glog.V(6).Infof("[%v] parse handshake response done, uname: %v, dbname: %v", s.name, uname, dbname)
						s.uname = uname
						s.dbname = dbname
						waitting = false
						if s.cache.Len() > 0 {
							s.cache.Purge()
						}
						handshake = false
					}
					continue
				}

				// filter
				if basePacket.Seq() != mysqlReqSeq {
					glog.V(8).Infof("[%v] skip unconcerned packet %v", s.name, tcp.Payload)
					continue
				}

				if err = basePacket.ParseRequestPacket(reqPacket); err != nil {
					glog.V(6).Infof("[%v] parse request packet failed: %v", s.name, err)
					continue
				}

				// reuse message not sent
				if msg == nil {
					msg = &message.Message{}
				}
				// parse request and build message
				msg.TimestampReq = packet.Metadata().Timestamp.UnixNano()
				msg.UnknownExec = false
				switch reqPacket.cmd {
				case comQuery:
					switch reqPacket.queryType {
					case queryNormal:
						// this is a raw sql query
						msg.SQL, msg.Vars = generateQuery(reqPacket.Stmt(), true)
						msg.Raw = reqPacket.SQL()
						glog.V(6).Infof("[%v] [query][normal] sql: %v", s.name, reqPacket.SQL())
					case queryPrepare:
						msg.SQL = reqPacket.SQL()
						msg.Raw = ""
						glog.V(6).Infof("[%v] [query][prepare] name: %v, sql: %v", s.name, reqPacket.queryName, msg.SQL)
					case queryExecute:
						stmtName := reqPacket.queryName
						if v, ok := s.cache.Get(stmtName); !ok {
							msg.UnknownExec = true
							glog.V(5).Infof("[%v] [query][execute] no corresponding local statement found, stmtName: %v", s.name, stmtName)
						} else {
							msg.SQL = v.(string)
							msg.Raw = ""
							glog.V(6).Infof("[%v] [query][execute] stmtName: %v, sql: %v", s.name, stmtName, msg.SQL)
						}
					}
				case comStmtPrepare:
					// the statement will be registered if processed OK
					glog.V(6).Infof("[%v] [prepare] sql: %v", s.name, reqPacket.SQL())
				case comStmtExecute:
					stmtID := reqPacket.stmtID
					if v, ok := s.cache.Get(stmtID); !ok {
						// no statement, the corresponding prepare request has not been captured.
						msg.UnknownExec = true
						glog.V(5).Infof("[%v] [execute] no corresponding local statement found, stmtID: %v", s.name, stmtID)
					} else {
						msg.SQL = v.(string)
						msg.Raw = ""
						glog.V(6).Infof("[%v] [execute] stmtID: %v, sql: %v", s.name, stmtID, msg.SQL)
					}
				case comInitDB:
					glog.V(6).Infof("[%v] [init db] dbname: %v", s.name, reqPacket.dbname)
				default:
					// not the packet concerned, continue
					glog.V(8).Infof("[%v] receive unconcerned request packet", s.name)
					waitting = false
					continue
				}
				// request ready
				waitting = true
			} else {
				// parse server packet
				// Note: there may be many mysql packets in one tcp packet.
				// we only care about the first mysql packet,
				// which should only be the first part of tcp payload regardless of what the tcp packet seq is.
				if _, err = basePacket.DecodeFromBytes(tcp.Payload); err != nil {
					glog.V(6).Infof("[%v] parse response base packet failed: %v", s.name, err)
					continue
				}

				if basePacket.Seq() == mysqlReqSeq {
					handshake = true
					continue
				}

				if !waitting {
					// if there is no request, skip this packet ASAP
					continue
				}

				// filter
				if basePacket.Seq() != mysqlRspSeq {
					glog.V(8).Infof("[%v] skip unconcerned packet %v", s.name, tcp.Payload)
					continue
				}

				if err = basePacket.ParseResponsePacket(reqPacket.cmd, rspPacket); err != nil {
					glog.V(6).Infof("[%v] parse request packet failed: %v", s.name, err)
					continue
				}
				msg.TimestampRsp = packet.Metadata().Timestamp.UnixNano()
				msg.Latency = float32(msg.TimestampRsp-msg.TimestampReq) / 1000000

				// parse reponse and fill message
				switch rspPacket.flag {
				case iOK:
					msg.Err = false
					msg.AffectRows = rspPacket.affectedRows
					msg.ServerStatus = rspPacket.status
					switch reqPacket.CMD() {
					case comQuery:
						// if is a prepare query, register the SQL.
						if reqPacket.queryType == queryPrepare {
							s.cache.Remove(reqPacket.queryName)
							s.cache.Add(reqPacket.queryName, reqPacket.SQL())
							glog.V(6).Infof("[%v] [query][prepare] response OK, stmtName: %v, sql: %v", s.name, reqPacket.queryName, reqPacket.SQL())
						}
					case comStmtPrepare:
						// register the prepare statement.
						s.cache.Remove(rspPacket.stmtID)
						s.cache.Add(rspPacket.stmtID, reqPacket.SQL())
						glog.V(6).Infof("[%v] [prepare] response OK, stmtID: %v, sql: %v", s.name, rspPacket.stmtID, reqPacket.SQL())
					case comInitDB:
						s.dbname = reqPacket.dbname
						glog.V(6).Infof("[%v] [init db] response OK, dbname: %v", s.name, reqPacket.dbname)
					}
				case iERR:
					msg.Err = true
					msg.ErrMsg = rspPacket.message
					msg.Errno = rspPacket.errno
				default:
					// response for SELECT
					msg.Err = false
				}

				// report
				// don't report those message without SQL.
				// there is no SQL in prepare message.
				// need more precise filter about control command such as START, END.
				if (len(msg.SQL) > 5 || msg.UnknownExec) && msg.Latency < maxSpan {
					// fill client and server info
					msg.ServerIP = s.localIP
					msg.ServerPort = s.localPort
					msg.ClientIP = s.clientIP
					msg.ClientPort = s.clientPort
					// set db name
					if len(s.dbname) != 0 {
						msg.DB = s.dbname
					} else {
						// find db name
						clientAddr := fmt.Sprintf("%s:%v", s.clientIP, s.clientPort)
						if info := s.assembly.watcher.Get(clientAddr); info != nil {
							msg.DB = string(info.DB)
						} else {
							msg.DB = unknowDbName
						}
					}
					msg.AssemblyKey, _, _ = msg.AssemblyHashKey()

					glog.V(6).Infof("[%v] mysql query parsed done: %v", s.name, msg.SQL)

					s.assembly.out <- msg
					msg = nil
				}
				waitting = false
			}
		case <-s.stop:
			glog.V(6).Infof("[%v] close stream", s.name)
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
	watcher   *util.ConnectionWatcher // wathcer to get connection info
}

// Assemble send the packet to specify stream
func (a *Assembly) Assemble(packet gopacket.Packet) {
	key := Key{packet.NetworkLayer().NetworkFlow(), packet.TransportLayer().TransportFlow()}
	reverse := Key{key.net.Reverse(), key.transport.Reverse()}
	var s *MysqlStream
	if a.streamMap[key] != nil {
		s = a.streamMap[key]
	} else {
		s = a.streamMap[reverse]
	}

	if s == nil {
		var serverIP, clientIP string
		var serverPort, clientPort uint16
		if a.isRequest(key.net, key.transport) {
			serverIP = key.net.Dst().String()
			serverPort = binary.BigEndian.Uint16(key.transport.Dst().Raw())
			clientIP = key.net.Src().String()
			clientPort = binary.BigEndian.Uint16(key.transport.Src().Raw())
		} else {
			serverIP = key.net.Src().String()
			serverPort = binary.BigEndian.Uint16(key.transport.Src().Raw())
			clientIP = key.net.Dst().String()
			clientPort = binary.BigEndian.Uint16(key.transport.Dst().Raw())
		}

		s = newMysqlStream(a, serverIP, serverPort, clientIP, clientPort, key)
		a.streamMap[key] = s
	}
	s.lastSeen = packet.Metadata().Timestamp
	s.in <- packet
}

// CloseOlderThan remove those streams expired and return the number of them
func (a *Assembly) CloseOlderThan(t time.Time) int {
	count := 0
	packetNum := 0
	cacheItemNum := 0
	for k, v := range a.streamMap {
		if v.lastSeen.Before(t) {
			count++
			v.close()
			delete(a.streamMap, k)
			packetNum += len(v.in)
			cacheItemNum += v.cache.Len()
		}
	}
	glog.V(3).Infof("[%v] packets: %d, cache items: %d", a.wname, packetNum, cacheItemNum)
	return count
}
