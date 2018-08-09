package server

import (
	"errors"
	"math/rand"
	"net/url"
	"time"

	"github.com/golang/glog"
	"github.com/gorilla/websocket"
)

// Slot keep the connection for a server
type Slot struct {
	addr        string
	path        string
	connections []*Client
	dialer      *websocket.Dialer
}

func (s *Slot) newClient() (*Client, error) {
	u := url.URL{Scheme: "ws", Host: s.addr, Path: s.path}
	conn, _, err := s.dialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}
	client := &Client{hub: nil, conn: conn, send: make(chan []byte, 256), dead: false, retry: 0}
	go client.readPump()
	go client.writePump()
	s.connections = append(s.connections, client)
	return client, nil
}

func (s *Slot) getClient() (*Client, error) {
	if len(s.connections) == 0 {
		return s.newClient()
	}

	id := rand.Int() % len(s.connections)
	client := s.connections[rand.Int()%len(s.connections)]
	if !client.dead {
		return client, nil
	}
	s.connections = append(s.connections[:id], s.connections[id+1:]...)
	return s.newClient()
}

// ClientPool keep the connections for pushing data
// TODO: perference support
type ClientPool struct {
	slots  []*Slot
	dialer *websocket.Dialer
	path   string
}

func newClientPool(servers []string, path string, preconnect bool) *ClientPool {
	p := &ClientPool{
		dialer: &websocket.Dialer{HandshakeTimeout: time.Duration(50) * time.Millisecond},
		path:   path,
	}

	for _, server := range servers {
		slot := &Slot{dialer: p.dialer, path: p.path, addr: server}
		p.slots = append(p.slots, slot)
		if preconnect {
			_, err := slot.getClient()
			if err != nil {
				glog.Warningf("[pool] preconnect to %v%v failed: %v", slot.addr, slot.path, err)
			}
		}
	}
	return p
}

func (p *ClientPool) getClient() (*Client, error) {
	length := len(p.slots)
	if length == 0 {
		return nil, errors.New("no server list")
	}

	pos := rand.Int() % len(p.slots)
	for i := 0; i < length; i++ {
		pos = (pos + i) % len(p.slots)
		slot := p.slots[pos]
		client, err := slot.getClient()
		if err != nil {
			glog.Warningf("[pool] get client of %v%v failed: %v", slot.addr, slot.path, err)
			continue
		}
		return client, nil
	}
	return nil, errors.New("[pool] all servers failed")
}

// Pusher always push the message to one server in the pool
// TODO: a instance of special Client may be a cute implementation
type Pusher struct {
	pool *ClientPool
	send chan []byte
}

func newPusher(servers []string, path string, preconnect bool) *Pusher {
	p := &Pusher{
		pool: newClientPool(servers, path, preconnect),
		send: make(chan []byte, 256),
	}
	go p.Run()
	return p
}

// Run start the push process
func (p *Pusher) Run() {
	for {
		m := <-p.send
		client, err := p.pool.getClient()
		if err != nil {
			glog.Warningf("[pusher] push message failed: %v", err)
		} else {
			select {
			case client.send <- m:
			default:
				close(client.send)
				glog.Warningf("[pusher] push message to %v failed: queue full", client.conn.RemoteAddr())
			}
		}
	}
}
