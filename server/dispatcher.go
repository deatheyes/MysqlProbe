package server

import (
	"net/http"

	"github.com/golang/glog"
)

// Dispatcher is a hub to serve websocket, it runs on every node in the cluster
type Dispatcher struct {
	clients    map[*Client]bool // registered clients
	broadcast  chan []byte      // inbound messages
	register   chan *Client     // client register channel
	unregister chan *Client     // client unregister channel
	pusher     *Pusher          // pool to push message
}

// NewDispatcher create a new Dispatcher object
func NewDispatcher(pusher *Pusher) *Dispatcher {
	return &Dispatcher{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		pusher:     pusher,
	}
}

// Run starts the push process
func (d *Dispatcher) Run() {
	for {
		select {
		case client := <-d.register:
			// TODO: check if exist
			glog.V(6).Info("dispatcher register client")
			d.clients[client] = true
		case client := <-d.unregister:
			glog.V(6).Info("dispatcher unregister client")
			if _, ok := d.clients[client]; ok {
				delete(d.clients, client)
				close(client.send)
			}
		case message := <-d.broadcast:
			glog.V(6).Info("dispatcher receive report")
			// push data to dynamic servers
			for client := range d.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(d.clients, client)
				}
			}
			// push data to static server pool
			if d.pusher != nil {
				select {
				case d.pusher.send <- message:
				default:
					glog.Warning("[pusher] queue full")
				}
			}
		}
	}
}

// In return the broadcast channel
func (d *Dispatcher) In() chan<- []byte {
	return d.broadcast
}

func (d *Dispatcher) ProcessData(data []byte) {
	// dispatcher don't need to read any data
}

func (d *Dispatcher) Register() chan<- *Client {
	return d.register
}

func (d *Dispatcher) Unregister() chan<- *Client {
	return d.unregister
}

func serveWs(dispatcher *Dispatcher, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		glog.Warningf("http connection upgrade failed: %v", err)
		return
	}

	// we don't care about 'dead' and 'retry' of the client in server mode
	client := &Client{hub: dispatcher, conn: conn, send: make(chan []byte, 256), ping: true}
	client.hub.Register() <- client
	go client.writePump()
	go client.readPump()
}
