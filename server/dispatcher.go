package server

import (
	"net/http"

	"github.com/golang/glog"
)

// dispatcher is a hub to serve websocket, it runs on every node in the cluster
type Dispatcher struct {
	clients    map[*Client]bool // registered clients
	broadcast  chan []byte      // inbound messages
	register   chan *Client     // client register channel
	unregister chan *Client     // client unregister channel
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (d *Dispatcher) Run() {
	for {
		select {
		case client := <-d.register:
			// TODO: check if exist
			d.clients[client] = true
		case client := <-d.unregister:
			if _, ok := d.clients[client]; ok {
				delete(d.clients, client)
				close(client.send)
			}
		case message := <-d.broadcast:
			for client := range d.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(d.clients, client)
				}
			}
		}
	}
}

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
	client := &Client{hub: dispatcher, conn: conn, send: make(chan []byte, 256)}
	client.hub.Register() <- client
	go client.writePump()
	go client.readPump()
}
