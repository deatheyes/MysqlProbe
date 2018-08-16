package server

import (
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/gorilla/websocket"
)

const (
	writeTimeout   = time.Second
	pongTimeout    = 30 * time.Second
	pingPeriod     = (pongTimeout * 9) / 10
	retryPeriod     = 10 * time.Second
	maxMessageSize = 1 << 24
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1 << 24,
	WriteBufferSize: 1 << 24,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Client is a integration of network data used by hub
type Client struct {
	hub   Hub             // hub to register this client
	conn  *websocket.Conn // websocket connection
	send  chan []byte     // channel of outbound messages
	dead  bool            // flag if the client is closed, used to detect a retry
	retry int             // count of retry
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
		c.dead = true
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				// channel has been closed by the hub
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				glog.V(8).Info("channel has been closed by the dispatcher")
				return
			}

			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				glog.Warningf("get next writer failed: %v", err)
				return
			}

			w.Write(message)

			if err := w.Close(); err != nil {
				glog.Warningf("close writer failed: %v", err)
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				// client closed unexpected
				glog.Warningf("ping client failed: %v", err)
				return
			}
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		if c.hub != nil {
			c.hub.Unregister() <- c
		}
		c.conn.Close()
		c.dead = true
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongTimeout))
	c.conn.SetPongHandler(
		func(string) error {
			c.conn.SetReadDeadline(time.Now().Add(pongTimeout))
			return nil
		},
	)

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				glog.Warningf("connection closed unexpected: %v", err)
			} else {
				glog.Warningf("read data failed: %v", err)
			}
			break
		}
		if c.hub != nil {
			c.hub.ProcessData(data)
		}
	}
}
