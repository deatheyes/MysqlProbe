package server

import (
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/gorilla/websocket"
)

var (
	connectTimeout       = time.Second
	writeTimeout         = time.Second
	pingTimeout          = 30 * time.Second
	retryPeriod          = 10 * time.Second
	maxMessageSize int64 = 1 << 24
)

func pingPeriod() time.Duration {
	return pingTimeout * 9 / 10
}

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
	ping  bool            // if need to ping the peer
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod())
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
			if c.ping {
				c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					// client closed unexpected
					glog.Warningf("ping client failed: %v", err)
					return
				}
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
	c.conn.SetReadDeadline(time.Now().Add(pingTimeout))
	c.conn.SetPongHandler(
		func(string) error {
			c.conn.SetReadDeadline(time.Now().Add(pingTimeout))
			return nil
		},
	)

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				glog.Warningf("connection closed unexpected: %v", err)
			} else {
				glog.Warningf("read data failed: %v", err)
			}
			return
		}
		if c.hub != nil {
			c.hub.ProcessData(data)
		}
	}
}
