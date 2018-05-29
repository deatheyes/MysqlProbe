package server

type Hub interface {
	ProcessData(data []byte)
	Register() chan<- *Client
	Unregister() chan<- *Client
}
