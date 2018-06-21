package server

// Hub is the interface processing Client
type Hub interface {
	ProcessData(data []byte)
	Register() chan<- *Client
	Unregister() chan<- *Client
}
