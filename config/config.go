package config

import (
	"io/ioutil"
	"os"
	"path"

	"gopkg.in/yaml.v2"
)

// Config holds the parameters starting  probe, server and cluster
type Config struct {
	Slave           bool              `yaml:"slave"`           // run as slave in the cluster
	Port            uint16            `yaml:"serverport"`      // server port
	Interval        uint16            `yaml:"interval"`        // report interval
	SlowThresholdMs int64             `yaml:"slowthresholdms"` // threshold to record slow query
	Cluster         Cluster           `yaml:"cluster"`         // cluster config
	Probe           Probe             `yaml:"probe"`           // probe conifg, only slave will start a probe
	Pusher          Pusher            `yaml:"pusher"`          // pusher config
	Watcher         ConnectionWatcher `yaml:"watcher"`         // connection watcher config
	Websocket       Websocket         `yaml:"websocket"`       // websocket config
	Role            string            `yaml:"-"`               // role of this node
	Path            string            `yaml:"-"`               // config file path
}

// Cluster specify the arguments to run cluster
type Cluster struct {
	Gossip bool   `yaml:"gossip"` // if run as gossip cluster mode
	Group  string `yaml:"group"`  // cluster name
	Port   uint16 `yaml:"port"`   // gossip binding port, random if not set
}

// Probe specify the arguments to initialize pcap
type Probe struct {
	Device  string `yaml:"device"`       // local devices probe monitor, splited by ','
	Port    uint16 `yaml:"port"`         // port for bpf filter
	SnapLen int32  `yaml:"snappylength"` // snappy buffer length
	Workers int    `yaml:"workers"`      // worker number
}

// Pusher specify the arguments to create static receiver pool
type Pusher struct {
	Servers    string `yaml:"servers"`    // server list splited by ','
	Path       string `yaml:"path"`       // websocket path
	Preconnect bool   `yaml:"preconnect"` // preconnect to all servers
}

// ConnectionWatcher is the config of util.ConnectionWatcher
type ConnectionWatcher struct {
	Uname    string `yaml:"uname"`
	Password string `yaml:"password"`
}

// Websocket is the config of websocket client and server
type Websocket struct {
	ConnectTimeoutMs int   `yaml:"connecttimeoutms"`
	WriteTimeoutMs   int   `yaml:"writetimeoutms"`
	PingTimeoutS     int   `yaml:"pingtimeouts"`
	ReconnectPeriodS int   `yaml:"reconnectperiods"`
	MaxMessageSize   int64 `yaml:"maxmessagesize"`
}

// ReadFile load config from file
func ReadFile(file string) (*Config, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	c := &Config{}
	if err := yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}
	return c, nil
}

// ToBytes marshal the config
func ToBytes(config *Config) []byte {
	data, _ := yaml.Marshal(config)
	return data
}

// Seeds holds the basic cluster infomation
type Seeds struct {
	Epic  uint64   `yaml:"epic"`
	Name  string   `yaml:"name"`
	Addrs []string `yaml:"seeds"`
	Role  string   `yaml:"role"`
}

// SeedsFromFile read seed from file
func SeedsFromFile(file string) (*Seeds, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	s := &Seeds{}
	if err := yaml.Unmarshal(data, s); err != nil {
		return nil, err
	}
	return s, nil
}

// SeedsToBytes marshal the seeds
func SeedsToBytes(s *Seeds) []byte {
	data, _ := yaml.Marshal(s)
	return data
}

// SeedsToFile write the seeds to file
func SeedsToFile(seeds *Seeds, file string) error {
	dir := path.Dir(file)
	tmpfile, err := ioutil.TempFile(dir, "seeds")
	if err != nil {
		return err
	}
	defer os.Remove(tmpfile.Name())

	content, err := yaml.Marshal(seeds)
	if err != nil {
		return err
	}

	if _, err := tmpfile.Write(content); err != nil {
		return err
	}

	if err := tmpfile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpfile.Name(), file); err != nil {
		return err
	}
	return nil
}
