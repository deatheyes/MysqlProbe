package config

import (
	"io/ioutil"
	"os"
	"path"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Slave    bool    `yaml:"slave"`      // run as slave in the cluster
	Port     uint16  `yaml:"serverport"` // server port
	Interval uint16  `yaml:"interval"`   // report interval
	Cluster  Cluster `yaml:"cluster"`    // cluster config
	Probe    Probe   `yaml:"probe"`      // probe conifg, only slave will start a probe
	Role     string  `yaml:-`            // role of this node
	Path     string  `yaml:-`            // config file path
}

type Cluster struct {
	Gossip bool   `yaml:"gossip"` // if run as gossip cluster mode
	Group  string `yaml:"group"`  // cluster name
	Port   uint16 `yaml:'port'`   // gossip bind port, random if not set
}

type Probe struct {
	Device  string `yaml:"device"`       // local device probe monitor
	Port    uint16 `yaml:"port"`         // port for bpf filter
	SnapLen int32  `yaml:"snappylength"` // snappy buffer length
	Workers int    `yaml:"workers"`      // worker number
}

func ConfigFromFile(file string) (*Config, error) {
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

func ConfigToBytes(config *Config) []byte {
	data, _ := yaml.Marshal(config)
	return data
}

type Seeds struct {
	Addrs []string `yaml:"seeds"`
}

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

func SeedsToBytes(s *Seeds) []byte {
	data, _ := yaml.Marshal(s)
	return data
}

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
