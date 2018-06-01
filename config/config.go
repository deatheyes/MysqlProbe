package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Slave    bool    `yaml:"slave"`    // run as slave in the cluster
	Port     uint16  `yaml:"serverport"`  // server port
	Interval uint16  `yaml:"interval"` // report interval
	Cluster  Cluster `yaml:"cluster"`  // cluster config
	Probe    Probe   `yaml:"probe"`    // probe conifg, only slave will start a probe
}

type Cluster struct {
	Enable bool     `yaml:"enable"` // if run as cluster mode
	Seeds  []string `yaml:"seeds"`  // seeds to boot the cluster, if there is cluster node info file, this conifg will be ignored
	Group  string   `yaml:"group"`  // cluster name
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
	if err := yaml.Unmarshal([]byte(data), &c); err != nil {
		return nil, err
	}
	return c, nil
}

func ConfigToBytes(config *Config) []byte {
	data, _ := yaml.Marshal(config)
	return data
}
