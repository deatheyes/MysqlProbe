package util

import (
	"net"
)

// GetLocalIPs return all the address
func GetLocalIPs() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	var ret []string
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}

		if ipnet.IP.To4() != nil {
			ret = append(ret, ipnet.IP.String())
		}
	}
	return ret, nil
}
