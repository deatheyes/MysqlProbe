package cluster

import (
	"time"
)

var joinRetryInterval = 5 * time.Second
var refreshSeedInterval = 30 * time.Second
