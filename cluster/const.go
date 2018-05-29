package cluster

import (
	"time"
)

var joinRetryInterval = time.Second * 5
var refreshSeedInterval = time.Minute
