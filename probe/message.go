package probe

import (
	"time"
)

type Message struct {
	Sql          string    // templated sql
	Err          error     // package process error
	TimestampReq time.Time // timestamp for request package
	TimestampRsp time.Time // timestamp for response package
}
