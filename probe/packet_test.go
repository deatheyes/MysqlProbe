package probe

import (
	"testing"
)

func TestParseBasePacket(t *testing.T) {
	data := []byte{0x01, 0x00, 0x00, 0x01, 0x00}
	packet := &MysqlBasePacket{}
	len, err := packet.DecodeFromBytes(data)
	if err != nil {
		t.Error(err)
	}

	want := 5
	if len != want {
		t.Errorf("unexpected length: %v, want: %v", len, want)
	}
}
