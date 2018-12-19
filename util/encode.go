package util

import (
	"crypto/md5"
	"fmt"
)

// ReadLengthEncodedInteger is a length decoder
func ReadLengthEncodedInteger(b []byte) (uint64, bool, int) {
	if len(b) == 0 {
		return 0, true, 1
	}

	switch b[0] {
	// 251: NULL
	case 0xfb:
		return 0, true, 1

	// 252: value of following 2
	case 0xfc:
		return uint64(b[1]) | uint64(b[2])<<8, false, 3

	// 253: value of following 3
	case 0xfd:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, false, 4

	// 254: value of following 8
	case 0xfe:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
				uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
				uint64(b[7])<<48 | uint64(b[8])<<56,
			false, 9
	}

	// 0-250: value of first byte
	return uint64(b[0]), false, 1
}

// ReadStatus return the mysql reponse status
func ReadStatus(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}

// Hash return the hash code of a string
func Hash(key string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(key)))
}
