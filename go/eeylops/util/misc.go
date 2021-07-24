package util

import (
	"encoding/binary"
	"github.com/golang/glog"
)

func UintToBytes(num uint64) []byte {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, num)
	return val
}

func BytesToUint(num []byte) uint64 {
	if len(num) != 8 {
		glog.Fatalf("Expected 8 bytes, got: %d", len(num))
	}
	return binary.BigEndian.Uint64(num)
}
