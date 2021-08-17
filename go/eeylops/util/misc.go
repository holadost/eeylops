package util

import (
	"encoding/binary"
	"github.com/golang/glog"
	"os"
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

func CreateDir(dirName string) {
	glog.Infof("Creating directory: %s if not exists", dirName)
	if err := os.MkdirAll(dirName, 0774); err != nil {
		glog.Fatalf("Unable to create test dir: %s", dirName)
	}
}
