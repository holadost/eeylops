package util

import "encoding/binary"

func UintToBytes(num uint64) []byte {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, num)
	return val
}

func BytesToUint(num []byte) uint64 {
	return binary.BigEndian.Uint64(num)
}
