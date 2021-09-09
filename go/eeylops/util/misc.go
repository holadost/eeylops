package util

import (
	"encoding/binary"
	"errors"
	"github.com/golang/glog"
	"math"
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

func RenameDir(oldPath string, newPath string) {
	glog.Infof("Renaming directory: %s to %s", oldPath, newPath)
	if err := os.Rename(oldPath, newPath); err != nil {
		glog.Fatalf("Unable to rename dir: %s to %s", oldPath, newPath)
	}
}

func ContainsInt(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func MaxInt(a int64, b int64) int64 {
	if a <= b {
		return b
	}
	return a
}

func MaxUint(a uint64, b uint64) uint64 {
	if a <= b {
		return b
	}
	return a
}

func MinInt(a int64, b int64) int64 {
	if a <= b {
		return a
	}
	return b
}

func MinUint(a uint64, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

var emptySliceErr = errors.New("empty slice")

func MaxIntSlice(a []int64) (int64, error) {
	if len(a) == 0 {
		return -1, emptySliceErr
	}
	max := int64(math.MinInt64)
	for _, e := range a {
		if e >= max {
			max = e
		}
	}
	return max, nil
}

func MaxUintSlice(a []uint64) (uint64, error) {
	if len(a) == 0 {
		return 0, emptySliceErr
	}
	max := uint64(0)
	for _, e := range a {
		if e >= max {
			max = e
		}
	}
	return max, nil
}

func MinIntSlice(a []int64) (int64, error) {
	if len(a) == 0 {
		return -1, emptySliceErr
	}
	min := int64(math.MaxInt64)
	for _, e := range a {
		if e <= min {
			min = e
		}
	}
	return min, nil
}

func MinUintSlice(a []uint64) (uint64, error) {
	if len(a) == 0 {
		return -1, emptySliceErr
	}
	min := uint64(math.MaxUint64)
	for _, e := range a {
		if e <= min {
			min = e
		}
	}
	return min, nil
}
