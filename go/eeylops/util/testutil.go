package util

import (
	"fmt"
	"github.com/golang/glog"
	"os"
	"testing"
)

func CreateTestDir(t *testing.T, testName string) string {
	dataDir := fmt.Sprintf("/tmp/%s", testName)
	err := os.RemoveAll(dataDir)
	if err != nil {
		t.Fatalf("Unable to delete test directory: %s", dataDir)
	}
	err = os.MkdirAll(dataDir, 0774)
	if err != nil {
		glog.Fatalf("Unable to create test dir: %s", dataDir)
	}
	return dataDir
}

func CreateDir(dirName string) string {
	dataDir := fmt.Sprintf("/tmp/%s", dirName)
	err := os.RemoveAll(dataDir)
	if err != nil {
		glog.Fatalf("Unable to delete test directory: %s", dataDir)
	}
	err = os.MkdirAll(dataDir, 0774)
	if err != nil {
		glog.Fatalf("Unable to create test dir: %s", dataDir)
	}
	return dataDir
}

func LogTestMarker(testName string) {
	glog.InfoDepth(1, fmt.Sprintf("\n\n============================================================ %s "+
		"============================================================\n\n", testName))
}
