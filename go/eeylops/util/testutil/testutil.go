package testutil

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

// CreateFreshTestDir creates a fresh directory. If a directory already exists, it is first deleted and then
// recreated.
func CreateFreshTestDir(dirName string) string {
	dataDir := fmt.Sprintf("/tmp/%s", dirName)
	glog.Infof("Deleting and recreating directory: %s", dataDir)
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
