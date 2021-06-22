package hedwig

import (
	"fmt"
	"github.com/golang/glog"
	"os"
	"testing"
)

func createTestDirForInstanceManager(t *testing.T, testName string) string {
	dataDir := fmt.Sprintf("/tmp/topic_controller/%s", testName)
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

func TestTopicController(t *testing.T) {
	glog.Infof("*******************************************************************************************\n\n")
	glog.Infof("Starting TestTopicController")
	glog.Infof("Topic controller test finished successfully")
}
