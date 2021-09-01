package storage

import (
	"eeylops/server/base"
	"eeylops/util"
	"fmt"
	"github.com/golang/glog"
	"os"
	"testing"
)

func createConsumerStoreTestDir(t *testing.T, testName string) string {
	dataDir := fmt.Sprintf("/tmp/consumer_store_test/%s", testName)
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

func c(t *testing.T) {
	util.LogTestMarker("TestConsumerStore")
	testDir := createConsumerStoreTestDir(t, "TestConsumerStore")
	cs := NewConsumerStore(testDir)
	consumerID := "consumer1"
	topidName := base.TopicIDType(1)
	partitionID := uint(1)
	if err := cs.RegisterConsumer(consumerID, topidName, partitionID, 100); err != nil {
		glog.Fatalf("Unable to register consumer due to err: %s", err.Error())
		return
	}
	numCommits := 100
	for ii := 0; ii < numCommits; ii++ {
		if err := cs.Commit(consumerID, topidName, partitionID, base.Offset(ii), 101); err != nil {
			glog.Fatalf("Unable to commit for consumer due to err: %s", err.Error())
			return
		}
		if ii%5 == 0 {
			num, err := cs.GetLastCommitted(consumerID, topidName, partitionID)
			if err != nil {
				glog.Fatalf("Unable to get last committed offset due to err: %s", err.Error())
				return
			}
			if num != base.Offset(ii) {
				glog.Fatalf("Value Mismatch. Expected: %d, Got: %d", uint64(ii), num)
				return
			}
		}
	}
	_, err := cs.GetLastCommitted("consumer2", topidName, partitionID)
	if err == nil {
		glog.Fatalf("Got a committed offset even when we were not expecting it")
		return
	}
	err = cs.Close()
	if err != nil {
		glog.Fatalf("Failed to close the store due to err: %s", err.Error())
		return
	}
	glog.Infof("Consumer store test finished successfully")
}
