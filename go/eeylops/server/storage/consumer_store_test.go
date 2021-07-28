package storage

import (
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

func TestConsumerStore(t *testing.T) {
	util.LogTestMarker("TestConsumerStore")
	testDir := createConsumerStoreTestDir(t, "TestConsumerStore")
	cs := NewConsumerStore(testDir)
	consumerID := "consumer1"
	topidName := "topic1"
	partitionID := uint(1)
	if err := cs.RegisterConsumer(consumerID, topidName, partitionID); err != nil {
		glog.Fatalf("Unable to register consumer due to err: %s", err.Error())
		return
	}
	numCommits := 100
	for ii := 0; ii < numCommits; ii++ {
		if err := cs.Commit(consumerID, topidName, partitionID, uint64(ii)); err != nil {
			glog.Fatalf("Unable to commit for consumer due to err: %s", err.Error())
			return
		}
		if ii%5 == 0 {
			num, err := cs.GetLastCommitted(consumerID, topidName, partitionID)
			if err != nil {
				glog.Fatalf("Unable to get last committed offset due to err: %s", err.Error())
				return
			}
			if num != uint64(ii) {
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
