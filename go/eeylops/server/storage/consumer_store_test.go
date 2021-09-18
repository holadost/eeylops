package storage

import (
	"eeylops/server/base"
	"eeylops/util/testutil"
	"github.com/golang/glog"
	"testing"
)

func TestConsumerStore(t *testing.T) {
	testutil.LogTestMarker("TestConsumerStore")
	testDir := testutil.CreateFreshTestDir("TestConsumerStore")
	cs := NewConsumerStore(testDir, "1")
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

func TestConsumerStoreCleanup(t *testing.T) {
	testutil.LogTestMarker("TestConsumerStoreCleanup")
	testDir := testutil.CreateFreshTestDir("TestConsumerStoreCleanup")
	cs := NewConsumerStore(testDir, "1")
	successTopID := base.TopicIDType(1)
	failTopID := base.TopicIDType(2)
	partitionID := uint(100)
	consumers := []string{"consumer1", "consumer2", "consumer3", "consumer4"}
	topIds := []base.TopicIDType{successTopID, failTopID}
	glog.Infof("Registering consumers!")
	for _, consumerID := range consumers {
		for _, topid := range topIds {
			if err := cs.RegisterConsumer(consumerID, topid, partitionID, 100); err != nil {
				glog.Fatalf("Unable to register consumer due to err: %s", err.Error())
				return
			}
		}

	}

	glog.Infof("Committing offsets for consumers!")
	numCommits := 50
	for ii := 0; ii < numCommits; ii++ {
		if ii%5 == 0 {
			if err := cs.Close(); err != nil {
				glog.Fatalf("Failed to close the store due to err: %s", err.Error())
				return
			}
			cs = NewConsumerStore(testDir, "1")
		}
		for _, consumerID := range consumers {
			for _, topid := range topIds {
				if err := cs.Commit(consumerID, topid, partitionID, base.Offset(ii), 101); err != nil {
					glog.Fatalf("Unable to commit for consumer due to err: %s", err.Error())
					return
				}
				num, err := cs.GetLastCommitted(consumerID, topid, partitionID)
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
	}
	if err := cs.Close(); err != nil {
		glog.Fatalf("Failed to close the store due to err: %s", err.Error())
		return
	}
	cs = NewConsumerStore(testDir, "1")

	glog.Infof("Removing non existent topic consumers from the store")
	cs.RemoveNonExistentTopicConsumers([]base.TopicIDType{failTopID})
	for _, consumerID := range consumers {
		_, err := cs.GetLastCommitted(consumerID, failTopID, partitionID)
		if err == nil {
			glog.Fatalf("Got a committed offset even when we were not expecting it")
			return
		}
		_, err = cs.GetLastCommitted(consumerID, successTopID, partitionID)
		if err != nil {
			glog.Fatalf("Error while expecting last committed offset: %v", err)
			return
		}
	}
}
