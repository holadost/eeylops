package storage

import (
	"eeylops/server/base"
	"eeylops/util"
	"fmt"
	"github.com/golang/glog"
	"os"
	"testing"
)

func createTopicStoreTestDir(t *testing.T, testName string) string {
	dataDir := fmt.Sprintf("/tmp/topic_store_test/%s", testName)
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

func TestTopicStore(t *testing.T) {
	util.LogTestMarker("TestTopicStore")
	testDir := createTopicStoreTestDir(t, "TestTopicStore")
	numTopics := 100
	closeReopenIterNum := 5
	readVerifyIterNum := 10
	markForRemovalIterNum := 15
	ts := NewTopicStore(testDir)
	for ii := 0; ii < numTopics; ii++ {

		// Add a new topic.
		var topic base.Topic
		topic.Name = fmt.Sprintf("topic-%d", ii)
		topic.ToRemove = false
		topic.PartitionIDs = []int{0, 1, 2, 3}
		topic.TTLSeconds = 86400
		if err := ts.AddTopic(topic); err != nil {
			glog.Fatalf("Unable to add topic due to err: %s", err.Error())
			return
		}

		// Close and reopen the topic store every 5 iterations.
		if ii%closeReopenIterNum == 0 {
			glog.Infof("Closing and reopening topic store")
			if err := ts.Close(); err != nil {
				glog.Fatalf("Unable to close topic store due to err: %s", err.Error())
				return
			}
			ts = NewTopicStore(testDir)
		}

		// Mark the current topic for removal every 15th iteration.
		if ii%markForRemovalIterNum == 0 {
			glog.Infof("Marking topic: %s for removal", topic.Name)
			if err := ts.MarkTopicForRemoval(topic.Name); err != nil {
				glog.Fatalf("Unable to mark topic: %s for removal due to err: %s", topic.Name, err.Error())
				return
			}
		}

		// Read and verify all the topics every 10 iterations.
		if ii%readVerifyIterNum == 0 {
			for jj := 0; jj < ii+1; jj++ {
				topicName := fmt.Sprintf("topic-%d", jj)
				rtopic, err := ts.GetTopic(topicName)
				if err != nil {
					glog.Fatalf("Failed to get topic: %s due to err: %s", topicName, err.Error())
					return
				}
				if rtopic.Name != topicName {
					glog.Fatalf("Topic Name mismatch. Expected: %s, Got: %s", topicName, rtopic.Name)
				}
				if jj%15 == 0 {
					if !rtopic.ToRemove {
						glog.Fatalf("Topic: %s was not marked for removal", rtopic.Name)
						return
					}
				}
			}
		}
	}
	glog.Infof("Topic store test finished successfully")
}
