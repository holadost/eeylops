package storage

import (
	"eeylops/server/base"
	"eeylops/util"
	"fmt"
	"github.com/golang/glog"
	"os"
	"testing"
	"time"
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

func TestTopicConfigStore(t *testing.T) {
	util.LogTestMarker("TestTopicConfigStore")
	testDir := createTopicStoreTestDir(t, "TestTopicConfigStore")
	numTopics := 100
	closeReopenIterNum := 5
	readVerifyIterNum := 5
	ts := NewTopicsConfigStore(testDir)
	topicNameGen := func(val int) string {
		return fmt.Sprintf("topic-%d", val)
	}
	for ii := 0; ii < numTopics; ii++ {
		// Add a new topic.
		var topic base.TopicConfig
		topic.Name = topicNameGen(ii)
		topic.PartitionIDs = []int{0, 1, 2, 3}
		topic.TTLSeconds = 86400
		topic.ID = base.TopicIDType(ii)
		topic.CreatedAt = time.Now()
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
			ts = NewTopicsConfigStore(testDir)
		}
		// Read and verify all the topics every readVerifyIterNum iterations.
		if ii%readVerifyIterNum == 0 {
			for jj := 0; jj < ii+1; jj++ {
				topicName := topicNameGen(jj)
				rtopic, err := ts.GetTopic(topicName)
				if err != nil {
					glog.Fatalf("Failed to get topic: %s due to err: %s", topicName, err.Error())
					return
				}
				if rtopic.Name != topicName {
					glog.Fatalf("Topic Name mismatch. Expected: %s, Got: %s", topicName, rtopic.Name)
				}
			}
		}
	}

	allTopics, err := ts.GetAllTopics()
	if err != nil {
		glog.Fatalf("Hit an unexpected error while fetching all topics: %s", err.Error())
	}
	if len(allTopics) != numTopics {
		glog.Fatalf("Num topics mismatch. Expected: %d, got: %d", numTopics, len(allTopics))
	}

	// Remove topics.
	for ii := 0; ii < numTopics; ii++ {
		err := ts.RemoveTopic(topicNameGen(ii))
		if err != nil {
			glog.Fatalf("Unable to remove topic due to err: %s", err.Error())
		}
		if ii%readVerifyIterNum == 0 {
			for jj := 0; jj < numTopics; jj++ {
				topicName := topicNameGen(jj)
				_, err := ts.GetTopic(topicName)
				if jj <= ii {
					if err != ErrTopicNotFound {
						glog.Fatalf("Expected topic to not be found but got err: %v", err)
					}
				} else {
					if err != nil {
						glog.Fatalf("Got an unexpected error: %v while fetching topic: %s", err, topicName)
					}
				}
			}
		}
	}
	glog.Infof("Topic store test finished successfully")
}
