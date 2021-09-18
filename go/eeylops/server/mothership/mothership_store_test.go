package mothership

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	"eeylops/util/testutil"
	"fmt"
	"github.com/golang/glog"
	"testing"
	"time"
)

func TestMothershipStore(t *testing.T) {
	testutil.LogTestMarker("TestMothershipStore")
	testDir := testutil.CreateFreshTestDir("TestMothershipStore")
	numTopics := 100
	closeReopenIterNum := 5
	readVerifyIterNum := 5
	ts := NewMothershipStore(testDir)
	topicNameGen := func(val int) string {
		return fmt.Sprintf("topic-%d", val)
	}
	glog.Infof("Mothership store initialized!")
	lts := int64(0)
	for ii := 1; ii <= numTopics; ii++ {
		glog.Infof("Adding topic: %d", ii)
		// Add a new topic.
		var topic base.TopicConfig
		topic.Name = topicNameGen(ii)
		topic.PartitionIDs = []int{0, 1, 2, 3}
		topic.TTLSeconds = 86400
		topic.CreatedAt = time.Now()
		if err := ts.AddTopic(topic, int64(100+ii)); err != nil {
			glog.Fatalf("Unable to add topic due to err: %s", err.Error())
			return
		}
		lts++
		if lts != ts.topicsConfigLogicalTimestamp {
			glog.Fatalf("Invalid logical timestamp: %d. Expected: %d", ts.topicsConfigLogicalTimestamp, lts)
			return
		}
		// Close and reopen the topic store every 5 iterations.
		if ii%closeReopenIterNum == 0 {
			glog.Infof("Closing and reopening mothership store")
			if err := ts.Close(); err != nil {
				glog.Fatalf("Unable to close mothership store due to err: %s", err.Error())
				return
			}
			ts = NewMothershipStore(testDir)
		}
		// Read and verify all the topics every readVerifyIterNum iterations.
		if ii%readVerifyIterNum == 0 {
			for jj := 1; jj <= ii; jj++ {
				topicName := topicNameGen(jj)
				rtopic, err := ts.GetTopicByName(topicName)
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

	allTopics := ts.GetAllTopics()
	if len(allTopics) != numTopics {
		glog.Fatalf("Num topics mismatch. Expected: %d, got: %d", numTopics, len(allTopics))
	}

	// Remove topics.
	for ii := 1; ii <= numTopics; ii++ {
		err := ts.RemoveTopic(base.TopicIDType(ii), int64(10000+ii))
		if err != nil {
			glog.Fatalf("Unable to remove topic due to err: %s", err.Error())
		}
		lts++
		if lts != ts.topicsConfigLogicalTimestamp {
			glog.Fatalf("Invalid logical timestamp: %d. Expected: %d", ts.topicsConfigLogicalTimestamp, lts)
			return
		}
		// Close and reopen the topic store every 5 iterations.
		if ii%closeReopenIterNum == 0 {
			glog.Infof("Closing and reopening mothership store")
			if err := ts.Close(); err != nil {
				glog.Fatalf("Unable to close mothership store due to err: %s", err.Error())
				return
			}
			ts = NewMothershipStore(testDir)
		}
		if ii%readVerifyIterNum == 0 {
			for jj := 1; jj <= numTopics; jj++ {
				topicName := topicNameGen(jj)
				tpc, err := ts.GetTopicByName(topicName)
				if jj <= ii {
					if err != storage.ErrTopicNotFound {
						glog.Fatalf("Expected topic: %s to not be found but got err: %v. Topic: %s",
							topicName, err, tpc.ToString())
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
