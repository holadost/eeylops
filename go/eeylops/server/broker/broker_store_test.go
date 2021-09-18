package broker

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	"eeylops/util/testutil"
	"github.com/golang/glog"
	"os"
	"testing"
	"time"
)

func TestBrokerStore(t *testing.T) {
	testutil.LogTestMarker("TestBrokerStore")
	testDir := testutil.CreateFreshTestDir("TestBrokerStore")
	scanIntervalSecs := 5
	opts := BrokerStoreOpts{
		RootDirectory:           testDir,
		BrokerID:                "1",
		StoreGCScanIntervalSecs: scanIntervalSecs,
	}
	brokerStore := NewBrokerStore(opts)
	topicName := "topic1"
	var topic base.TopicConfig
	topic.Name = topicName
	topic.PartitionIDs = []int{2, 4, 6, 8}
	topic.TTLSeconds = 86400 * 7
	topic.ID = 1
	topic.CreatedAt = time.Now()
	if err := brokerStore.AddTopic(topic, 100); err != nil {
		glog.Fatalf("Unable to add topic due to err: %s", err.Error())
	}
	err := brokerStore.AddTopic(topic, 101)
	if err == storage.ErrTopicExists {
		glog.V(1).Infof("Topic was not updated as expected")
	} else {
		glog.Fatalf("Added topic: %s even though we should not have. Error: %v", topicName, err)
	}

	tp, err := brokerStore.GetTopicByName(topicName)
	if err != nil {
		glog.Fatalf("Unable to fetch topic due to err: %s", err.Error())
	}
	glog.V(1).Infof("Topic: %v", tp)

	_, err = brokerStore.GetTopicByName("topic2")
	if err != storage.ErrTopicNotFound {
		glog.Fatalf("Fetched a topic that was never created")
	}
	tpc, err := brokerStore.GetTopicByID(topic.ID)
	if err != nil {
		glog.Fatalf("Unable to fetch topic by ID due to err: %s", err.Error())
	}
	if tpc.Name != topicName || tpc.ID != topic.ID || tpc.TTLSeconds != topic.TTLSeconds {
		glog.Fatalf("Mismatch in topic. Expected: \n%v, \n\nGot: \n%v", topic, tpc)
	}
	for ii, id := range topic.PartitionIDs {
		if id != tpc.PartitionIDs[ii] {
			glog.Fatalf("Partition mismatch. Expected: %d, Got: %d", id, tpc.PartitionIDs[ii])
		}
	}
	err = brokerStore.RemoveTopic(topic.ID, 102)
	if err != nil {
		glog.Fatalf("Failed to mark topic for removal due to err: %s", err.Error())
	}
	tp, err = brokerStore.GetTopicByName(topicName)
	if err == storage.ErrTopicNotFound {
		glog.V(1).Infof("Did not find topic as expected after it was deleted")
	} else {
		glog.Fatalf("Got topic even though it was deleted?")
	}

	glog.Infof("Waiting for manager to reclaim the topic directory")
	time.Sleep(2 * time.Second)
	_, err = os.Stat(brokerStore.getTopicDirectory(topic.Name, topic.ID))
	glog.V(1).Infof("Stat Err: %v", err)
	if err == nil {
		glog.Fatalf("Directory should have been deleted but it wasn't")
	}
	glog.Infof("Topic brokerStore test finished successfully")
}
