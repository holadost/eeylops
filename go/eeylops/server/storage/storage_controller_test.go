package storage

import (
	"eeylops/server/base"
	"fmt"
	"github.com/golang/glog"
	"os"
	"testing"
	"time"
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

func TestStorageController(t *testing.T) {
	glog.Infof("*******************************************************************************************\n\n")
	glog.Infof("Starting TestStorageController")
	scanIntervalSecs := 5
	opts := StorageControllerOpts{
		RootDirectory:           createTestDirForInstanceManager(t, "TestStorageController"),
		ControllerID:            "1",
		StoreGCScanIntervalSecs: scanIntervalSecs,
	}
	controller := NewStorageController(opts)
	topicName := "topic1"
	topic := base.Topic{
		Name:         topicName,
		PartitionIDs: []int{2, 4, 6, 8},
		TTLSeconds:   86400,
		ToRemove:     false,
	}
	if err := controller.AddTopic(topic); err != nil {
		glog.Fatalf("Unable to add topic due to err: %s", err.Error())
	}
	err := controller.AddTopic(topic)
	if err == ErrTopicExists {
		glog.V(1).Infof("Topic was not updated as expected")
	} else {
		glog.Fatalf("Added topic: %s even though we should not have. Error: %v", topicName, err)
	}

	tp, err := controller.GetTopic(topicName)
	if err != nil {
		glog.Fatalf("Unable to fetch topic due to err: %s", err.Error())
	}
	glog.V(1).Infof("Topic: %v", tp)

	_, err = controller.GetTopic("topic2")
	if err != ErrTopicNotFound {
		glog.Fatalf("Fetched a topic that was never created")
	}

	err = controller.RemoveTopic(topicName)
	if err != nil {
		glog.Fatalf("Failed to mark topic for removal due to err: %s", err.Error())
	}
	tp, err = controller.GetTopic(topicName)
	if err == ErrTopicNotFound {
		glog.V(1).Infof("Did not find topic as expected after it was deleted")
	} else {
		glog.Fatalf("Got topic even though it was deleted?")
	}

	glog.Infof("Waiting for janitor to reclaim the topic directory")
	time.Sleep(15 * time.Second)
	_, err = os.Stat(controller.getTopicRootDirectory(topicName))
	glog.V(1).Infof("Stat Err: %v", err)
	if err == nil {
		glog.Fatalf("Directory should have been deleted but it wasn't")
	}
	glog.Infof("Topic controller test finished successfully")
}
