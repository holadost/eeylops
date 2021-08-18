package hedwig

import (
	"context"
	"eeylops/comm"
	"eeylops/server/base"
	"eeylops/util"
	"fmt"
	"github.com/golang/glog"
	"testing"
	"time"
)

func TestInstanceManager_AddRemoveGetTopic(t *testing.T) {
	util.LogTestMarker("TestInstanceManager_AddRemoveGetTopic")
	testDirName := util.CreateTestDir(t, "TestInstanceManager_AddRemoveGetTopic")
	clusterID := "nikhil1nikhil1"
	opts := InstanceManagerOpts{
		DataDirectory: testDirName,
		ClusterID:     clusterID,
		PeerAddresses: nil,
	}
	generateTopicName := func(id int) string {
		return fmt.Sprintf("hello_topic_%d", id)
	}
	numTopics := 15
	im := NewInstanceManager(&opts)
	// Add topics.
	for ii := 0; ii < numTopics; ii++ {
		var req comm.CreateTopicRequest
		var topic comm.Topic
		topic.PartitionIds = []int32{1, 2, 3, 4}
		topic.TtlSeconds = 86400 * 7
		topic.TopicName = generateTopicName(ii)
		req.Topic = &topic
		err := im.AddTopic(context.Background(), &req)
		if err != nil {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic", err.Error())
		}
	}

	// Get topics.
	var allTopics []*base.TopicConfig
	for ii := 0; ii < numTopics; ii++ {
		var req comm.GetTopicRequest
		req.ClusterId = clusterID
		req.TopicName = generateTopicName(ii)
		topic, err := im.GetTopic(context.Background(), &req)
		if err != nil {
			glog.Fatalf("Error while fetching topic: %s", generateTopicName(ii))
		}
		glog.Infof("Received topic: %s, ID: %d", topic.Name, topic.ID)
		allTopics = append(allTopics, topic)
	}

	// Get all topics.
	topics := im.GetAllTopics(context.Background())
	if len(topics) != numTopics {
		glog.Fatalf("Did not get all the topics as expected! Expected: %d, Got: %d", numTopics, len(topics))
	}

	// Remove topics.
	for ii := 0; ii < numTopics; ii += 2 {
		glog.Infof("Removing topic: %s", allTopics[ii].ToString())
		var req comm.RemoveTopicRequest
		req.TopicId = int32(allTopics[ii].ID)
		req.ClusterId = clusterID
		err := im.RemoveTopic(context.Background(), &req)
		if err != nil {
			glog.Fatalf("Unable to remove topic: %s(%d) due to err: %s",
				allTopics[ii].Name, allTopics[ii].ID, err.Error())
		}
	}

	// Get topics.
	for ii := 0; ii < numTopics; ii++ {
		var req comm.GetTopicRequest
		req.ClusterId = clusterID
		req.TopicName = generateTopicName(ii)
		_, err := im.GetTopic(context.Background(), &req)
		if ii%2 == 1 {
			if err != nil {
				glog.Fatalf("Error while fetching topic: %s", generateTopicName(ii))
			}
		} else {
			if err == nil {
				glog.Fatalf("Expected ErrTopicNotFound but got nil instead")
			}
			he, ok := err.(*hedwigError)
			if !ok {
				glog.Fatalf("Got an unexpected error type. Expected hedwigError")
			}
			if he.errorCode != KErrTopicNotFound {
				glog.Fatalf("Expected ErrTopicNotFound but got %d, %s", he.errorCode, he.errorMsg)
			}
		}
	}

	// Add removed topics.
	for ii := 0; ii < numTopics; ii += 2 {
		glog.Infof("Adding removed topics")
		var req comm.CreateTopicRequest
		var topic comm.Topic
		topic.PartitionIds = []int32{1, 2, 3, 4}
		topic.TtlSeconds = 86400 * 7
		topic.TopicName = generateTopicName(ii)
		req.Topic = &topic
		err := im.AddTopic(context.Background(), &req)
		if err != nil {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic", err.Error())
		}
	}

	// Get all topics again and check that we them all.
	for ii := 0; ii < numTopics; ii++ {
		var req comm.GetTopicRequest
		req.ClusterId = clusterID
		req.TopicName = generateTopicName(ii)
		topic, err := im.GetTopic(context.Background(), &req)
		if err != nil {
			glog.Fatalf("Error while fetching topic: %s", generateTopicName(ii))
		}
		glog.Infof("Received topic: %s, ID: %d", topic.Name, topic.ID)
	}
	time.Sleep(5 * time.Second)
}
