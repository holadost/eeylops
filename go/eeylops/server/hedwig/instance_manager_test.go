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

func TestInstanceManager_Consumer(t *testing.T) {
	util.LogTestMarker("TestInstanceManager_Consumer")
	testDirName := util.CreateTestDir(t, "TestInstanceManager_Consumer")
	clusterID := "nikhil1nikhil1"
	opts := InstanceManagerOpts{
		DataDirectory: testDirName,
		ClusterID:     clusterID,
		PeerAddresses: nil,
	}
	generateConsumerName := func(id int) string {
		return fmt.Sprintf("hello_consumer_%d", id)
	}
	numConsumers := 15
	commitOffset := base.Offset(100)
	im := NewInstanceManager(&opts)
	// Add topics.
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.RegisterSubscriberRequest
		req.SubscriberId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		err := im.RegisterSubscriber(context.Background(), &req)
		if err != nil {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic", err.Error())
		}
	}

	// Get last committed offset for all consumers.
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.LastCommittedRequest
		req.SubscriberId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		offset, err := im.GetLastCommitted(context.Background(), &req)
		if err != nil {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic", err.Error())
		}
		if offset != -1 {
			glog.Fatalf("Found an offset: %d even though no offsets were committed", offset)
		}
	}

	// Get last committed offset for non-existent consumers.
	for ii := numConsumers; ii < 2*numConsumers; ii++ {
		var req comm.LastCommittedRequest
		req.SubscriberId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		_, err := im.GetLastCommitted(context.Background(), &req)
		if err == nil {
			glog.Fatalf("Expected ErrConsumerNotRegistered, got: %v", err)
		}
		meraErr, ok := err.(*hedwigError)
		if !ok {
			glog.Fatalf("Expected hedwigError, got: %v", err)
		}
		if meraErr.errorCode != KErrBackendStorage {
			glog.Fatalf("Expected backend storage error. Got: %s", meraErr.errorCode.ToString())
		}
	}

	// Commit offsets for consumer.
	glog.Infof("Committing offsets for registered consumers")
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.CommitRequest
		req.SubscriberId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		req.Offset = int64(commitOffset)
		err := im.Commit(context.Background(), &req)
		if err != nil {
			glog.Fatalf("Unable to commit offset due to err: %s", err.Error())
		}
	}

	// Get last committed offset for all consumers.
	glog.Infof("Checking committed offsets")
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.LastCommittedRequest
		req.SubscriberId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		offset, err := im.GetLastCommitted(context.Background(), &req)
		if err != nil {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic", err.Error())
		}
		if offset != commitOffset {
			glog.Fatalf("Found an offset: %d even though no offsets were committed", offset)
		}
	}

	glog.Infof("Committing offsets for non-registered consumers")
	for ii := numConsumers; ii < 2*numConsumers; ii++ {
		var req comm.CommitRequest
		req.SubscriberId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		req.Offset = int64(commitOffset)
		err := im.Commit(context.Background(), &req)
		if err == nil {
			// We still expect no errors since FSM will just paper over it with a warning log!
			glog.Fatalf("Unable to commit offset due to err: %s", err.Error())
		}
	}
}
