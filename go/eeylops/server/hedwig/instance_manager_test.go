package hedwig

import (
	"bytes"
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

	// Add same topics again. We should get an error.
	for ii := 0; ii < numTopics; ii++ {
		var req comm.CreateTopicRequest
		var topic comm.Topic
		topic.PartitionIds = []int32{1, 2}
		topic.TtlSeconds = 86400 * 5
		topic.TopicName = generateTopicName(ii)
		req.Topic = &topic
		err := im.AddTopic(context.Background(), &req)
		if err == nil {
			glog.Fatalf("Expected an error but got: nil")
		}
		acErr, ok := err.(*hedwigError)
		if !ok {
			glog.Fatalf("Unable to cast received err: %v to hedwig error", err)
		}
		if acErr.errorCode != KErrTopicExists {
			glog.Fatalf("Expected: %s got: %s", KErrTopicExists.ToString(), acErr.errorCode.ToString())
		}
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

func TestInstanceManager_ConsumerCommit(t *testing.T) {
	util.LogTestMarker("TestInstanceManager_ConsumerCommit")
	testDirName := util.CreateTestDir(t, "TestInstanceManager_ConsumerCommit")
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
	// Register consumers for non-existent topics.
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.RegisterConsumerRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		err := im.RegisterConsumer(context.Background(), &req)
		he, ok := err.(*hedwigError)
		if !ok {
			glog.Fatalf("Unable to type cast error to hedwigError. Error: %v", err)
		}
		if he.errorCode != KErrTopicNotFound {
			glog.Fatalf("Expected %s, got: %s", KErrTopicNotFound.ToString(), he.errorCode.ToString())
		}
	}

	// Add a topic.
	var req comm.CreateTopicRequest
	var topic comm.Topic
	topic.PartitionIds = []int32{1, 2, 3, 4}
	topic.TtlSeconds = 86400 * 7
	topic.TopicName = "hello_topic_1"
	req.Topic = &topic
	err := im.AddTopic(context.Background(), &req)
	if err != nil {
		glog.Fatalf("Expected no error but got: %s. Unable to add topic", err.Error())
	}

	// Register consumers for non-existent topics.
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.RegisterConsumerRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		err := im.RegisterConsumer(context.Background(), &req)
		if err != nil {
			glog.Fatalf("Unable to register consumer commit due to err: %s", err.Error())
		}
	}

	// Get last committed offset for all consumers.
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.LastCommittedRequest
		req.ConsumerId = generateConsumerName(ii)
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
		req.ConsumerId = generateConsumerName(ii)
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
		req.ConsumerId = generateConsumerName(ii)
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
		req.ConsumerId = generateConsumerName(ii)
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

	// Commit offsets for non-registered consumers and expect errors.
	glog.Infof("Committing offsets for non-registered consumers")
	for ii := numConsumers; ii < 2*numConsumers; ii++ {
		var req comm.CommitRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		req.Offset = int64(commitOffset)
		err := im.Commit(context.Background(), &req)
		if err == nil {
			// We still expect no errors since FSM will just paper over it with a warning log!
			glog.Fatalf("Unable to commit offset due to err: %s", err.Error())
		}
		he, ok := err.(*hedwigError)
		if !ok {
			glog.Fatalf("Unable to type cast error to hedwigError. Error: %v", err)
		}
		if he.errorCode != KErrSubscriberNotRegistered {
			glog.Fatalf("Unexpected error code: %s, expected: %s",
				he.errorCode.ToString(), KErrSubscriberNotRegistered.ToString())
		}
	}
}

func TestInstanceManager_ProduceConsume(t *testing.T) {
	util.LogTestMarker("TestInstanceManager_ProduceConsume")
	testDirName := util.CreateTestDir(t, "TestInstanceManager_ProduceConsume")
	clusterID := "nikhil1nikhil1"
	topicName := "produce_consume_topic"
	opts := InstanceManagerOpts{
		DataDirectory: testDirName,
		ClusterID:     clusterID,
		PeerAddresses: nil,
	}
	numIters := 500
	batchSize := 10
	im := NewInstanceManager(&opts)
	partIDs := []int32{1, 2, 3, 4}
	var req comm.CreateTopicRequest
	var topic comm.Topic
	topic.PartitionIds = partIDs
	topic.TtlSeconds = 86400 * 7
	topic.TopicName = topicName
	req.Topic = &topic
	err := im.AddTopic(context.Background(), &req)
	if err != nil {
		glog.Fatalf("Expected no error but got: %s. Unable to add topic", err.Error())
	}
	var greq comm.GetTopicRequest
	greq.ClusterId = clusterID
	greq.TopicName = topicName
	topicCfg, err := im.GetTopic(context.Background(), &greq)
	if err != nil {
		glog.Fatalf("Error while fetching topic: %s", topicName)
	}
	glog.Infof("Received topic: %s, ID: %d", topicCfg.Name, topicCfg.ID)
	producersDone := make(chan struct{}, len(topic.PartitionIds))
	for _, pid := range partIDs {
		go func(prtID int32) {
			glog.Infof("Starting producer on partition: %d", prtID)
			for ii := 0; ii < numIters; ii++ {
				var values [][]byte
				for jj := 0; jj < batchSize; jj++ {
					values = append(values, []byte(fmt.Sprintf("value-%d", (ii*batchSize)+jj)))
				}
				var req comm.ProduceRequest
				req.ClusterId = clusterID
				req.TopicId = int32(topicCfg.ID)
				req.PartitionId = prtID
				req.Values = values
				err := im.Produce(context.Background(), &req)
				if err != nil {
					glog.Fatalf("Got error while producing. Err: %s", err.Error())
				}
			}
			producersDone <- struct{}{}
		}(pid)
	}

	for ii := 0; ii < len(partIDs); ii++ {
		<-producersDone
	}
	close(producersDone)
	glog.Infof("All producers have finished!")

	consumersDone := make(chan struct{}, len(topic.PartitionIds))
	for _, pid := range partIDs {
		go func(prtID int32) {
			glog.Infof("Starting consumer on partition: %d", prtID)
			prevNextOffset := int64(0)
			for ii := 0; ii < numIters+1; ii++ {
				var req comm.ConsumeRequest
				req.ClusterId = clusterID
				req.TopicId = int32(topicCfg.ID)
				req.PartitionId = prtID
				req.StartOffset = prevNextOffset
				req.ConsumerId = "nikhil"
				req.BatchSize = int32(batchSize)
				req.AutoCommit = false
				req.ResumeFromLastCommittedOffset = false
				ret, err := im.Consume(context.Background(), &req)
				if err != nil {
					glog.Fatalf("Got an unexpected error while scanning values: %s from partition: %d",
						err.Error(), prtID)
				}
				if ret.Error != nil {
					glog.Fatalf("Unexpected error while scanning values: %s from partition: %d",
						ret.Error.Error(), prtID)
				}
				expectedNextOffset := base.Offset((ii + 1) * batchSize)
				if ii < numIters-1 {
					if ret.NextOffset != expectedNextOffset {
						glog.Fatalf("Expected next offset: %d, Got: %d, Partition: %d",
							expectedNextOffset, ret.NextOffset, prtID)
					}
				} else {
					glog.Infof("Scan should have finished now. Next offset: %d", ret.NextOffset)
					if !(ret.NextOffset == expectedNextOffset || ret.NextOffset == -1) {
						glog.Fatalf("Expected next offset: %d or -1, Got: %d, Partition: %d",
							expectedNextOffset, ret.NextOffset, prtID)
					}
				}
				if ii < numIters {
					for jj := 0; jj < batchSize; jj++ {
						expectedVal := []byte(fmt.Sprintf("value-%d", (ii*batchSize)+jj))
						if bytes.Compare(ret.Values[jj].Value, expectedVal) != 0 {
							glog.Fatalf("Expected value: %s, Got: %s, Partition: %d",
								string(expectedVal), string(ret.Values[jj].Value), prtID)
						}
					}
				} else {
					if len(ret.Values) != 0 {
						glog.Fatalf("Expected 0 records, got: %d from partition: %d", len(ret.Values), prtID)
					}
				}
				if ret.NextOffset >= 0 {
					prevNextOffset = int64(ret.NextOffset)
				}
			}
			consumersDone <- struct{}{}
		}(pid)
	}
	for ii := 0; ii < len(partIDs); ii++ {
		<-consumersDone
	}
	close(consumersDone)
	glog.Infof("All consumers have finished!")
}
