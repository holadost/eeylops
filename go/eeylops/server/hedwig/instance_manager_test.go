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
		resp := im.AddTopic(context.Background(), &req)
		err := resp.GetError()
		if ErrorCode(err.GetErrorCode()) != KErrNoError {
			glog.Fatalf("Expected no error but got: %v. Unable to add topic", err)
		}
	}

	// Get topics.
	var allTopics []*comm.Topic
	for ii := 0; ii < numTopics; ii++ {
		var req comm.GetTopicRequest
		req.ClusterId = clusterID
		req.TopicName = generateTopicName(ii)
		resp := im.GetTopic(context.Background(), &req)
		err := resp.GetError()
		if ErrorCode(err.GetErrorCode()) != KErrNoError {
			glog.Fatalf("Error while fetching topic: %s", generateTopicName(ii))
		}
		topic := resp.GetTopic()
		glog.Infof("Received topic: %s, ID: %d", topic.GetTopicName(), topic.GetTopicId())
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
		resp := im.AddTopic(context.Background(), &req)
		err := resp.GetError()
		if ErrorCode(err.GetErrorCode()) != KErrTopicExists {
			glog.Fatalf("Unexpected error while adding same topic: %s, %v", generateTopicName(ii), err)
		}
	}

	// Get all topics.
	resp := im.GetAllTopics(context.Background())
	allTopicsErr := resp.GetError()
	if ErrorCode(allTopicsErr.GetErrorCode()) != KErrNoError {
		glog.Fatalf("Unexpected error while getting all topics: %v", allTopicsErr)
	}
	if len(resp.GetTopics()) != numTopics {
		glog.Fatalf("Did not get the desired number of topics. Expected: %d, Got: %d", numTopics,
			len(resp.GetTopics()))
	}

	// Remove topics.
	for ii := 0; ii < numTopics; ii += 2 {
		glog.Infof("Removing topic: %s:%d", allTopics[ii].GetTopicName(), allTopics[ii].GetTopicId())
		var req comm.RemoveTopicRequest
		req.TopicId = int32(allTopics[ii].GetTopicId())
		req.ClusterId = clusterID
		resp := im.RemoveTopic(context.Background(), &req)
		err := resp.GetError()
		if ErrorCode(err.GetErrorCode()) != KErrNoError {
			glog.Fatalf("Unable to remove topic: %s(%d) due to err: %v",
				allTopics[ii].GetTopicName(), allTopics[ii].GetTopicId(), err)
		}
	}

	// Remove topics that don't exist.
	glog.Infof("Removing non existent topics")
	for ii := numTopics * 2; ii < numTopics*3; ii++ {
		var req comm.RemoveTopicRequest
		req.TopicId = int32(numTopics*10 + ii)
		req.ClusterId = clusterID
		resp := im.RemoveTopic(context.Background(), &req)
		err := resp.GetError()
		ec := ErrorCode(err.GetErrorCode())
		if ec != KErrTopicNotFound {
			glog.Fatalf("Failed while removing non-existent topic due to unexpected error: %s", ec.ToString())
		}
	}

	// Get topics.
	for ii := 0; ii < numTopics; ii++ {
		var req comm.GetTopicRequest
		req.ClusterId = clusterID
		req.TopicName = generateTopicName(ii)
		resp := im.GetTopic(context.Background(), &req)
		err := resp.GetError()
		if ii%2 == 1 {
			// This topic must exist.
			if ErrorCode(err.GetErrorCode()) != KErrNoError {
				glog.Fatalf("Error while fetching topic: %s", generateTopicName(ii))
			}
		} else {
			// This topic must not exist.
			if ErrorCode(err.GetErrorCode()) != KErrTopicNotFound {
				glog.Fatalf("Expected ErrTopicNotFound but got %s instead",
					ErrorCode(err.GetErrorCode()).ToString())
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
		resp := im.AddTopic(context.Background(), &req)
		err := resp.GetError()
		if ErrorCode(err.GetErrorCode()) != KErrNoError {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic",
				ErrorCode(err.GetErrorCode()).ToString())
		}
	}

	// Get all topics again and check that we them all.
	for ii := 0; ii < numTopics; ii++ {
		var req comm.GetTopicRequest
		req.ClusterId = clusterID
		req.TopicName = generateTopicName(ii)
		resp := im.GetTopic(context.Background(), &req)
		err := resp.GetError()
		ec := ErrorCode(err.GetErrorCode())
		if ec != KErrNoError {
			glog.Fatalf("Error while fetching topic: %s. Error: %s", generateTopicName(ii), ec.ToString())
		}
	}
	time.Sleep(2 * time.Second)
	// TODO: Check that the removed topics have been deleted from the underlying file system.
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
		resp := im.RegisterConsumer(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if ec != KErrTopicNotFound {
			glog.Fatalf("Expected %s, got: %s", KErrTopicNotFound.ToString(), ec.ToString())
		}
	}

	// Add a topic.
	var req comm.CreateTopicRequest
	var topic comm.Topic
	topic.PartitionIds = []int32{1, 2, 3, 4}
	topic.TtlSeconds = 86400 * 7
	topic.TopicName = "hello_topic_1"
	req.Topic = &topic
	resp := im.AddTopic(context.Background(), &req)
	ec := ErrorCode(resp.GetError().GetErrorCode())
	if ec != KErrNoError {
		glog.Fatalf("Expected no error but got: %s. Unable to add topic", ec.ToString())
	}

	// Register consumers for topics.
	glog.Infof("Registering consumers")
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.RegisterConsumerRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		resp := im.RegisterConsumer(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if ec != KErrNoError {
			glog.Fatalf("Unable to register consumer commit due to err: %s", ec.ToString())
		}
	}

	// Get last committed offset for all consumers.
	glog.Infof("Fetching last committed offsets for all registered consumers")
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.LastCommittedRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		resp := im.GetLastCommitted(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if ec != KErrNoError {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic", ec.ToString())
		}
		if resp.GetOffset() != -1 {
			glog.Fatalf("Found an offset: %d even though no offsets were committed", resp.GetOffset())
		}
	}

	// Get last committed offset for non-existent topics.
	glog.Infof("Fetching last committed offsets for non-existent topics")
	for ii := numConsumers; ii < 2*numConsumers; ii++ {
		var req comm.LastCommittedRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 2 // Topic does not exist.
		req.PartitionId = 2
		resp := im.GetLastCommitted(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if ec != KErrTopicNotFound {
			glog.Fatalf("Expected %s, got: %v", KErrTopicNotFound.ToString(), ec.ToString())
		}
	}

	// Get last committed offset for non-existent partitions.
	glog.Infof("Fetching last committed offsets for non-existent partitions")
	for ii := numConsumers; ii < 2*numConsumers; ii++ {
		var req comm.LastCommittedRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 5 // Partition does not exist.
		resp := im.GetLastCommitted(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if !(ec == KErrTopicNotFound || ec == KErrPartitionNotFound) {
			glog.Fatalf("Expected %s or %s, got: %v",
				KErrTopicNotFound.ToString(), KErrPartitionNotFound.ToString(), ec.ToString())
		}
	}

	// Get last committed offset for non-existent consumers.
	glog.Infof("Fetching last committed offsets for non-existent consumers")
	for ii := numConsumers; ii < 2*numConsumers; ii++ {
		var req comm.LastCommittedRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		resp := im.GetLastCommitted(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if ec != KErrSubscriberNotRegistered {
			glog.Fatalf("Expected %s, got: %v", KErrSubscriberNotRegistered.ToString(), ec.ToString())
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
		resp := im.Commit(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if ec != KErrNoError {
			glog.Fatalf("Unable to commit offset due to err: %s", ec.ToString())
		}
	}

	// Commit offsets for registered consumers but non-existent topics.
	glog.Infof("Committing offsets for registered consumers but non existent topics")
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.CommitRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 2
		req.PartitionId = 2
		req.Offset = int64(commitOffset)
		resp := im.Commit(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if ec != KErrTopicNotFound {
			glog.Fatalf("Unable to commit offset due to err: %s", ec.ToString())
		}
	}

	// Commit offsets for registered consumers but non-existent partitions.
	glog.Infof("Committing offsets for registered consumers but non existent partitions")
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.CommitRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 5
		req.Offset = int64(commitOffset)
		resp := im.Commit(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if !(ec == KErrTopicNotFound || ec == KErrPartitionNotFound) {
			glog.Fatalf("Unable to commit offset due to err: %s", ec.ToString())
		}
	}

	// Get last committed offset for all consumers.
	glog.Infof("Checking committed offsets")
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.LastCommittedRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		resp := im.GetLastCommitted(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if ec != KErrNoError {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic", ec.ToString())
		}
		if base.Offset(resp.GetOffset()) != commitOffset {
			glog.Fatalf("Found an offset: %d even though no offsets were committed", resp.GetOffset())
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
		resp := im.Commit(context.Background(), &req)
		ec := ErrorCode(resp.GetError().GetErrorCode())
		if ec != KErrSubscriberNotRegistered {
			glog.Fatalf("Unexpected error code: %s, expected: %s",
				ec.ToString(), KErrSubscriberNotRegistered.ToString())
		}
	}
}

//func TestInstanceManager_ProduceConsume(t *testing.T) {
//	util.LogTestMarker("TestInstanceManager_ProduceConsume")
//	testDirName := util.CreateTestDir(t, "TestInstanceManager_ProduceConsume")
//	clusterID := "nikhil1nikhil1"
//	topicName := "produce_consume_topic"
//	opts := InstanceManagerOpts{
//		DataDirectory: testDirName,
//		ClusterID:     clusterID,
//		PeerAddresses: nil,
//	}
//	numIters := 500
//	batchSize := 10
//	im := NewInstanceManager(&opts)
//	partIDs := []int32{1, 2, 3, 4}
//	var req comm.CreateTopicRequest
//	var topic comm.Topic
//	topic.PartitionIds = partIDs
//	topic.TtlSeconds = 86400 * 7
//	topic.TopicName = topicName
//	req.Topic = &topic
//	err := im.AddTopic(context.Background(), &req)
//	if err != nil {
//		glog.Fatalf("Expected no error but got: %s. Unable to add topic", err.Error())
//	}
//	var greq comm.GetTopicRequest
//	greq.ClusterId = clusterID
//	greq.TopicName = topicName
//	topicCfg, err := im.GetTopic(context.Background(), &greq)
//	if err != nil {
//		glog.Fatalf("Error while fetching topic: %s", topicName)
//	}
//	glog.Infof("Received topic: %s, ID: %d", topicCfg.Name, topicCfg.ID)
//	producersDone := make(chan struct{}, len(topic.PartitionIds))
//	for _, pid := range partIDs {
//		go func(prtID int32) {
//			glog.Infof("Starting producer on partition: %d", prtID)
//			for ii := 0; ii < numIters; ii++ {
//				var values [][]byte
//				for jj := 0; jj < batchSize; jj++ {
//					values = append(values, []byte(fmt.Sprintf("value-%d", (ii*batchSize)+jj)))
//				}
//				var req comm.ProduceRequest
//				req.ClusterId = clusterID
//				req.TopicId = int32(topicCfg.ID)
//				req.PartitionId = prtID
//				req.Values = values
//				err := im.Produce(context.Background(), &req)
//				if err != nil {
//					glog.Fatalf("Got error while producing. Err: %s", err.Error())
//				}
//			}
//			producersDone <- struct{}{}
//		}(pid)
//	}
//
//	for ii := 0; ii < len(partIDs); ii++ {
//		<-producersDone
//	}
//	close(producersDone)
//	glog.Infof("All producers have finished!")
//
//	consumersDone := make(chan struct{}, len(topic.PartitionIds))
//	for _, pid := range partIDs {
//		go func(prtID int32) {
//			glog.Infof("Starting consumer on partition: %d", prtID)
//			prevNextOffset := int64(0)
//			for ii := 0; ii < numIters+1; ii++ {
//				var req comm.ConsumeRequest
//				req.ClusterId = clusterID
//				req.TopicId = int32(topicCfg.ID)
//				req.PartitionId = prtID
//				req.StartOffset = prevNextOffset
//				req.ConsumerId = "nikhil"
//				req.BatchSize = int32(batchSize)
//				req.AutoCommit = false
//				req.ResumeFromLastCommittedOffset = false
//				ret, err := im.Consume(context.Background(), &req)
//				if err != nil {
//					glog.Fatalf("Got an unexpected error while scanning values: %s from partition: %d",
//						err.Error(), prtID)
//				}
//				if ret.Error != nil {
//					glog.Fatalf("Unexpected error while scanning values: %s from partition: %d",
//						ret.Error.Error(), prtID)
//				}
//				expectedNextOffset := base.Offset((ii + 1) * batchSize)
//				if ii < numIters-1 {
//					if ret.NextOffset != expectedNextOffset {
//						glog.Fatalf("Expected next offset: %d, Got: %d, Partition: %d",
//							expectedNextOffset, ret.NextOffset, prtID)
//					}
//				} else {
//					glog.Infof("Scan should have finished now. Next offset: %d", ret.NextOffset)
//					if !(ret.NextOffset == expectedNextOffset || ret.NextOffset == -1) {
//						glog.Fatalf("Expected next offset: %d or -1, Got: %d, Partition: %d",
//							expectedNextOffset, ret.NextOffset, prtID)
//					}
//				}
//				if ii < numIters {
//					for jj := 0; jj < batchSize; jj++ {
//						expectedVal := []byte(fmt.Sprintf("value-%d", (ii*batchSize)+jj))
//						if bytes.Compare(ret.Values[jj].Value, expectedVal) != 0 {
//							glog.Fatalf("Expected value: %s, Got: %s, Partition: %d",
//								string(expectedVal), string(ret.Values[jj].Value), prtID)
//						}
//					}
//				} else {
//					if len(ret.Values) != 0 {
//						glog.Fatalf("Expected 0 records, got: %d from partition: %d", len(ret.Values), prtID)
//					}
//				}
//				if ret.NextOffset >= 0 {
//					prevNextOffset = int64(ret.NextOffset)
//				}
//			}
//			consumersDone <- struct{}{}
//		}(pid)
//	}
//	for ii := 0; ii < len(partIDs); ii++ {
//		<-consumersDone
//	}
//	close(consumersDone)
//	glog.Infof("All consumers have finished!")
//}
