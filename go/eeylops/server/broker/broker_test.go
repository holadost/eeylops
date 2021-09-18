package broker

import (
	"bytes"
	"context"
	"eeylops/comm"
	"eeylops/server/base"
	"eeylops/util/testutil"
	"fmt"
	"github.com/golang/glog"
	"runtime"
	"testing"
)

func TestBroker_ConsumerCommit(t *testing.T) {
	testutil.LogTestMarker("TestBroker_ConsumerCommit")
	testDirName := testutil.CreateFreshTestDir("TestBroker_ConsumerCommit")
	opts := BrokerOpts{
		DataDirectory: testDirName,
		PeerAddresses: nil,
		BrokerID:      "broker-1",
	}
	generateConsumerName := func(id int) string {
		return fmt.Sprintf("hello_consumer_%d", id)
	}
	numConsumers := 15
	commitOffset := base.Offset(100)
	im := NewBroker(&opts)
	// Register consumers for non-existent topics.
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.RegisterConsumerRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		resp := im.RegisterConsumer(context.Background(), &req)
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KErrTopicNotFound {
			glog.Fatalf("Expected %s, got: %s", comm.Error_KErrTopicNotFound.String(), ec.String())
		}
	}

	// Add a topic.
	var req comm.CreateTopicRequest
	var topic comm.Topic
	topic.PartitionIds = []int32{1, 2, 3, 4}
	topic.TtlSeconds = 86400 * 7
	topic.TopicName = "hello_topic_1"
	topic.TopicId = 1
	req.Topic = &topic
	resp := im.AddTopic(context.Background(), &req)
	ec := resp.GetError().GetErrorCode()
	if ec != comm.Error_KNoError {
		glog.Fatalf("Expected no error but got: %s. Unable to add topic", ec.String())
	}

	// Register consumers for topics.
	glog.Infof("Registering consumers")
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.RegisterConsumerRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		resp := im.RegisterConsumer(context.Background(), &req)
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KNoError {
			glog.Fatalf("Unable to register consumer commit due to err: %s", ec.String())
		}
		glog.Infof("Successfully registered consumer: %s", generateConsumerName(ii))
	}

	// Get last committed offset for all consumers.
	glog.Infof("Fetching last committed offsets for all registered consumers")
	for ii := 0; ii < numConsumers; ii++ {
		var req comm.LastCommittedRequest
		req.ConsumerId = generateConsumerName(ii)
		req.TopicId = 1
		req.PartitionId = 2
		resp := im.GetLastCommitted(context.Background(), &req)
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KNoError {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic", ec.String())
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
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KErrTopicNotFound {
			glog.Fatalf("Expected %s, got: %v", comm.Error_KErrTopicNotFound.String(), ec.String())
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
		ec := resp.GetError().GetErrorCode()
		if !(ec == comm.Error_KErrTopicNotFound || ec == comm.Error_KErrPartitionNotFound) {
			glog.Fatalf("Expected %s or %s, got: %v",
				comm.Error_KErrTopicNotFound.String(), comm.Error_KErrPartitionNotFound.String(), ec.String())
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
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KErrConsumerNotRegistered {
			glog.Fatalf("Expected %s, got: %v", comm.Error_KErrConsumerNotRegistered.String(), ec.String())
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
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KNoError {
			glog.Fatalf("Unable to commit offset due to err: %s", ec.String())
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
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KErrTopicNotFound {
			glog.Fatalf("Unable to commit offset due to err: %s", ec.String())
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
		ec := resp.GetError().GetErrorCode()
		if !(ec == comm.Error_KErrTopicNotFound || ec == comm.Error_KErrPartitionNotFound) {
			glog.Fatalf("Unable to commit offset due to err: %s", ec.String())
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
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KNoError {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic", ec.String())
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
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KErrConsumerNotRegistered {
			glog.Fatalf("Unexpected error code: %s, expected: %s",
				ec.String(), comm.Error_KErrConsumerNotRegistered.String())
		}
	}
}

func TestBroker_ProduceConsume(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	testutil.LogTestMarker("TestBroker_ProduceConsume")
	testDirName := testutil.CreateTestDir(t, "TestBroker_ProduceConsume")
	topicName := "produce_consume_topic"
	opts := BrokerOpts{
		DataDirectory: testDirName,
		PeerAddresses: nil,
		BrokerID:      "broker-1",
	}
	numIters := 5000
	batchSize := 10
	broker := NewBroker(&opts)
	partIDs := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var req comm.CreateTopicRequest
	var topic comm.Topic
	topic.PartitionIds = partIDs
	topic.TtlSeconds = 86400 * 7
	topic.TopicName = topicName
	topic.TopicId = 1
	req.Topic = &topic
	resp := broker.AddTopic(context.Background(), &req)
	ec := resp.GetError().GetErrorCode()
	if ec != comm.Error_KNoError {
		glog.Fatalf("Expected no error but got: %s. Unable to add topic", ec.String())
	}

	// Produce and consume from non-existent topics and partitions.
	produceNonExistentTopic := func() {
		var values [][]byte
		for jj := 0; jj < batchSize; jj++ {
			values = append(values, []byte(fmt.Sprintf("value-%d", (jj*batchSize)+jj)))
		}
		var req comm.ProduceRequest
		req.TopicId = 100
		req.PartitionId = 2
		req.Values = values
		resp := broker.Produce(context.Background(), &req)
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KErrTopicNotFound {
			glog.Fatalf("Got error while producing. Expected: %s, Got: %s. Msg: %s",
				comm.Error_KErrTopicNotFound.String(), ec.String(), resp.GetError().GetErrorMsg())
		}
	}
	produceNonExistentTopic()
	consumeNonExistentTopic := func() {
		var req comm.ConsumeRequest
		req.TopicId = 1001
		req.PartitionId = 1
		req.StartOffset = 0
		req.ConsumerId = "nikhil"
		req.BatchSize = int32(batchSize)
		req.AutoCommit = false
		req.ResumeFromLastCommittedOffset = false
		resp := broker.Consume(context.Background(), &req)
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KErrTopicNotFound {
			glog.Fatalf("Got an unexpected error while scanning values. Expected: %s, Got: %s",
				comm.Error_KErrTopicNotFound.String(), ec.String())
		}
	}
	consumeNonExistentTopic()

	// Produce and consume from non-existent partitions.
	produceNonExistentPartition := func() {
		var values [][]byte
		for jj := 0; jj < batchSize; jj++ {
			values = append(values, []byte(fmt.Sprintf("value-%d", (jj*batchSize)+jj)))
		}
		var req comm.ProduceRequest
		req.TopicId = 1
		req.PartitionId = 100
		req.Values = values
		resp := broker.Produce(context.Background(), &req)
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KErrPartitionNotFound {
			glog.Fatalf("Got unexpected error while producing. Expected: %s, Got: %s. Msg: %s",
				comm.Error_KErrPartitionNotFound.String(), ec.String(), resp.GetError().GetErrorMsg())
		}
	}
	produceNonExistentPartition()
	consumeNonExistentPartition := func() {
		var req comm.ConsumeRequest
		req.TopicId = 1
		req.PartitionId = 100
		req.StartOffset = 0
		req.ConsumerId = "nikhil"
		req.BatchSize = int32(batchSize)
		req.AutoCommit = false
		req.ResumeFromLastCommittedOffset = false
		resp := broker.Consume(context.Background(), &req)
		ec := resp.GetError().GetErrorCode()
		if ec != comm.Error_KErrPartitionNotFound {
			glog.Fatalf("Got an unexpected error while scanning values. Expected: %s, Got: %s",
				comm.Error_KErrPartitionNotFound.String(), ec.String())
		}
	}
	consumeNonExistentPartition()

	// Produce and consume values.
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
				req.TopicId = 1
				req.PartitionId = prtID
				req.Values = values
				resp := broker.Produce(context.Background(), &req)
				ec := resp.GetError().GetErrorCode()
				if ec != comm.Error_KNoError {
					glog.Fatalf("Got error while producing. Err: %s. Msg: %s", ec.String(),
						resp.GetError().GetErrorMsg())
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
				req.TopicId = 1
				req.PartitionId = prtID
				req.StartOffset = prevNextOffset
				req.ConsumerId = "nikhil"
				req.BatchSize = int32(batchSize)
				req.AutoCommit = false
				req.ResumeFromLastCommittedOffset = false
				resp := broker.Consume(context.Background(), &req)
				ec := resp.GetError().GetErrorCode()
				if ec != comm.Error_KNoError {
					glog.Fatalf("Got an unexpected error while scanning values: %s from partition: %d",
						ec.String(), prtID)
				}
				expectedNextOffset := base.Offset((ii + 1) * batchSize)
				gotNextOffset := base.Offset(resp.GetNextOffset())
				if ii < numIters-1 {
					if gotNextOffset != expectedNextOffset {
						glog.Fatalf("Expected next offset: %d, Got: %d, Partition: %d",
							expectedNextOffset, gotNextOffset, prtID)
					}
				} else {
					glog.Infof("Scan should have finished now. Next offset: %d", gotNextOffset)
					if !(gotNextOffset == expectedNextOffset || gotNextOffset == -1) {
						glog.Fatalf("Expected next offset: %d or -1, Got: %d, Partition: %d",
							expectedNextOffset, gotNextOffset, prtID)
					}
				}
				if ii < numIters {
					for jj := 0; jj < batchSize; jj++ {
						expectedVal := []byte(fmt.Sprintf("value-%d", (ii*batchSize)+jj))
						if bytes.Compare(resp.GetValues()[jj].Value, expectedVal) != 0 {
							glog.Fatalf("Expected value: %s, Got: %s, Partition: %d",
								string(expectedVal), string(resp.GetValues()[jj].Value), prtID)
						}
					}
				} else {
					if len(resp.GetValues()) != 0 {
						glog.Fatalf("Expected 0 records, got: %d from partition: %d",
							len(resp.GetValues()), prtID)
					}
				}
				if gotNextOffset >= 0 {
					prevNextOffset = int64(gotNextOffset)
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
