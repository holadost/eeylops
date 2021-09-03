package client

import (
	"eeylops/server"
	"eeylops/server/base"
	"eeylops/util/testutil"
	"fmt"
	"github.com/golang/glog"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestClient_AddRemoveTopic(t *testing.T) {
	testutil.LogTestMarker("TestClient_AddRemoveTopic")
	testDirPath := testutil.CreateFreshTestDir("TestClient_AddRemoveTopic")
	addr := NodeAddress{
		Host: "0.0.0.0",
		Port: 25001,
	}
	glog.Infof("Initializing RPC server")
	rpcServer := server.TestOnlyNewRPCServer(addr.Host, addr.Port, testDirPath)
	go rpcServer.Run()
	defer rpcServer.Stop()
	time.Sleep(time.Second)
	numTopics := 15
	generateTopicName := func(idx int) string {
		return fmt.Sprintf("hello_world_topic_%d", idx)
	}
	glog.Infof("Initializing client")
	client := NewClient(addr)
	// Add topics.
	for ii := 0; ii < numTopics; ii++ {
		glog.Infof("Creating topic: %s", generateTopicName(ii+1))
		err := client.CreateTopic(generateTopicName(ii+1), []int{1, 2, 3, 4}, 86400)
		if err != nil {
			glog.Fatalf("Expected no error but got: %v. Unable to add topic", err)
		}
	}

	// Get topics.
	var allTopics []*base.TopicConfig
	for ii := 0; ii < numTopics; ii++ {
		topic, err := client.GetTopic(generateTopicName(ii + 1))
		if err != nil {
			glog.Fatalf("Error while fetching topic: %s, error: %v", generateTopicName(ii+1), err)
		}
		glog.Infof("Received topic: %s", topic.ToString())
		allTopics = append(allTopics, &topic)
	}

	// Add same topics again. We should get an error.
	for ii := 0; ii < numTopics; ii++ {
		err := client.CreateTopic(generateTopicName(ii+1), []int{1, 2, 3, 4}, 86400)
		if err == nil {
			glog.Fatalf("Expected error while adding same topic: %s, but got %v", generateTopicName(ii), err)
		}
		glog.Infof("While adding topic: %s again, received err: %v", generateTopicName(ii+1), err)
	}

	// Remove every alternate topic.
	for ii := 0; ii < numTopics; ii += 2 {
		glog.Infof("Removing topic: %s:%d", allTopics[ii].Name, allTopics[ii].ID)
		err := client.RemoveTopic(allTopics[ii].Name)
		if err != nil {
			glog.Fatalf("Unable to remove topic: %s(%d) due to err: %v", allTopics[ii].Name, allTopics[ii].ID,
				err)
		}
	}

	// Remove topics that don't exist.
	glog.Infof("Removing non existent topics")
	for ii := numTopics * 2; ii < numTopics*3; ii++ {
		glog.Infof("Removing topic: %s:%d", generateTopicName(ii+1), ii+1)
		err := client.RemoveTopic(generateTopicName(ii + 1))
		if err == nil {
			glog.Fatalf("Expected err but got nil")
		}
		glog.Infof("Received error while attempting to remove non-existent topic: %v", err.Error())
	}

	// Add removed topics.
	for ii := 0; ii < numTopics; ii += 2 {
		glog.Infof("Creating topic: %s", generateTopicName(ii+1))
		err := client.CreateTopic(generateTopicName(ii+1), []int{1, 2, 3, 4}, 86400)
		if err != nil {
			glog.Fatalf("Expected no error but got: %v. Unable to add topic", err)
		}
	}

	// Get all topics again and check that we them all.
	for ii := 0; ii < numTopics; ii++ {
		topic, err := client.GetTopic(generateTopicName(ii + 1))
		if err != nil {
			glog.Fatalf("Error while fetching topic: %s, error: %v", generateTopicName(ii+1), err)
		}
		glog.Infof("Received topic: %s", topic.ToString())
		allTopics = append(allTopics, &topic)
	}
}

func TestClient_ProducerConsumer(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	testutil.LogTestMarker("TestClient_AddRemoveTopic")
	testDirPath := testutil.CreateFreshTestDir("TestClient_AddRemoveTopic")
	addr := NodeAddress{
		Host: "0.0.0.0",
		Port: 25002,
	}
	glog.Infof("Initializing RPC server")
	rpcServer := server.TestOnlyNewRPCServer(addr.Host, addr.Port, testDirPath)
	go rpcServer.Run()
	defer rpcServer.Stop()
	time.Sleep(time.Second)
	generateTopicName := func(idx int) string {
		return fmt.Sprintf("hello_world_topic_%d", idx)
	}

	glog.Infof("Initializing client")
	client := NewClient(addr)
	defer client.Close()

	glog.Infof("Creating topic: %s", generateTopicName(1))
	testTopicName := generateTopicName(1)
	testPartitions := []int{1, 2, 3, 4}
	err := client.CreateTopic(testTopicName, testPartitions, 86400)
	if err != nil {
		glog.Fatalf("Expected no error but got: %v. Unable to add topic", err)
	}

	// Produce and consume from non-existent topics and partitions.
	produceNonExistentTopic := func() {
		glog.Infof("Checking if producer for non existent topics return error")
		_, err := client.NewProducer(generateTopicName(2), 1)
		if err == nil {
			glog.Fatalf("Expected an error but got nil")
		}
		glog.Infof("Received error while attempting to register producer to a non-existent topic: %v", err)
	}
	consumeNonExistentTopic := func() {
		glog.Infof("Checking if consumer for non existent topics return error")
		cfg := ConsumerConfig{
			ConsumerID:              "foobar",
			TopicName:               generateTopicName(2),
			PartitionID:             1,
			AutoCommit:              true,
			ResumeFromLastCommitted: true,
			StartEpochNs:            0,
			EndEpochNs:              0,
		}
		_, err := client.NewConsumer(cfg)
		if err == nil {
			glog.Fatalf("Expected an error but got nil")
		}
		glog.Infof("Received error while attempting to register producer to a non-existent topic: %v", err)
	}
	produceNonExistentTopic()
	consumeNonExistentTopic()

	// Produce and consume from non-existent partitions.
	produceNonExistentPartition := func() {
		_, err := client.NewProducer(generateTopicName(1), 100)
		if err == nil {
			glog.Fatalf("Expected an error but got nil")
		}
		glog.Infof("Received error while attempting to register producer to a non-existent topic: %v", err)
	}
	consumeNonExistentPartition := func() {
		glog.Infof("Checking if consumer for non existent partition return error")
		cfg := ConsumerConfig{
			ConsumerID:              "foobar",
			TopicName:               generateTopicName(1),
			PartitionID:             100,
			AutoCommit:              true,
			ResumeFromLastCommitted: true,
			StartEpochNs:            0,
			EndEpochNs:              0,
		}
		_, err := client.NewConsumer(cfg)
		if err == nil {
			glog.Fatalf("Expected an error but got nil")
		}
		glog.Infof("Received error while attempting to register producer to a non-existent topic: %v", err)
	}
	produceNonExistentPartition()
	consumeNonExistentPartition()

	// Produce and consume values.
	producersDone := make(chan struct{}, len(testPartitions))
	numIters := 500
	batchSize := 10
	generateStringValue := func(iternum int, batchnum int) string {
		return fmt.Sprintf("value-%d", (iternum*batchSize)+batchnum)
	}
	generateByteValue := func(iternum int, batchnum int) []byte {
		return []byte(generateStringValue(iternum, batchnum))
	}
	time.Sleep(time.Second)
	now := time.Now()
	for _, pid := range testPartitions {
		go func(prtID int) {
			glog.Infof("Starting producer on partition: %d", prtID)
			producer, err := client.NewProducer(testTopicName, prtID)
			if err != nil {
				glog.Fatalf("Unable to create producer due to err: %v", err)
			}
			totalSize := 0
			for ii := 0; ii < numIters; ii++ {
				var values [][]byte
				for jj := 0; jj < batchSize; jj++ {
					val := generateByteValue(ii, jj)
					totalSize += len(val)
					values = append(values, val)
				}
				if err := producer.Produce(values); err != nil {
					glog.Fatalf("Unable to produce data to topic: %s, partition: %d due to err: %v. "+
						"Iter: %d", testTopicName, prtID, err, ii)
				}
			}
			producersDone <- struct{}{}
			glog.Infof("Total bytes written: %d bytes or %d kilo bytes", totalSize, totalSize/1024)
		}(pid)
	}
	for ii := 0; ii < len(testPartitions); ii++ {
		<-producersDone
	}
	close(producersDone)
	glog.Infof("All producers have finished! Total Time: %v", time.Since(now))

	consumersDone := make(chan struct{}, len(testPartitions))
	for _, pid := range testPartitions {
		go func(prtID int) {
			glog.Infof("Starting consumer on partition: %d", prtID)
			cfg := ConsumerConfig{
				ConsumerID:              "foobar" + "-" + strconv.Itoa(prtID),
				TopicName:               generateTopicName(1),
				PartitionID:             prtID,
				AutoCommit:              true,
				ResumeFromLastCommitted: true,
				StartEpochNs:            0,
				EndEpochNs:              0,
			}
			consumer, err := client.NewConsumer(cfg)
			if err != nil {
				glog.Fatalf("Unable to register consumer for partition: %d", prtID)
			}
			iter := -1
			totalMsgsGot := 0
			totalMsgsExpected := numIters * batchSize
			for {
				iter++
				glog.Infof("Consumer partition: %d, iteration: %d", prtID, iter+1)
				messages, err := consumer.Consume(batchSize, -1)
				if err != nil {
					if err == ErrConsumerDone {
						glog.Infof("Consumer has finished!")
						break
					}
					glog.Fatalf("Unable to fetch values from partition: %d, iteration: %d due to err: %v",
						prtID, iter, err)
				}
				for ii, msg := range messages {
					expectedOffset := base.Offset((iter * batchSize) + ii)
					if msg.Offset != expectedOffset {
						glog.Fatalf("Expected offset: %d, Got: %d, Partition: %d", expectedOffset, msg.Offset,
							prtID)
					}
					expectedVal := string(generateByteValue(iter, ii))
					gotVal := string(msg.Value)
					if gotVal != expectedVal {
						glog.Fatalf("Expected: %s, Got: %s", expectedVal, gotVal)
					}
					totalMsgsGot += 1
				}
			}
			if totalMsgsGot != totalMsgsExpected {
				glog.Fatalf("Expected total messages: %d, Got: %d", totalMsgsExpected, totalMsgsGot)
			}
			consumersDone <- struct{}{}
		}(pid)
	}
	for ii := 0; ii < len(testPartitions); ii++ {
		<-consumersDone
	}
	close(consumersDone)
	glog.Infof("All consumers have finished!")
}
