package client

import (
	"eeylops/server"
	"eeylops/server/base"
	"eeylops/util/testutil"
	"fmt"
	"github.com/golang/glog"
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
	// runtime.GOMAXPROCS(2)
	testutil.LogTestMarker("TestClient_AddRemoveTopic")
	testDirPath := testutil.CreateFreshTestDir("TestClient_AddRemoveTopic")
	addr := NodeAddress{
		Host: "0.0.0.0",
		Port: 25002,
	}
	glog.Infof("Initializing RPC server")
	rpcServer := server.TestOnlyNewRPCServer(addr.Host, addr.Port, testDirPath)
	go rpcServer.Run()
	time.Sleep(time.Second)
	generateTopicName := func(idx int) string {
		return fmt.Sprintf("hello_world_topic_%d", idx)
	}

	glog.Infof("Initializing client")
	client := NewClient(addr)
	defer client.Close()

	glog.Infof("Creating topic: %s", generateTopicName(1))
	testTopicName := generateTopicName(1)
	testPartitions := []int{1, 2, 3, 4, 5, 6, 7, 8}
	err := client.CreateTopic(testTopicName, testPartitions, 86400)
	if err != nil {
		glog.Fatalf("Expected no error but got: %v. Unable to add topic", err)
	}

	// Produce and consume from non-existent topics and partitions.
	produceNonExistentTopic := func() {
		_, err := client.NewProducer(generateTopicName(2), 1)
		if err == nil {
			glog.Fatalf("Expected an error but got nil")
		}
		glog.Infof("Received error while attempting to register producer to a non-existent topic: %v", err)
	}
	produceNonExistentTopic()
	//consumeNonExistentTopic := func() {
	//	var req comm.ConsumeRequest
	//	req.TopicId = 1001
	//	req.PartitionId = 1
	//	req.StartOffset = 0
	//	req.ConsumerId = "nikhil"
	//	req.BatchSize = int32(batchSize)
	//	req.AutoCommit = false
	//	req.ResumeFromLastCommittedOffset = false
	//	resp := broker.Consume(context.Background(), &req)
	//	ec := resp.GetError().GetErrorCode()
	//	if ec != comm.Error_KErrTopicNotFound {
	//		glog.Fatalf("Got an unexpected error while scanning values. Expected: %s, Got: %s",
	//			comm.Error_KErrTopicNotFound.String(), ec.String())
	//	}
	//}
	//consumeNonExistentTopic()
	//
	// Produce and consume from non-existent partitions.
	produceNonExistentPartition := func() {
		_, err := client.NewProducer(generateTopicName(1), 100)
		if err == nil {
			glog.Fatalf("Expected an error but got nil")
		}
		glog.Infof("Received error while attempting to register producer to a non-existent topic: %v", err)
	}
	produceNonExistentPartition()

	//consumeNonExistentPartition := func() {
	//	var req comm.ConsumeRequest
	//	req.TopicId = 1
	//	req.PartitionId = 100
	//	req.StartOffset = 0
	//	req.ConsumerId = "nikhil"
	//	req.BatchSize = int32(batchSize)
	//	req.AutoCommit = false
	//	req.ResumeFromLastCommittedOffset = false
	//	resp := broker.Consume(context.Background(), &req)
	//	ec := resp.GetError().GetErrorCode()
	//	if ec != comm.Error_KErrPartitionNotFound {
	//		glog.Fatalf("Got an unexpected error while scanning values. Expected: %s, Got: %s",
	//			comm.Error_KErrPartitionNotFound.String(), ec.String())
	//	}
	//}
	//consumeNonExistentPartition()
	//
	// Produce and consume values.
	producersDone := make(chan struct{}, len(testPartitions))
	numIters := 5000
	batchSize := 10
	generateStringValue := func(iternum int, batchnum int) string {
		return fmt.Sprintf("value-%d", (iternum*batchSize)+batchnum)
	}
	generateByteValue := func(iternum int, batchnum int) []byte {
		return []byte(generateStringValue(iternum, batchnum))
	}
	//generateStringValue := func(iternum int, batchnum int) string {
	//	return string(generateByteValue(iternum, batchnum))
	//}
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

	//consumersDone := make(chan struct{}, len(topic.PartitionIds))
	//for _, pid := range partIDs {
	//	go func(prtID int32) {
	//		glog.Infof("Starting consumer on partition: %d", prtID)
	//		prevNextOffset := int64(0)
	//		for ii := 0; ii < numIters+1; ii++ {
	//			var req comm.ConsumeRequest
	//			req.TopicId = 1
	//			req.PartitionId = prtID
	//			req.StartOffset = prevNextOffset
	//			req.ConsumerId = "nikhil"
	//			req.BatchSize = int32(batchSize)
	//			req.AutoCommit = false
	//			req.ResumeFromLastCommittedOffset = false
	//			resp := broker.Consume(context.Background(), &req)
	//			ec := resp.GetError().GetErrorCode()
	//			if ec != comm.Error_KNoError {
	//				glog.Fatalf("Got an unexpected error while scanning values: %s from partition: %d",
	//					ec.String(), prtID)
	//			}
	//			expectedNextOffset := base.Offset((ii + 1) * batchSize)
	//			gotNextOffset := base.Offset(resp.GetNextOffset())
	//			if ii < numIters-1 {
	//				if gotNextOffset != expectedNextOffset {
	//					glog.Fatalf("Expected next offset: %d, Got: %d, Partition: %d",
	//						expectedNextOffset, gotNextOffset, prtID)
	//				}
	//			} else {
	//				glog.Infof("Scan should have finished now. Next offset: %d", gotNextOffset)
	//				if !(gotNextOffset == expectedNextOffset || gotNextOffset == -1) {
	//					glog.Fatalf("Expected next offset: %d or -1, Got: %d, Partition: %d",
	//						expectedNextOffset, gotNextOffset, prtID)
	//				}
	//			}
	//			if ii < numIters {
	//				for jj := 0; jj < batchSize; jj++ {
	//					expectedVal := []byte(fmt.Sprintf("value-%d", (ii*batchSize)+jj))
	//					if bytes.Compare(resp.GetValues()[jj].Value, expectedVal) != 0 {
	//						glog.Fatalf("Expected value: %s, Got: %s, Partition: %d",
	//							string(expectedVal), string(resp.GetValues()[jj].Value), prtID)
	//					}
	//				}
	//			} else {
	//				if len(resp.GetValues()) != 0 {
	//					glog.Fatalf("Expected 0 records, got: %d from partition: %d",
	//						len(resp.GetValues()), prtID)
	//				}
	//			}
	//			if gotNextOffset >= 0 {
	//				prevNextOffset = int64(gotNextOffset)
	//			}
	//		}
	//		consumersDone <- struct{}{}
	//	}(pid)
	//}
	//for ii := 0; ii < len(partIDs); ii++ {
	//	<-consumersDone
	//}
	//close(consumersDone)
	glog.Infof("All consumers have finished!")
}
