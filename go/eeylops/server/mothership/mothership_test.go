package mothership

import (
	"context"
	"eeylops/comm"
	"eeylops/util/testutil"
	"fmt"
	"github.com/golang/glog"
	"testing"
	"time"
)

func TestMotherShip_AddRemoveGetTopic(t *testing.T) {
	testutil.LogTestMarker("TestMotherShip_AddRemoveGetTopic")
	testDir := testutil.CreateTestDir(t, "TestMotherShip_AddRemoveGetTopic")
	generateTopicName := func(id int) string {
		return fmt.Sprintf("hello_topic_%d", id)
	}
	numTopics := 15
	im := NewMotherShip(testDir)
	// Add topics.
	for ii := 0; ii < numTopics; ii++ {
		var req comm.CreateTopicRequest
		var topic comm.Topic
		topic.PartitionIds = []int32{1, 2, 3, 4}
		topic.TtlSeconds = 86400 * 7
		topic.TopicName = generateTopicName(ii + 1)
		req.Topic = &topic
		resp := im.AddTopic(context.Background(), &req)
		err := resp.GetError()
		if err.GetErrorCode() != comm.Error_KNoError {
			glog.Fatalf("Expected no error but got: %v. Unable to add topic", err)
		}
	}

	// Get topics.
	var allTopics []*comm.Topic
	for ii := 0; ii < numTopics; ii++ {
		var req comm.GetTopicRequest
		req.TopicName = generateTopicName(ii + 1)
		resp := im.GetTopic(context.Background(), &req)
		err := resp.GetError()
		if err.GetErrorCode() != comm.Error_KNoError {
			glog.Fatalf("Error while fetching topic: %s", generateTopicName(ii+1))
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
		topic.TopicName = generateTopicName(ii + 1)
		req.Topic = &topic
		resp := im.AddTopic(context.Background(), &req)
		err := resp.GetError()
		if err.GetErrorCode() != comm.Error_KErrTopicExists {
			glog.Fatalf("Unexpected error while adding same topic: %s, %v", generateTopicName(ii), err)
		}
	}

	// Get all topics.
	resp := im.GetAllTopics(context.Background())
	allTopicsErr := resp.GetError()
	if allTopicsErr.GetErrorCode() != comm.Error_KNoError {
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
		req.TopicId = allTopics[ii].GetTopicId()
		resp := im.RemoveTopic(context.Background(), &req)
		err := resp.GetError()
		if err.GetErrorCode() != comm.Error_KNoError {
			glog.Fatalf("Unable to remove topic: %s(%d) due to err: %v",
				allTopics[ii].GetTopicName(), allTopics[ii].GetTopicId(), err)
		}
	}

	// Remove topics that don't exist.
	glog.Infof("Removing non existent topics")
	for ii := numTopics * 2; ii < numTopics*3; ii++ {
		var req comm.RemoveTopicRequest
		req.TopicId = int32(numTopics*10 + ii)
		resp := im.RemoveTopic(context.Background(), &req)
		err := resp.GetError()
		ec := comm.Error_ErrorCodes(err.GetErrorCode())
		if ec != comm.Error_KErrTopicNotFound {
			glog.Fatalf("Failed while removing non-existent topic due to unexpected error: %s", ec.String())
		}
	}

	// Get topics.
	for ii := 0; ii < numTopics; ii++ {
		var req comm.GetTopicRequest
		req.TopicName = generateTopicName(ii + 1)
		resp := im.GetTopic(context.Background(), &req)
		err := resp.GetError()
		if ii%2 == 1 {
			// This topic must exist.
			if err.GetErrorCode() != comm.Error_KNoError {
				glog.Fatalf("Error while fetching topic: %s", req.TopicName)
			}
		} else {
			// This topic must not exist.
			if err.GetErrorCode() != comm.Error_KErrTopicNotFound {
				glog.Fatalf("Expected ErrTopicNotFound but got %s instead",
					err.GetErrorCode().String())
			}
		}
	}

	// Add removed topics.
	for ii := 0; ii < numTopics; ii += 2 {
		var req comm.CreateTopicRequest
		var topic comm.Topic
		topic.PartitionIds = []int32{1, 2, 3, 4}
		topic.TtlSeconds = 86400 * 7
		topic.TopicName = generateTopicName(ii + 1)
		req.Topic = &topic
		resp := im.AddTopic(context.Background(), &req)
		err := resp.GetError()
		if err.GetErrorCode() != comm.Error_KNoError {
			glog.Fatalf("Expected no error but got: %s. Unable to add topic",
				err.GetErrorCode().String())
		}
	}

	// Get all topics again and check that we them all.
	for ii := 0; ii < numTopics; ii++ {
		var req comm.GetTopicRequest
		req.TopicName = generateTopicName(ii + 1)
		resp := im.GetTopic(context.Background(), &req)
		err := resp.GetError()
		ec := err.GetErrorCode()
		if ec != comm.Error_KNoError {
			glog.Fatalf("Error while fetching topic: %s. Error: %s", generateTopicName(ii+1), ec.String())
		}
	}
	time.Sleep(2 * time.Second)
	// TODO: Check that the removed topics have been deleted from the underlying file system.
}
