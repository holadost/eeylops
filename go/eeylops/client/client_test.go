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
