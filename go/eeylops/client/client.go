package client

import (
	"context"
	"eeylops/comm"
	"eeylops/server/base"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type NodeAddress struct {
	Host string
	Port int
}

type Client struct {
	rpcClient comm.EeylopsServiceClient
	clusterID string
}

func NewClient(clusterID string, addr NodeAddress) *Client {
	var client Client
	client.clusterID = clusterID
	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", addr.Host, addr.Port), grpc.WithInsecure())
	if err != nil {
		glog.Fatalf("Unable to establish connection to server due to err: %s", err.Error())
	}
	defer cc.Close()
	client.rpcClient = comm.NewEeylopsServiceClient(cc)
	return &client
}

func (client *Client) NewProducer(topicName string, partitionID int) (*Producer, error) {
	topicConfig, err := client.getTopic(topicName)
	if err != nil {
		return nil, err
	}
	if !isPartitionPresentInTopicConfig(topicConfig, partitionID) {
		return nil, newError(comm.Error_KErrPartitionNotFound,
			fmt.Sprintf("Partition: %d is not present in topic partitions: %v",
				partitionID, topicConfig.PartitionIDs))
	}
	return newProducer(topicConfig.ID, partitionID, client.clusterID, client.rpcClient), nil
}

func (client *Client) NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.ResumeFromLastCommitted {
		// Confirm that StartEpochNs is not specified.
		if cfg.StartEpochNs > 0 {
			glog.Fatalf("Cannot specify both StartEpochNs and ResumeFromLastCommitted. Only one must be set")
		}
	} else {
		if cfg.StartEpochNs <= 0 {
			glog.Fatalf("Either StartEpochNs or ResumeFromLastCommitted must be set")
		}
	}
	topicConfig, err := client.getTopic(cfg.TopicName)
	if err != nil {
		return nil, err
	}
	if !isPartitionPresentInTopicConfig(topicConfig, cfg.PartitionID) {
		return nil, newError(comm.Error_KErrPartitionNotFound,
			fmt.Sprintf("Partition: %d is not present in topic partitions: %v",
				cfg.PartitionID, topicConfig.PartitionIDs))
	}
	err = client.registerConsumer(cfg.ConsumerID, topicConfig.ID, cfg.PartitionID)
	if err != nil {
		return nil, err
	}
	cfg.rpcClient = client.rpcClient
	cfg.topicID = topicConfig.ID
	cfg.clusterID = client.clusterID
	return newConsumer(&cfg), nil
}

func (client *Client) CreateTopic(topicName string, partitionIDs []int, ttlSeconds int64) error {
	var req comm.CreateTopicRequest
	var prtIDs []int32
	for _, prtID := range partitionIDs {
		prtIDs = append(prtIDs, int32(prtID))
	}
	topic := &comm.Topic{
		TopicName:    topicName,
		PartitionIds: prtIDs,
		TtlSeconds:   int32(ttlSeconds),
	}
	req.Topic = topic
	req.ClusterId = client.clusterID
	// TODO: Perform RPC on leader
	resp, err := client.rpcClient.CreateTopic(context.Background(), &req)
	if err != nil {
		return newError(comm.Error_KErrTransport, err.Error())
	}
	errProto := resp.GetError()
	// TODO: Handle not leader errors by switching to the leader.
	if errProto.GetErrorCode() != comm.Error_KNoError {
		return newError(errProto.GetErrorCode(), errProto.GetErrorMsg())
	}
	return nil
}

func (client *Client) RemoveTopic(topicName string) error {
	topicConfig, err := client.getTopic(topicName)
	if err != nil {
		glog.Warningf("Unable to fetch topic config for: %s due to err: %s. Cannot remove topic",
			topicName, err.Error())
		return err
	}
	var req comm.RemoveTopicRequest
	req.ClusterId = client.clusterID
	req.TopicId = int32(topicConfig.ID)
	// TODO: Perform RPC on leader
	resp, err := client.rpcClient.RemoveTopic(context.Background(), &req)
	if err != nil {
		return newError(comm.Error_KErrTransport, err.Error())
	}
	// TODO: Retry on KErrNotLeader.
	if resp.GetError().GetErrorCode() != comm.Error_KNoError {
		return newError(resp.GetError().GetErrorCode(), resp.GetError().GetErrorMsg())
	}
	return nil
}

func (client *Client) registerConsumer(consumerId string, topicId base.TopicIDType, partitionId int) error {
	var req comm.RegisterConsumerRequest
	req.ClusterId = client.clusterID
	req.ConsumerId = consumerId
	req.TopicId = int32(topicId)
	req.PartitionId = int32(partitionId)
	// TODO: Perform RPC on leader
	resp, err := client.rpcClient.RegisterConsumer(context.Background(), &req)
	if err != nil {
		return newError(comm.Error_KErrTransport, err.Error())
	}
	// TODO: Retry on KErrNotLeader.
	if resp.GetError().GetErrorCode() != comm.Error_KNoError {
		return newError(resp.GetError().GetErrorCode(), resp.GetError().GetErrorMsg())
	}
	return nil
}

func (client *Client) getTopic(topicName string) (base.TopicConfig, error) {
	var req comm.GetTopicRequest
	req.TopicName = topicName
	// TODO: Perform RPC on leader
	resp, err := client.rpcClient.GetTopic(context.Background(), &req)
	if err != nil {
		return base.TopicConfig{}, newError(comm.Error_KErrTransport, err.Error())
	}
	// TODO: Retry on KErrNotLeader.
	if resp.GetError().GetErrorCode() != comm.Error_KNoError {
		return base.TopicConfig{}, newError(resp.GetError().GetErrorCode(), resp.GetError().GetErrorMsg())
	}
	topicProto := resp.GetTopic()
	var topicConfig base.TopicConfig
	topicConfig.Name = topicProto.GetTopicName()
	topicConfig.ID = base.TopicIDType(topicProto.GetTopicId())
	topicConfig.TTLSeconds = int(topicProto.GetTtlSeconds())
	var prtIDs []int
	for _, pid := range topicProto.GetPartitionIds() {
		prtIDs = append(prtIDs, int(pid))
	}
	topicConfig.PartitionIDs = prtIDs
	return topicConfig, nil
}

func (client *Client) getLastCommitted(consumerID string, topicID base.TopicIDType, partitionID int) (base.Offset, error) {
	var req comm.LastCommittedRequest
	req.ConsumerId = consumerID
	req.ClusterId = client.clusterID
	req.TopicId = int32(topicID)
	req.PartitionId = int32(partitionID)
	req.Sync = true
	// TODO: Perform RPC on leader
	resp, err := client.rpcClient.GetLastCommitted(context.Background(), &req)
	if err != nil {
		return -1, newError(comm.Error_KErrTransport, err.Error())
	}
	// TODO: Retry on KErrNotLeader.
	if resp.GetError().GetErrorCode() != comm.Error_KNoError {
		return -1, newError(resp.GetError().GetErrorCode(), resp.GetError().GetErrorMsg())
	}
	return base.Offset(resp.GetOffset()), nil
}

func isPartitionPresentInTopicConfig(topicCfg base.TopicConfig, partitionID int) (found bool) {
	found = false
	for _, prtID := range topicCfg.PartitionIDs {
		if partitionID == prtID {
			found = true
			return
		}
	}
	return
}
