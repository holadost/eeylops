package client

import (
	"context"
	"eeylops/comm"
	"eeylops/server/base"
	"eeylops/util"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"math/rand"
	"time"
)

type NodeAddress struct {
	Host string
	Port int
}

type Client struct {
	rpcClient comm.EeylopsServiceClient
	cc        *grpc.ClientConn
}

func NewClient(addr NodeAddress) *Client {
	var client Client
	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", addr.Host, addr.Port), grpc.WithInsecure())
	if err != nil {
		glog.Fatalf("Unable to establish connection to server due to err: %s", err.Error())
	}
	client.cc = cc
	client.rpcClient = comm.NewEeylopsServiceClient(cc)
	return &client
}

func (client *Client) NewProducer(topicName string, partitionID int) (*Producer, error) {
	topicConfig, err := client.GetTopic(topicName)
	if err != nil {
		return nil, err
	}
	if !isPartitionPresentInTopicConfig(topicConfig, partitionID) {
		return nil, newError(comm.Error_KErrPartitionNotFound,
			fmt.Sprintf("Partition: %d is not present in topic partitions: %v",
				partitionID, topicConfig.PartitionIDs))
	}
	return newProducer(topicConfig.ID, partitionID, client.rpcClient), nil
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
	topicConfig, err := client.GetTopic(cfg.TopicName)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(defaultTimeoutSecs)*time.Second)
	defer cancel()
	// TODO: Perform RPC on leader
	fn := func(attempt int) (bool, error) {
		resp, err := client.rpcClient.CreateTopic(ctx, &req)
		if err != nil {
			return true, newError(comm.Error_KErrTransport, err.Error())
		}
		errProto := resp.GetError()
		// TODO: Handle not leader errors by switching to the leader.
		if errProto.GetErrorCode() != comm.Error_KNoError {
			switch errProto.GetErrorCode() {
			case comm.Error_KErrNotLeader:
				// TODO: Find the correct leader here.
				return true, newError(comm.Error_KErrNotLeader, "unable to determine leader")
			case comm.Error_KErrBackend:
				return true, newError(comm.Error_KErrBackend, "backend error")
			default:
				return false, newError(errProto.GetErrorCode(), errProto.GetErrorMsg())
			}
		}
		return false, nil
	}
	bfn := func(attempt int) {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	}
	return util.RetryWithContext(ctx, fn, bfn)
}

func (client *Client) RemoveTopic(topicName string) error {
	topicConfig, err := client.GetTopic(topicName)
	if err != nil {
		glog.Warningf("Unable to fetch topic config for: %s due to err: %s. Cannot remove topic",
			topicName, err.Error())
		return err
	}
	var req comm.RemoveTopicRequest
	req.TopicId = int32(topicConfig.ID)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(defaultTimeoutSecs)*time.Second)
	defer cancel()
	fn := func(attempt int) (bool, error) {
		resp, err := client.rpcClient.RemoveTopic(ctx, &req)
		if err != nil {
			return true, newError(comm.Error_KErrTransport, err.Error())
		}
		if resp.GetError().GetErrorCode() != comm.Error_KNoError {
			errProto := resp.GetError()
			switch errProto.GetErrorCode() {
			case comm.Error_KErrNotLeader:
				// TODO: Find the correct leader here.
				return true, newError(comm.Error_KErrNotLeader, "unable to determine leader")
			case comm.Error_KErrBackend:
				return true, newError(comm.Error_KErrBackend, "backend error")
			default:
				return false, newError(errProto.GetErrorCode(), errProto.GetErrorMsg())
			}
		}
		return false, nil
	}
	bfn := func(attempt int) {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	}
	return util.RetryWithContext(ctx, fn, bfn)
}

func (client *Client) Close() {
	client.cc.Close()
}

func (client *Client) registerConsumer(consumerId string, topicId base.TopicIDType, partitionId int) error {
	var req comm.RegisterConsumerRequest
	req.ConsumerId = consumerId
	req.TopicId = int32(topicId)
	req.PartitionId = int32(partitionId)
	// TODO: Perform RPC on leader
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(defaultTimeoutSecs)*time.Second)
	defer cancel()
	fn := func(attempt int) (bool, error) {
		resp, err := client.rpcClient.RegisterConsumer(ctx, &req)
		if err != nil {
			return true, newError(comm.Error_KErrTransport, err.Error())
		}
		if resp.GetError().GetErrorCode() != comm.Error_KNoError {
			errProto := resp.GetError()
			switch errProto.GetErrorCode() {
			case comm.Error_KErrNotLeader:
				// TODO: Find the correct leader here.
				return true, newError(comm.Error_KErrNotLeader, "unable to determine leader")
			case comm.Error_KErrBackend:
				// TODO: Find the correct leader here.
				return true, newError(comm.Error_KErrBackend, "backend error")
			default:
				return false, newError(errProto.GetErrorCode(), errProto.GetErrorMsg())
			}
		}
		return false, nil
	}
	bfn := func(attempt int) {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	}
	return util.RetryWithContext(ctx, fn, bfn)
}

func (client *Client) GetTopic(topicName string) (base.TopicConfig, error) {
	var req comm.GetTopicRequest
	req.TopicName = topicName
	var topicConfig base.TopicConfig
	// TODO: Perform RPC on leader
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(defaultTimeoutSecs)*time.Second)
	defer cancel()
	fn := func(attempt int) (bool, error) {
		resp, err := client.rpcClient.GetTopic(ctx, &req)
		if err != nil {
			return true, newError(comm.Error_KErrTransport, err.Error())
		}
		// TODO: Retry on KErrNotLeader.
		if resp.GetError().GetErrorCode() != comm.Error_KNoError {
			errProto := resp.GetError()
			switch errProto.GetErrorCode() {
			case comm.Error_KErrNotLeader:
				// TODO: Find the correct leader here.
				return true, newError(comm.Error_KErrNotLeader, "unable to determine leader")
			case comm.Error_KErrBackend:
				// TODO: Find the correct leader here.
				return true, newError(comm.Error_KErrBackend, "backend error")
			default:
				return false, newError(errProto.GetErrorCode(), errProto.GetErrorMsg())
			}
		}
		topicProto := resp.GetTopic()
		topicConfig.Name = topicProto.GetTopicName()
		topicConfig.ID = base.TopicIDType(topicProto.GetTopicId())
		topicConfig.TTLSeconds = int(topicProto.GetTtlSeconds())
		var prtIDs []int
		for _, pid := range topicProto.GetPartitionIds() {
			prtIDs = append(prtIDs, int(pid))
		}
		topicConfig.PartitionIDs = prtIDs
		return false, nil
	}
	bfn := func(attempt int) {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	}
	retErr := util.RetryWithContext(ctx, fn, bfn)
	return topicConfig, retErr
}

func (client *Client) getLastCommitted(consumerID string, topicID base.TopicIDType,
	partitionID int) (base.Offset, error) {
	var req comm.LastCommittedRequest
	req.ConsumerId = consumerID
	req.TopicId = int32(topicID)
	req.PartitionId = int32(partitionID)
	req.Sync = true
	var myOffset base.Offset
	// TODO: Perform RPC on leader
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(defaultTimeoutSecs)*time.Second)
	defer cancel()
	fn := func(attempt int) (bool, error) {
		resp, err := client.rpcClient.GetLastCommitted(context.Background(), &req)
		if err != nil {
			return true, newError(comm.Error_KErrTransport, err.Error())
		}
		// TODO: Retry on KErrNotLeader.
		if resp.GetError().GetErrorCode() != comm.Error_KNoError {
			errProto := resp.GetError()
			switch errProto.GetErrorCode() {
			case comm.Error_KErrNotLeader:
				// TODO: Find the correct leader here.
				return true, newError(comm.Error_KErrNotLeader, "unable to determine leader")
			case comm.Error_KErrBackend:
				// TODO: Find the correct leader here.
				return true, newError(comm.Error_KErrBackend, "backend error")
			default:
				return false, newError(errProto.GetErrorCode(), errProto.GetErrorMsg())
			}
		}
		myOffset = base.Offset(resp.GetOffset())
		return false, nil
	}
	bfn := func(attempt int) {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	}
	retErr := util.RetryWithContext(ctx, fn, bfn)
	return myOffset, retErr
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
