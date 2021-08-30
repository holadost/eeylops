package client

import (
	"context"
	"eeylops/comm"
	"eeylops/server/base"
	"eeylops/util"
	"errors"
	"github.com/golang/glog"
	"math/rand"
	"time"
)

type Message struct {
	Value   []byte      // Data associated with the message.
	Offset  base.Offset // Offset.
	EpochNs int64       // Epoch time(in nanoseconds) when the message was appended.
}

const defaultTimeoutSecs = 60

type ConsumerConfig struct {
	// Consumer ID. This is a compulsory parameter.
	ConsumerID string
	// Name of the topic. This is a compulsory parameter.
	TopicName string
	// Partition ID. This is a compulsory parameter.
	PartitionID int
	// Flag to indicate whether the consumer must perform auto commit. Defaults to false.
	AutoCommit bool
	// FLag to indicate whether the consumer must restart from the last read value. If false, we check
	// StartOffset, StartEpochNs to determine the best start offset. If neither of these values are set
	// as well, then we default to the earliest unexpired offset in the partition. This is optional. Defaults to
	// false.
	ResumeFromLastCommitted bool
	// This specifies the start epoch(in nanoseconds) for consume. All messages returned will have timestamps
	// >= StartEpochNs. This is an optional parameter. This must not be specified if ResumeFromLastCommitted is true.
	StartEpochNs int64
	// This specifies the end epoch(in nanoseconds) for consume. All messages returned will have timestamps
	// strictly < EndEpochNs.
	EndEpochNs int64

	// Internal fields.
	// Topic ID.
	topicID base.TopicIDType
	// Cluster ID.
	clusterID string
	// RPC client.
	rpcClient comm.EeylopsServiceClient
}

func (cc *ConsumerConfig) WithConsumerID(id string) {
	cc.ConsumerID = id
}

func (cc *ConsumerConfig) WithTopicName(name string) {
	cc.TopicName = name
}

func (cc *ConsumerConfig) WithPartitionID(id int) {
	cc.PartitionID = id
}

func (cc *ConsumerConfig) WithAutoCommit() {
	cc.AutoCommit = true
}

func (cc *ConsumerConfig) WithResumeFromLastCommitted() {
	cc.ResumeFromLastCommitted = true
}

func (cc *ConsumerConfig) WithStartEpochNs(ts int64) {
	cc.StartEpochNs = ts
}

func (cc *ConsumerConfig) WithEndEpochNs(ts int64) {
	cc.EndEpochNs = ts
}

// DefaultConsumerConfig is a helper function that returns the default consumer config with the given parameters.
func DefaultConsumerConfig(consumerID string, topicName string, partitionID int) ConsumerConfig {
	cc := ConsumerConfig{
		ConsumerID:              consumerID,
		TopicName:               topicName,
		PartitionID:             partitionID,
		AutoCommit:              false,
		ResumeFromLastCommitted: true,
		StartEpochNs:            0,
		EndEpochNs:              0,
	}
	return cc
}

// ErrConsumerDone is returned by consumer when there is nothing more to scan.
var ErrConsumerDone = errors.New("consumer has finished")
var ErrInvalidCommit = errors.New("invalid commit")

type Consumer struct {
	topicID                 base.TopicIDType
	partitionID             int
	rpcClient               comm.EeylopsServiceClient
	clusterID               string
	autoCommit              bool
	resumeFromLastCommitted bool
	startEpochNs            int64
	endEpochNs              int64
	nextOffset              base.Offset
	firstConsumeDone        bool // Flag to indicate whether Consume has been called at least once.
	consumerDone            bool // Flag to indicate when consumer has finished scanning the partition.
}

func newConsumer(cfg *ConsumerConfig) *Consumer {
	consumer := &Consumer{
		topicID:                 cfg.topicID,
		partitionID:             cfg.PartitionID,
		clusterID:               cfg.clusterID,
		rpcClient:               cfg.rpcClient,
		autoCommit:              cfg.AutoCommit,
		startEpochNs:            cfg.StartEpochNs,
		endEpochNs:              cfg.EndEpochNs,
		resumeFromLastCommitted: cfg.ResumeFromLastCommitted,
		nextOffset:              -1,
		firstConsumeDone:        false,
		consumerDone:            false,
	}
	if consumer.rpcClient == nil {
		glog.Fatalf("Invalid rpc client: %v", consumer.rpcClient)
	}
	consumer.initialize()
	return consumer
}

func (consumer *Consumer) initialize() {

}

// Consume reads one or more messages from the given topic and partition.
// batchSize specifies the number of messages that need to be fetched.
// timeout specifies the duration for which the operation is
func (consumer *Consumer) Consume(batchSize int, timeout time.Duration) ([]Message, error) {
	var req comm.ConsumeRequest
	var messages []Message
	req.ClusterId = consumer.clusterID
	req.TopicId = int32(consumer.topicID)
	req.PartitionId = int32(consumer.partitionID)
	req.BatchSize = int32(batchSize)
	req.EndTimestamp = consumer.endEpochNs
	req.AutoCommit = consumer.autoCommit
	if !consumer.firstConsumeDone {
		// This is the first consume call. Use consumer config to start scans.
		req.ResumeFromLastCommittedOffset = consumer.resumeFromLastCommitted
		req.StartTimestamp = consumer.startEpochNs
	} else {
		if consumer.consumerDone {
			// We have finished consuming all messages from the partition.
			return nil, ErrConsumerDone
		}
		// For subsequent fetches, we can use the nextOffset to resume from where we left off.
		req.ResumeFromLastCommittedOffset = false
		req.StartOffset = int64(consumer.nextOffset)
	}
	if timeout <= 0 {
		timeout = time.Duration(defaultTimeoutSecs) * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	consumeFn := func(attempt int) (bool, error) {
		resp, rpcErr := consumer.rpcClient.Consume(ctx, &req)
		if (rpcErr != nil) || (resp.GetError().GetErrorCode() != comm.Error_KNoError) {
			if rpcErr != nil {
				return true, newError(comm.Error_KErrTransport, "Transport error")
			} else {
				switch resp.GetError().GetErrorCode() {
				case comm.Error_KErrNotLeader:
					// TODO: Determine leader here before retrying.
					return true, newError(comm.Error_KErrNotLeader, "unable to find leader")
				case comm.Error_KErrBackend:
					// TODO: Switch to another node before retrying.
					return true, newError(comm.Error_KErrBackend, "backend error")
				default:
					// Stop retrying as this error code must not be retried.
					return false, newError(resp.GetError().GetErrorCode(), resp.GetError().GetErrorMsg())
				}
			}
		}
		// RPC was successful. Populate messages and return.
		for _, val := range resp.GetValues() {
			var msg Message
			msg.EpochNs = val.GetTimestamp()
			msg.Offset = base.Offset(val.GetOffset())
			msg.Value = val.GetValue()
			messages = append(messages, msg)
		}
		// Remember nextOffset for the next time Consume is called.
		if resp.GetNextOffset() != -1 {
			consumer.nextOffset = base.Offset(resp.GetNextOffset())
		} else {
			consumer.consumerDone = true
		}
		return false, nil
	}
	backoffFn := func(attempt int) {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	}
	retErr := util.DoRetryWithContext(ctx, consumeFn, backoffFn)
	return messages, retErr
}

func (consumer *Consumer) Commit() error {
	if consumer.nextOffset != -1 {
		return consumer.CommitOffset(consumer.nextOffset)
	} else {
		if !consumer.firstConsumeDone {
			// Cannot commit when nothing has been consumed.
			return ErrInvalidCommit
		}
		glog.Fatalf("Commit with nextOffset = -1 must not have been called")
	}
	return nil
}

func (consumer *Consumer) CommitOffset(offset base.Offset) error {
	var req comm.CommitRequest
	req.ClusterId = consumer.clusterID
	req.TopicId = int32(consumer.topicID)
	req.PartitionId = int32(consumer.partitionID)
	if offset < 0 {
		glog.Fatalf("Invalid offset: %d. Offset must be >= 0", offset)
	}
	req.Offset = int64(consumer.nextOffset)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(defaultTimeoutSecs)*time.Second)
	defer cancel()
	commitFn := func(attempt int) (bool, error) {
		resp, err := consumer.rpcClient.Commit(ctx, &req)
		if err != nil {
			return true, err
		}
		switch resp.GetError().GetErrorCode() {
		case comm.Error_KErrNotLeader:
			// TODO: Determine leader here.
			return true, newError(comm.Error_KErrNotLeader, "unable to find leader")
		case comm.Error_KErrBackend:
			return true, newError(comm.Error_KErrBackend, "backend error")
		case comm.Error_KErrReplication:
			return true, newError(comm.Error_KErrReplication, "replication error")
		default:
			// Stop retrying as this error code must not be retried.
			return false, newError(resp.GetError().GetErrorCode(), resp.GetError().GetErrorMsg())
		}
	}
	backoffFn := func(attempt int) {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	}
	return util.DoRetryWithContext(ctx, commitFn, backoffFn)
}
