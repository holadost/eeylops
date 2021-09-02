package client

import (
	"context"
	"eeylops/comm"
	"eeylops/server/base"
	"github.com/golang/glog"
)

type Producer struct {
	topicID     base.TopicIDType
	partitionID int
	rpcClient   comm.EeylopsServiceClient
	clusterID   string
}

func newProducer(topicID base.TopicIDType, partitionID int, clusterID string, rpcClient comm.EeylopsServiceClient) *Producer {
	producer := &Producer{
		topicID:     topicID,
		partitionID: partitionID,
		clusterID:   clusterID,
		rpcClient:   rpcClient,
	}
	if producer.rpcClient == nil {
		glog.Fatalf("Invalid rpc client: %v", producer.rpcClient)
	}
	producer.initialize()
	return producer
}

func (producer *Producer) initialize() {
	// TODO: Find leader and remember that for future produces.
}

// Produce appends the given batch 'data' to the specified partition. Each item in 'data' is a byte slice.
func (producer *Producer) Produce(data [][]byte) error {
	var req comm.ProduceRequest
	req.TopicId = int32(producer.topicID)
	req.PartitionId = int32(producer.partitionID)
	req.Values = append(req.Values, data...)
	resp, err := producer.rpcClient.Produce(context.Background(), &req)
	if err != nil {
		return newError(comm.Error_KErrTransport, err.Error())
	}
	if resp.GetError().GetErrorCode() != comm.Error_KNoError {
		return newError(resp.GetError().GetErrorCode(), resp.GetError().GetErrorMsg())
	}
	return nil
}
