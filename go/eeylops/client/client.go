package client

import (
	"context"
	"eeylops/comm"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type Client struct {
	rpcClients map[string]comm.EeylopsServiceClient
	clusterId  string
}

type NodeAddress struct {
	Host string
	Port int
}

func NewClient(clusterId string, addr NodeAddress) *Client {
	var client Client
	client.clusterId = clusterId
	cc, err := grpc.Dial(fmt.Sprintf("%s:%d", addr.Host, addr.Port), grpc.WithInsecure())
	if err != nil {
		glog.Fatalf("Unable to establish connection to server due to err: %s", err.Error())
	}
	defer cc.Close()
	rpcClient := comm.NewEeylopsServiceClient(cc)
	var req comm.GetClusterConfigRequest
	req.ClusterId = clusterId
	resp, err := rpcClient.GetClusterConfig(context.Background(), &req)
	if err != nil {
		glog.Fatalf("Unable to initialize client")
	}
	errProto := resp.GetError()
	if errProto.GetErrorCode() != comm.Error_KNoError {
		glog.Fatalf("Unexpected error code: %d, Error msg: %s", errProto.GetErrorCode(), errProto.GetErrorMsg())
	}
	return &client
}

func (client *Client) NewProducer(topicName string, partitionId int) *Producer {
	return nil
}

func (client *Client) NewConsumer(consumerId string, topicName string, partitionId int) *Consumer {
	return nil
}
