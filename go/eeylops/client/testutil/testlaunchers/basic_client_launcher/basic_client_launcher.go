package main

import (
	"context"
	"eeylops/comm"
	"flag"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

func main() {
	flag.Parse()
	cc, err := grpc.Dial("0.0.0.0:50051", grpc.WithInsecure())
	if err != nil {
		glog.Fatalf("Unable to establish connection to server due to err: %s", err.Error())
	}
	defer cc.Close()
	client := comm.NewEeylopsServiceClient(cc)
	doUnary(client)
}

func doUnary(client comm.EeylopsServiceClient) {
	glog.Infof("\n=============================== Unary RPC ===============================\n")
	var req comm.CreateTopicRequest
	var topic comm.Topic
	topic.PartitionIds = []int32{1, 2, 3, 4}
	topic.TtlSeconds = 86400 * 7
	topic.TopicName = "hello_rpc_topic_1"
	req.Topic = &topic
	req.ClusterId = "abcdefghij"
	resp, err := client.CreateTopic(context.Background(), &req)
	if err != nil {
		glog.Fatalf("Unable to get greeting from server due to err: %v", err)
	}
	errProto := resp.GetError()
	if errProto.ErrorCode != comm.Error_KNoError {
		glog.Fatalf("Unable to add topic due to err: %s: %s", errProto.ErrorCode.String(), errProto.ErrorMsg)
	}
}
