package main

import (
	"context"
	"eeylops/server/hedwig/playgrpc/greet"
	"flag"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

func main() {
	flag.Parse()
	cc, err := grpc.Dial("0.0.0.0:50051", grpc.WithInsecure())
	if err != nil {
		glog.Fatalf("Unable to establish connection to server due to err: %s", err.Error())
	}
	defer cc.Close()
	client := greet.NewGreetServiceClient(cc)
	doUnary(client)
	doServerStreaming(client)
}

func doUnary(client greet.GreetServiceClient) {
	glog.Infof("\n=============================== Unary RPC ===============================\n")
	var req greet.GreetReq
	var greeting greet.Greeting
	greeting.FirstName = "Nikhil"
	greeting.LastName = "Srinivasan"
	req.Greeting = &greeting
	resp, err := client.Greet(context.Background(), &req)
	if err != nil {
		glog.Fatalf("Unable to get greeting from server due to err: %v", err)
	}
	glog.Infof("Response: %s", resp.GetResult())
}

func doServerStreaming(client greet.GreetServiceClient) {
	glog.Infof("\n=============================== Server Streaming RPC ===============================\n")
	var req greet.GreetManyTimesReq
	var greeting greet.Greeting
	greeting.FirstName = "Nikhil"
	greeting.LastName = "Srinivasan"
	req.Greeting = &greeting
	stream, err := client.GreetManyTimes(context.Background(), &req)
	if err != nil {
		glog.Fatalf("Unable to start server stream due to err: %s", err.Error())
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.Fatalf("Error while receiving response: %s", err.Error())
		}
		glog.Infof("Received message: %s", resp.GetResult())
	}
}
