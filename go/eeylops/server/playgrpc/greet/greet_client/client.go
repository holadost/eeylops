package main

import (
	"context"
	greet2 "eeylops/server/playgrpc/greet"
	"flag"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
	"strconv"
	"time"
)

func main() {
	flag.Parse()
	cc, err := grpc.Dial("0.0.0.0:50051", grpc.WithInsecure())
	if err != nil {
		glog.Fatalf("Unable to establish connection to server due to err: %s", err.Error())
	}
	defer cc.Close()
	client := greet2.NewGreetServiceClient(cc)
	doUnary(client)
	doServerStreaming(client)
	doClientStreaming(client)
}

func doUnary(client greet2.GreetServiceClient) {
	glog.Infof("\n=============================== Unary RPC ===============================\n")
	var req greet2.GreetReq
	var greeting greet2.Greeting
	greeting.FirstName = "Nikhil"
	greeting.LastName = "Srinivasan"
	req.Greeting = &greeting
	resp, err := client.Greet(context.Background(), &req)
	if err != nil {
		glog.Fatalf("Unable to get greeting from server due to err: %v", err)
	}
	glog.Infof("Response: %s", resp.GetResult())
}

func doServerStreaming(client greet2.GreetServiceClient) {
	glog.Infof("\n=============================== Server Streaming RPC ===============================\n")
	var req greet2.GreetManyTimesReq
	var greeting greet2.Greeting
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

func doClientStreaming(client greet2.GreetServiceClient) {
	glog.Infof("\n=============================== Client Streaming RPC ===============================\n")
	var requests []*greet2.LongGreetReq
	for ii := 0; ii < 10; ii++ {
		var req greet2.LongGreetReq
		var greeting greet2.Greeting
		greeting.FirstName = "Nikhil" + strconv.Itoa(ii)
		greeting.LastName = "Srinivasan"
		req.Greeting = &greeting
		requests = append(requests, &req)
	}
	stream, err := client.LongGreet(context.Background())
	if err != nil {
		glog.Fatalf("Unable to start client side streaming due to err: %s", err.Error())
	}
	for ii, req := range requests {
		err = stream.Send(req)
		if err != nil {
			if err == io.EOF {
				glog.Warningf("Server ended the stream earlier than expected?")
				break
			}
		}
		glog.Infof("Sent %d messages", ii+1)
		time.Sleep(300 * time.Millisecond)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		glog.Fatalf("Unexpected error while receiving response from server: %s", err.Error())
	}
	glog.Infof("Successfully finished client side streaming as well!  Output: %s", resp.GetResult())
}
