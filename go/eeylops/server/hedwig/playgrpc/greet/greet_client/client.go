package main

import (
	"context"
	"eeylops/server/hedwig/playgrpc/greet"
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
	client := greet.NewGreetServiceClient(cc)
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
