package main

import (
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
	glog.Infof("Successfully created client: %f", client)
}
