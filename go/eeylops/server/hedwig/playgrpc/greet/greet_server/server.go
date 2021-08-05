package main

import (
	"eeylops/server/hedwig/playgrpc/greet"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"net"
)

type Server struct {
	greet.UnimplementedGreetServiceServer
}

func main() {
	flag.Parse()
	fmt.Println("Hello World!")
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		glog.Fatalf("Unable to listen due to err: %s", err.Error())
	}

	glog.Infof("Initializing new server")
	s := grpc.NewServer()
	greet.RegisterGreetServiceServer(s, &Server{})

	glog.Infof("Starting server")
	if err := s.Serve(lis); err != nil {
		glog.Fatalf("Unable to serve due to err: %s", err.Error())
	}
}
