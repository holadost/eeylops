package main

import (
	"context"
	greet2 "eeylops/server/playgrpc/greet"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
	"net"
	"strconv"
	"time"
)

type Server struct {
	greet2.UnimplementedGreetServiceServer
}

func (s *Server) Greet(ctx context.Context, req *greet2.GreetReq) (*greet2.GreetResp, error) {
	firstName := req.GetGreeting().GetFirstName()
	ret := "Hello " + firstName
	var resp greet2.GreetResp
	resp.Result = ret
	glog.Infof("Hello %s %s", req.GetGreeting().GetFirstName(), req.GetGreeting().GetLastName())
	return &resp, nil
}

func (s *Server) GreetManyTimes(req *greet2.GreetManyTimesReq, stream greet2.GreetService_GreetManyTimesServer) error {
	glog.Infof("============================= GreetManyTimes invoked!! ========================================")
	firstName := req.GetGreeting().GetFirstName()
	for ii := 0; ii < 10; ii++ {
		ret := "Hello " + firstName + " " + strconv.Itoa(ii)
		resp := &greet2.GreetManyTimesResp{Result: ret}
		err := stream.Send(resp)
		if err != nil {
			return err
		}
		if ii%3 == 0 {
			glog.Infof("Sent %d message(s)", ii+1)
		}
		time.Sleep(300 * time.Millisecond)
	}
	return nil
}

func (s *Server) LongGreet(stream greet2.GreetService_LongGreetServer) error {
	glog.Infof("============================= LongGreet invoked!! ========================================")
	result := "Hello "
	count := 0
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				var resp greet2.LongGreetResp
				resp.Result = result
				serr := stream.SendAndClose(&resp)
				if serr != nil {
					glog.Fatalf("Unable to send and close client stream due to err: %s", err.Error())
				}
				break
			}
			glog.Fatalf("Received unexpected error: %s", err.Error())
		}
		if count%3 == 0 {
			glog.Infof("Received %d messages", count+1)
		}
		count += 1
		result += req.GetGreeting().GetFirstName() + "! "
	}
	return nil
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
	greet2.RegisterGreetServiceServer(s, &Server{})

	glog.Infof("Starting server")
	if err := s.Serve(lis); err != nil {
		glog.Fatalf("Unable to serve due to err: %s", err.Error())
	}
}
