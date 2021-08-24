package hedwig

import (
	"context"
	"eeylops/comm"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"net"
)

var (
	FLAGrpcServerHost = flag.String("rpc_server_host", "0.0.0.0", "RPC server host name/IP")
	FLAGrpcServerPort = flag.Int("rpc_server_port", 50051, "RPC server port number")
)

type RPCServer struct {
	comm.UnimplementedEeylopsServiceServer
	host           string
	port           int
	instanceMgrMap map[string]*InstanceManager
}

func NewRPCServer(host string, port int, instanceMgrMap map[string]*InstanceManager) *RPCServer {
	rpcServer := new(RPCServer)
	if len(host) == 0 {
		if len(*FLAGrpcServerHost) == 0 {
			glog.Fatalf("No host provided for RPC server")
		}
		host = *FLAGrpcServerHost
	}
	if port == 0 {
		if *FLAGrpcServerPort == 0 {
			glog.Fatalf("No port provided for RPC server")
		}
		port = *FLAGrpcServerPort
	}
	rpcServer.instanceMgrMap = instanceMgrMap
	return rpcServer
}

func (srv *RPCServer) Run() {
	s := grpc.NewServer()
	comm.RegisterEeylopsServiceServer(s, &RPCServer{})
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *FLAGrpcServerHost, *FLAGrpcServerPort))
	if err != nil {
		glog.Fatalf("Unable to listen due to err: %s", err.Error())
	}
	glog.Infof("Starting server")
	if err := s.Serve(lis); err != nil {
		glog.Fatalf("Unable to serve due to err: %s", err.Error())
	}
}

func (srv *RPCServer) CreateTopic(ctx context.Context, req *comm.CreateTopicRequest) (*comm.CreateTopicResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		resp := comm.CreateTopicResponse{Error: srv.createInvalidClusterErrorProto()}
		return &resp, nil
	}
	resp := im.AddTopic(ctx, req)
	return resp, nil
}

func (srv *RPCServer) RemoveTopic(ctx context.Context, req *comm.RemoveTopicRequest) (*comm.RemoveTopicResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		resp := comm.RemoveTopicResponse{Error: srv.createInvalidClusterErrorProto()}
		return &resp, nil
	}
	resp := im.RemoveTopic(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetTopic(ctx context.Context, req *comm.GetTopicRequest) (*comm.GetTopicResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		resp := comm.GetTopicResponse{Error: srv.createInvalidClusterErrorProto()}
		return &resp, nil
	}
	resp := im.GetTopic(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetAllTopics(ctx context.Context, req *comm.GetAllTopicsRequest) (*comm.GetAllTopicsResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		resp := comm.GetAllTopicsResponse{Error: srv.createInvalidClusterErrorProto()}
		return &resp, nil
	}
	resp := im.GetAllTopics(ctx)
	return resp, nil
}

func (srv *RPCServer) Produce(ctx context.Context, req *comm.ProduceRequest) (*comm.ProduceResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		resp := comm.ProduceResponse{Error: srv.createInvalidClusterErrorProto()}
		return &resp, nil
	}
	resp := im.Produce(ctx, req)
	return resp, nil
}

func (srv *RPCServer) Consume(ctx context.Context, req *comm.ConsumeRequest) (*comm.ConsumeResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		resp := comm.ConsumeResponse{Error: srv.createInvalidClusterErrorProto()}
		return &resp, nil
	}
	resp := im.Consume(ctx, req)
	return resp, nil
}

func (srv *RPCServer) Commit(ctx context.Context, req *comm.CommitRequest) (*comm.CommitResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		resp := comm.CommitResponse{Error: srv.createInvalidClusterErrorProto()}
		return &resp, nil
	}
	resp := im.Commit(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetLastCommitted(ctx context.Context, req *comm.LastCommittedRequest) (*comm.LastCommittedResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		resp := comm.LastCommittedResponse{Error: srv.createInvalidClusterErrorProto()}
		return &resp, nil
	}
	resp := im.GetLastCommitted(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetLeader(ctx context.Context, req *comm.GetLeaderRequest) (*comm.GetLeaderResponse, error) {
	return &comm.GetLeaderResponse{}, nil
}

func (srv *RPCServer) GetClusterConfig(ctx context.Context, req *comm.GetClusterConfigRequest) (*comm.GetClusterConfigResponse, error) {
	return &comm.GetClusterConfigResponse{}, nil
}

func (srv *RPCServer) createInvalidClusterErrorProto() *comm.Error {
	return &comm.Error{
		ErrorCode: comm.Error_KErrInvalidClusterId,
		ErrorMsg:  "Invalid cluster id",
		LeaderId:  -1,
	}
}
