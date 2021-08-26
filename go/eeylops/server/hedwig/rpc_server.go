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
	FlagRpcServerHost = flag.String("rpc_server_host", "0.0.0.0", "RPC server host name/IP")
	FlagRpcServerPort = flag.Int("rpc_server_port", 50051, "RPC server port number")
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
		if len(*FlagRpcServerHost) == 0 {
			glog.Fatalf("No host provided for RPC server")
		}
		host = *FlagRpcServerHost
	}
	if port == 0 {
		if *FlagRpcServerPort == 0 {
			glog.Fatalf("No port provided for RPC server")
		}
		port = *FlagRpcServerPort
	}
	rpcServer.instanceMgrMap = instanceMgrMap
	return rpcServer
}

func (srv *RPCServer) Run() {
	s := grpc.NewServer()
	comm.RegisterEeylopsServiceServer(s, srv)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *FlagRpcServerHost, *FlagRpcServerPort))
	if err != nil {
		glog.Fatalf("Unable to listen on (%s:%d) due to err: %s", *FlagRpcServerHost, *FlagRpcServerPort,
			err.Error())
	}
	glog.Infof("Starting RPC server on host: %s, port: %d", *FlagRpcServerHost, *FlagRpcServerPort)
	if err := s.Serve(lis); err != nil {
		glog.Fatalf("Unable to serve due to err: %s", err.Error())
	}
	glog.Infof("RPC server has finished!")
}

func (srv *RPCServer) CreateTopic(ctx context.Context, req *comm.CreateTopicRequest) (*comm.CreateTopicResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		return &comm.CreateTopicResponse{Error: srv.createInvalidClusterErrorProto()}, nil
	}
	resp := im.AddTopic(ctx, req)
	return resp, nil
}

func (srv *RPCServer) RemoveTopic(ctx context.Context, req *comm.RemoveTopicRequest) (*comm.RemoveTopicResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		return &comm.RemoveTopicResponse{Error: srv.createInvalidClusterErrorProto()}, nil
	}
	resp := im.RemoveTopic(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetTopic(ctx context.Context, req *comm.GetTopicRequest) (*comm.GetTopicResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		return &comm.GetTopicResponse{Error: srv.createInvalidClusterErrorProto()}, nil
	}
	resp := im.GetTopic(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetAllTopics(ctx context.Context, req *comm.GetAllTopicsRequest) (*comm.GetAllTopicsResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		return &comm.GetAllTopicsResponse{Error: srv.createInvalidClusterErrorProto()}, nil
	}
	resp := im.GetAllTopics(ctx)
	return resp, nil
}

func (srv *RPCServer) Produce(ctx context.Context, req *comm.ProduceRequest) (*comm.ProduceResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		return &comm.ProduceResponse{Error: srv.createInvalidClusterErrorProto()}, nil
	}
	resp := im.Produce(ctx, req)
	return resp, nil
}

func (srv *RPCServer) Consume(ctx context.Context, req *comm.ConsumeRequest) (*comm.ConsumeResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		return &comm.ConsumeResponse{Error: srv.createInvalidClusterErrorProto()}, nil
	}
	resp := im.Consume(ctx, req)
	return resp, nil
}

func (srv *RPCServer) Commit(ctx context.Context, req *comm.CommitRequest) (*comm.CommitResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		return &comm.CommitResponse{Error: srv.createInvalidClusterErrorProto()}, nil
	}
	resp := im.Commit(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetLastCommitted(ctx context.Context, req *comm.LastCommittedRequest) (*comm.LastCommittedResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		return &comm.LastCommittedResponse{Error: srv.createInvalidClusterErrorProto()}, nil
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

func (srv *RPCServer) RegisterConsumer(ctx context.Context, req *comm.RegisterConsumerRequest) (*comm.RegisterConsumerResponse, error) {
	im, exists := srv.instanceMgrMap[req.GetClusterId()]
	if !exists {
		return &comm.RegisterConsumerResponse{Error: srv.createInvalidClusterErrorProto()}, nil
	}
	resp := im.RegisterConsumer(ctx, req)
	return resp, nil
}

func (srv *RPCServer) createInvalidClusterErrorProto() *comm.Error {
	return &comm.Error{
		ErrorCode: comm.Error_KErrInvalidClusterId,
		ErrorMsg:  "Invalid cluster id",
		LeaderId:  -1,
	}
}
