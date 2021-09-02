package server

import (
	"context"
	"eeylops/comm"
	"eeylops/server/base"
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
	host             string
	port             int
	instanceSelector *BrokerSelector
	motherShip       *MotherShip
	broker           *Broker
}

func NewRPCServer(host string, port int) *RPCServer {
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
	return rpcServer
}

func TestOnlyNewRPCServer(host string, port int, testDir string) *RPCServer {
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
	rpcServer.host = host
	rpcServer.port = port
	rpcServer.motherShip = NewMotherShip(testDir)
	brokerId := "hello_world_broker"
	opts := BrokerOpts{
		DataDirectory: testDir,
		PeerAddresses: nil,
		BrokerID:      brokerId,
	}
	rpcServer.broker = NewBroker(&opts)
	return rpcServer
}

func (srv *RPCServer) Run() {
	s := grpc.NewServer()
	comm.RegisterEeylopsServiceServer(s, srv)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", srv.host, srv.port))
	if err != nil {
		glog.Fatalf("Unable to listen on (%s:%d) due to err: %s", srv.host, srv.port,
			err.Error())
	}
	glog.Infof("Starting RPC server on host: %s, port: %d", srv.host, srv.port)
	if err := s.Serve(lis); err != nil {
		glog.Fatalf("Unable to serve due to err: %s", err.Error())
	}
	glog.Infof("RPC server has finished!")
}

func (srv *RPCServer) CreateTopic(ctx context.Context,
	req *comm.CreateTopicRequest) (*comm.CreateTopicResponse, error) {
	resp := srv.motherShip.AddTopic(ctx, req)
	if resp.GetError().GetErrorCode() != comm.Error_KNoError {
		return resp, nil
	}
	topic, err := srv.motherShip.topicsConfigStore.GetTopicByName(req.GetTopic().GetTopicName())
	if err != nil {
		glog.Fatalf("Just added topic to mothership but unable to get it")
	}
	req.Topic.TopicId = int32(topic.ID)
	resp = srv.broker.AddTopic(ctx, req)
	return resp, nil
}

func (srv *RPCServer) RemoveTopic(ctx context.Context,
	req *comm.RemoveTopicRequest) (*comm.RemoveTopicResponse, error) {
	resp := srv.motherShip.RemoveTopic(ctx, req)
	if resp.GetError().GetErrorCode() != comm.Error_KNoError {
		return resp, nil
	}
	resp = srv.broker.RemoveTopic(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetTopic(ctx context.Context, req *comm.GetTopicRequest) (*comm.GetTopicResponse, error) {
	resp := srv.motherShip.GetTopic(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetAllTopics(ctx context.Context,
	req *comm.GetAllTopicsRequest) (*comm.GetAllTopicsResponse, error) {
	resp := srv.motherShip.GetAllTopics(ctx)
	return resp, nil
}

func (srv *RPCServer) Produce(ctx context.Context, req *comm.ProduceRequest) (*comm.ProduceResponse, error) {
	resp := srv.broker.Produce(ctx, req)
	return resp, nil
}

func (srv *RPCServer) Consume(ctx context.Context, req *comm.ConsumeRequest) (*comm.ConsumeResponse, error) {
	resp := srv.broker.Consume(ctx, req)
	return resp, nil
}

func (srv *RPCServer) Commit(ctx context.Context, req *comm.CommitRequest) (*comm.CommitResponse, error) {
	resp := srv.broker.Commit(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetLastCommitted(ctx context.Context,
	req *comm.LastCommittedRequest) (*comm.LastCommittedResponse, error) {
	resp := srv.broker.GetLastCommitted(ctx, req)
	return resp, nil
}

func (srv *RPCServer) GetBroker(ctx context.Context, req *comm.GetBrokerRequest) (*comm.GetBrokerResponse, error) {
	return &comm.GetBrokerResponse{}, nil
}

func (srv *RPCServer) GetClusterConfig(ctx context.Context,
	req *comm.GetClusterConfigRequest) (*comm.GetClusterConfigResponse, error) {
	return &comm.GetClusterConfigResponse{}, nil
}

func (srv *RPCServer) RegisterConsumer(ctx context.Context,
	req *comm.RegisterConsumerRequest) (*comm.RegisterConsumerResponse, error) {
	im, err := srv.instanceSelector.GetInstance(base.TopicIDType(req.GetTopicId()), int(req.GetPartitionId()))
	if err != nil {
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
