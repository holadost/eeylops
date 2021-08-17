package hedwig

import (
	"flag"
	"github.com/golang/glog"
)

var (
	FLAGrpcServerHost = flag.String("rpc_server_host", "", "RPC server host name/IP")
	FLAGrpcServerPort = flag.Int("rpc_server_port", 0, "RPC server port number")
)

type RPCServer struct {
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
	glog.Infof("Starting RPC server")
}
