package hedwig

import (
	"eeylops/server/base"
	"flag"
	"github.com/golang/glog"
)

var (
	FlagClusterID = flag.String("cluster_id", "", "Cluster ID")
)

type NodeManager struct {
	rpcServer      *RPCServer
	instanceMgrMap map[string]*Broker
	observers      []chan *NodeManagerEvent
}

func NewNodeManager() *NodeManager {
	glog.Infof("Starting node manager")
	nm := new(NodeManager)
	nm.initialize()
	return nm
}

type NodeManagerEvent struct {
}

func (nm *NodeManager) initialize() {
	dataDir := base.GetDataDirectory()
	// For now, we are going to have only one instance.
	var iopts BrokerOpts
	iopts.DataDirectory = dataDir
	if len(*FlagClusterID) == 0 {
		glog.Fatalf("Cluster ID has not been provided")
	}
	iopts.ClusterID = *FlagClusterID
	iopts.PeerAddresses = nil
	im := NewBroker(&iopts)
	nm.instanceMgrMap = make(map[string]*Broker)
	nm.instanceMgrMap[iopts.ClusterID] = im
	nm.rpcServer = NewRPCServer("", 0, nm.instanceMgrMap)
}

func (nm *NodeManager) Run() {
	// Start RPC server.
	go nm.rpcServer.Run()
	// Block indefinitely.
	select {}
}
