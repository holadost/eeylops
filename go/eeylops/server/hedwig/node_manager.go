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
	instanceMgrMap map[string]*InstanceManager
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
	var iopts InstanceManagerOpts
	iopts.DataDirectory = dataDir
	if len(*FlagClusterID) == 0 {
		glog.Fatalf("Cluster ID has not been provided")
	}
	iopts.ClusterID = *FlagClusterID
	iopts.PeerAddresses = nil
	im := NewInstanceManager(&iopts)
	nm.instanceMgrMap = make(map[string]*InstanceManager)
	nm.instanceMgrMap[iopts.ClusterID] = im
	nm.rpcServer = NewRPCServer("", 0, nm.instanceMgrMap)
}

func (nm *NodeManager) Run() {
	glog.Infof("Starting node manager")
	nm.rpcServer.Run()
	select {}
}
