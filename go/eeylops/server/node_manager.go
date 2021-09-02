package server

import (
	"eeylops/server/base"
	"github.com/golang/glog"
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

	var brokerOpts BrokerOpts
	brokerOpts.DataDirectory = dataDir
	brokerOpts.PeerAddresses = nil
	// broker := NewBroker(&brokerOpts)
	nm.rpcServer = NewRPCServer("", 0)
}

func (nm *NodeManager) Run() {
	// Start RPC server.
	go nm.rpcServer.Run()
	// Block indefinitely.
	select {}
}
