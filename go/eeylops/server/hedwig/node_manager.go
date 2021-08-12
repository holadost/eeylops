package hedwig

type NodeManager struct {
	rpcServer  *RPCServer
	fsmManager *FSMManager
}

func NewNodeManager() *NodeManager {
	nm := new(NodeManager)
	nm.initialize()
	return nm
}

func (nm *NodeManager) initialize() {

}
