package hedwig

type FSMCommitOp struct {
	fsm *ClusterController
}

func NewFSMCommitOp(fsm *ClusterController) *FSMCommitOp {
	var op FSMCommitOp
	op.fsm = fsm
	return &op
}

func (op *FSMCommitOp) Execute() {

}

func (op *FSMCommitOp) Start() {

}
