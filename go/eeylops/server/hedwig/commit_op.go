package hedwig

type FSMCommitOp struct {
	fsm *InstanceManager
}

func NewFSMCommitOp(fsm *InstanceManager) *FSMCommitOp {
	var op FSMCommitOp
	op.fsm = fsm
	return &op
}

func (op *FSMCommitOp) Execute() {

}

func (op *FSMCommitOp) Start() {

}
