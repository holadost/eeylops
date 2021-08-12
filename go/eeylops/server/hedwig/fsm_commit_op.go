package hedwig

type FSMCommitOp struct {
	fsm *FSM
}

func NewFSMCommitOp(fsm *FSM) *FSMCommitOp {
	var op FSMCommitOp
	op.fsm = fsm
	return &op
}

func (op *FSMCommitOp) Execute() {

}

func (op *FSMCommitOp) Start() {

}
