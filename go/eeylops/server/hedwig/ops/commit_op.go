package ops

import "eeylops/server/hedwig"

type FSMCommitOp struct {
	fsm *hedwig.InstanceManager
}

func NewFSMCommitOp(fsm *hedwig.InstanceManager) *FSMCommitOp {
	var op FSMCommitOp
	op.fsm = fsm
	return &op
}

func (op *FSMCommitOp) Execute() {

}

func (op *FSMCommitOp) Start() {

}
