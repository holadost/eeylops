package ops

import (
	"context"
	"eeylops/comm"
	"eeylops/server/hedwig"
	"eeylops/server/replication"
	"eeylops/server/storage"
	"github.com/golang/glog"
	"github.com/hashicorp/raft"
	"time"
)

type PublishOp struct {
	replController    *replication.RaftController
	storageController *storage.StorageController
	req               *comm.PublishRequest
	ctx               context.Context
	fsm               *hedwig.FSM
}

func NewPublishOp(ctx context.Context, replCtl *replication.RaftController, stCtl *storage.StorageController,
	in *comm.PublishRequest) *PublishOp {
	op := &PublishOp{
		replController:    replCtl,
		storageController: stCtl,
		req:               in,
		ctx:               ctx,
	}
	return op
}

func NewPublishOpForSingleNode(ctx context.Context, replCtl *replication.RaftController,
	stCtl *storage.StorageController, in *comm.PublishRequest, fsm *hedwig.FSM) *PublishOp {
	op := NewPublishOp(ctx, replCtl, stCtl, in)
	op.fsm = fsm
	return op
}

func (op *PublishOp) Execute() error {
	// TODO: Use the context to ensure that we don't continue doing work even after we have been
	// TODO: cancelled.
	if len(op.req.GetTopicName()) == 0 {
		glog.Errorf("Invalid topic name. Req: %v", op.req.GetTopicName(), op.req)
		return hedwig.ErrInvalidArg
	}
	if op.req.GetPartitionId() < 0 {
		glog.Errorf("Invalid partition ID: %d. Req: %v", op.req.GetPartitionId(), op.req)
		return hedwig.ErrInvalidArg
	}
	// TODO: We will have to go through the replication controller first but for now,
	// TODO: skip that and just go add via storage controller. However, this will also change
	// TODO: in the future where we will simply apply to replication controller which will
	// TODO: internally handle adding it to the storage controller. We just have to wait for
	// TODO: that result.
	appendCmd := hedwig.AppendMessage{
		TopicName:   op.req.GetTopicName(),
		PartitionID: int(op.req.GetPartitionId()),
		Data:        op.req.GetValues(),
		Timestamp:   time.Now().UnixNano(),
	}
	cmd := hedwig.Command{
		CommandType:   hedwig.KAppendCommand,
		AppendCommand: appendCmd,
	}
	data := hedwig.Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(time.Now().UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Time{},
	}
	appErr := op.fsm.Apply(&log)
	retErr, err := appErr.(error)
	if !err {
		glog.Fatalf("Invalid return type for publish command. Expected error, got something else")
	}
	if retErr != nil {
		glog.Fatalf("Unable to apply to FSM due to err: %s", retErr.Error())
	}
	return nil
}
