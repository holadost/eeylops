package hedwig

import (
	"context"
	"eeylops/comm"
	"eeylops/server/base"
	"eeylops/server/replication"
	"eeylops/server/storage"
	"github.com/golang/glog"
	"github.com/hashicorp/raft"
	"time"
)

type PeerAddress struct {
	Host string // Host name.
	Port int    // Port number.
}

type InstanceManagerOpts struct {
	DataDirectory string        // Data directory for this InstanceManager.
	ClusterID     string        // Cluster ID.
	PeerAddresses []PeerAddress // List of peer addresses. The first address must be the current host's address.
}

// InstanceManager manages the replication and storage controller for the node.
type InstanceManager struct {
	replicationController *replication.RaftController
	storageController     *storage.StorageController
	fsm                   *FSM
}

func NewClusterController(opts *InstanceManagerOpts) *InstanceManager {
	var im InstanceManager
	var topts storage.StorageControllerOpts
	topts.StoreGCScanIntervalSecs = 300
	topts.RootDirectory = opts.DataDirectory
	topts.ControllerID = opts.ClusterID
	im.storageController = storage.NewStorageController(topts)
	return &im
}

func (im *InstanceManager) Produce(ctx context.Context, req *comm.PublishRequest) error {
	if len(req.GetTopicName()) == 0 {
		glog.Errorf("Invalid topic name. Req: %v", req.GetTopicName(), req)
		return ErrInvalidArg
	}
	if req.GetPartitionId() < 0 {
		glog.Errorf("Invalid partition ID: %d. Req: %v", req.GetPartitionId(), req)
		return ErrInvalidArg
	}
	// TODO: We will have to go through the replication controller first but for now,
	// TODO: skip that and just go add via storage controller. However, this will also change
	// TODO: in the future where we will simply apply to replication controller which will
	// TODO: internally handle adding it to the storage controller. We just have to wait for
	// TODO: that result.
	appendCmd := AppendMessage{
		TopicName:   req.GetTopicName(),
		PartitionID: int(req.GetPartitionId()),
		Data:        req.GetValues(),
		Timestamp:   time.Now().UnixNano(),
	}
	cmd := Command{
		CommandType:   KAppendCommand,
		AppendCommand: appendCmd,
	}
	data := Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(time.Now().UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Time{},
	}
	appErr := im.fsm.Apply(&log)
	retErr, err := appErr.(error)
	if !err {
		glog.Fatalf("Invalid return type for publish command. Expected error, got something else")
	}
	if retErr != nil {
		glog.Fatalf("Unable to apply to FSM due to err: %s", retErr.Error())
	}
	return nil
}

func (im *InstanceManager) Consume() {

}

func (im *InstanceManager) Publish() {

}

func (im *InstanceManager) Subscribe() {

}

func (im *InstanceManager) Commit(ctx context.Context, req *comm.CommitRequest) error {
	if len(req.GetTopicName()) == 0 {
		glog.Errorf("Invalid topic name. Req: %v", req.GetTopicName(), req)
		return ErrInvalidArg
	}
	if req.GetPartitionId() < 0 {
		glog.Errorf("Invalid partition ID: %d. Req: %v", req.GetPartitionId(), req)
		return ErrInvalidArg
	}
	commitCmd := CommitMessage{
		TopicName:   req.GetTopicName(),
		PartitionID: int(req.GetPartitionId()),
		Offset:      base.Offset(req.GetOffset()),
		ConsumerID:  req.GetSubscriberId(),
	}
	cmd := Command{
		CommandType:   KCommitCommand,
		CommitCommand: commitCmd,
	}
	data := Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(time.Now().UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Time{},
	}
	appErr := im.fsm.Apply(&log)
	retErr, err := appErr.(error)
	if !err {
		glog.Fatalf("Invalid return type for commit command. Expected error, got something else")
	}
	if retErr != nil {
		glog.Fatalf("Unable to apply to FSM due to err: %s", retErr.Error())
	}
	return nil

}

func (im *InstanceManager) GetLastCommitted(ctx context.Context) {

}

func (im *InstanceManager) AddTopic(ctx context.Context, req *comm.CreateTopicRequest) error {
	topic := req.GetTopic()
	if len(topic.GetTopicName()) == 0 {
		glog.Errorf("Invalid topic name. Req: %v", topic.GetTopicName(), req)
		return ErrInvalidArg
	}
	if len(topic.GetPartitionIds()) == 0 {
		glog.Errorf("No partitions provided while creating topic")
		return ErrInvalidArg
	}
	var prtIds []int
	for _, id := range topic.GetPartitionIds() {
		prtIds = append(prtIds, int(id))
	}
	addTopicMsg := AddTopicMessage{Topic: base.Topic{
		Name:         topic.GetTopicName(),
		PartitionIDs: prtIds,
		TTLSeconds:   int(topic.TtlSeconds),
		ToRemove:     false,
	}}
	cmd := Command{
		CommandType:     KAddTopicCommand,
		AddTopicCommand: addTopicMsg,
	}
	data := Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(time.Now().UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Time{},
	}
	appErr := im.fsm.Apply(&log)
	retErr, err := appErr.(error)
	if !err {
		glog.Fatalf("Invalid return type for commit command. Expected error, got something else")
	}
	if retErr != nil {
		glog.Fatalf("Unable to apply to FSM due to err: %s", retErr.Error())
	}
	return nil
}

func (im *InstanceManager) RemoveTopic(ctx context.Context, req *comm.RemoveTopicRequest) error {
	if len(req.GetTopicName()) == 0 {
		glog.Errorf("Invalid topic name. Req: %v", req.GetTopicName(), req)
		return ErrInvalidArg
	}
	rmTopicMsg := RemoveTopicMessage{TopicName: req.GetTopicName()}
	cmd := Command{
		CommandType:        KRemoveTopicCommand,
		RemoveTopicCommand: rmTopicMsg,
	}
	data := Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(time.Now().UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Time{},
	}
	appErr := im.fsm.Apply(&log)
	retErr, err := appErr.(error)
	if !err {
		glog.Fatalf("Invalid return type for commit command. Expected error, got something else")
	}
	if retErr != nil {
		glog.Fatalf("Unable to apply to FSM due to err: %s", retErr.Error())
	}
	return nil
}

func (im *InstanceManager) GetTopic() {

}

func (im *InstanceManager) GetAllTopics() {

}
