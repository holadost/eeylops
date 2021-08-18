package hedwig

import (
	"context"
	"eeylops/comm"
	"eeylops/server/base"
	"eeylops/server/replication"
	"eeylops/server/storage"
	sbase "eeylops/server/storage/base"
	"eeylops/util"
	"eeylops/util/logging"
	"github.com/golang/glog"
	"github.com/hashicorp/raft"
	"path"
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
	instanceDir           string
	clusterID             string
	peerAddresses         []PeerAddress
	replicationController *replication.RaftController
	storageController     *storage.StorageController
	fsm                   *FSM
	lastTopicIDAssigned   base.TopicIDType
}

func NewInstanceManager(opts *InstanceManagerOpts) *InstanceManager {
	im := new(InstanceManager)
	im.initialize(opts)
	return im
}

func (im *InstanceManager) initialize(opts *InstanceManagerOpts) {
	im.clusterID = opts.ClusterID
	im.peerAddresses = opts.PeerAddresses

	// Create a root directory for this instance.
	clusterRootDir := path.Join(opts.DataDirectory, im.clusterID)
	util.CreateDir(clusterRootDir)

	// Initialize storage controller.
	var stopts storage.StorageControllerOpts
	stopts.StoreGCScanIntervalSecs = 300
	stopts.RootDirectory = path.Join(clusterRootDir, "storage")
	stopts.ControllerID = im.clusterID
	im.storageController = storage.NewStorageController(stopts)
	allTopics := im.GetAllTopics(context.Background())
	max := base.TopicIDType(-1)
	for _, topic := range allTopics {
		if topic.ID > max {
			max = topic.ID
		}
	}
	im.lastTopicIDAssigned = max
	if im.lastTopicIDAssigned == -1 {
		im.lastTopicIDAssigned = 0
	}

	// Initialize FSM.
	fsm := NewFSM(im.storageController, logging.NewPrefixLogger(im.clusterID))
	im.fsm = fsm
	// TODO: Initialize replication controller.
}

func (im *InstanceManager) Produce(ctx context.Context, req *comm.PublishRequest) error {
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		glog.Errorf("Invalid topic ID. Req: %v", topicID)
		return makeHedwigError(KErrInvalidArg, nil, "Invalid topic name")
	}
	if req.GetPartitionId() < 0 {
		glog.Errorf("Invalid partition ID: %d. Req: %v", req.GetPartitionId(), req)
		return makeHedwigError(KErrInvalidArg, nil, "Invalid partition ID")
	}
	// TODO: We will have to go through the replication controller first but for now,
	// TODO: skip that and just go add via storage controller. However, this will also change
	// TODO: in the future where we will simply apply to replication controller which will
	// TODO: internally handle adding it to the storage controller. We just have to wait for
	// TODO: that result.
	appendCmd := AppendMessage{
		TopicID:     topicID,
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
	if appErr != nil {
		if !err {
			glog.Fatalf("Invalid return type for publish command. Expected error, got something else")
		}
		if retErr != nil {
			// Crash here since we cannot be sure that if the other nodes successfully applied this log or not.
			// This will require manual intervention.
			glog.Fatalf("Unable to apply to FSM due to err: %s", retErr.Error())
		}
	}
	return nil
}

func (im *InstanceManager) Consume(ctx context.Context, req *comm.SubscribeRequest) (*sbase.ScanEntriesRet, error) {
	if len(req.GetSubscriberId()) == 0 {
		glog.Errorf("Invalid argument. Subscriber id is not provided")
		return nil, makeHedwigError(KErrInvalidArg, nil, "Subscriber ID is invalid")
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		glog.Errorf("Invalid argument. Topic name is not provided")
		return nil, makeHedwigError(KErrInvalidArg, nil, "Topic name is invalid")
	}
	if req.GetPartitionId() < 0 {
		glog.Errorf("Invalid argument. Partition ID must be >= 0. Got: %d", req.GetPartitionId())
		return nil, makeHedwigError(KErrInvalidArg, nil, "Partition ID is invalid")
	}
	// Either start offset, start timestamp or resume from last committed offset must be provided.
	if req.GetStartOffset() < 0 && req.GetStartTimestamp() <= 0 && !req.GetResumeFromLastCommittedOffset() {
		glog.Errorf("Either StartOffset, StartTimestamp or ResumeFromLastCommittedOffset must be provided")
		return nil, makeHedwigError(KErrInvalidArg, nil,
			"Either StartOffset, StartTimestamp or ResumeFromLastCommittedOffset must be provided")
	}
	prt, err := im.storageController.GetPartition(topicID, int(req.GetPartitionId()))
	if err != nil {
		glog.Errorf("Unable to get partition: %d, topic: %d due to err: %s",
			req.GetPartitionId(), topicID, err.Error())
		return nil, makeHedwigError(KErrBackendStorage, err, "Unable to find partition")
	}
	batchSize := uint64(1)
	if req.GetBatchSize() > 0 {
		batchSize = uint64(req.GetBatchSize())
	}
	var scanArg sbase.ScanEntriesArg
	scanArg.NumMessages = batchSize
	if req.GetEndTimestamp() > 0 {
		scanArg.EndTimestamp = req.GetEndTimestamp()
	}
	if req.GetStartOffset() >= 0 {
		scanArg.StartOffset = base.Offset(req.GetStartOffset())
	} else if req.GetStartTimestamp() > 0 {
		scanArg.StartTimestamp = req.GetStartTimestamp()
	} else {
		// Resume from last committed offset.
		// TODO: Ensure that we are leader before we do this or it could lead to inconsistencies.
		off, err := im.storageController.GetConsumerStore().GetLastCommitted(
			req.GetSubscriberId(), topicID, uint(req.GetPartitionId()))
		if err != nil {
			glog.Errorf("Unable to determine last committed offset for subscriber: %s, topic: %d, "+
				"partition: %d due to err: %s", req.GetSubscriberId(), topicID, req.GetPartitionId(), err.Error())
			return nil, makeHedwigError(KErrBackendStorage, err, "Unable to determine last committed offset")
		}
		scanArg.StartOffset = off
	}
	ret := prt.Scan(ctx, &scanArg)
	if ret.Error != nil {
		glog.Errorf("Scan failed for subscriber: %s, topic: %d, partition: %d due to err: %s",
			req.GetSubscriberId(), topicID, req.GetPartitionId(), ret.Error.Error())
		ret.Error = makeHedwigError(KErrBackendStorage, err, "Unable to subscribe")
		return ret, ret.Error
	}
	return ret, nil
}

func (im *InstanceManager) Publish() {

}

func (im *InstanceManager) Subscribe() {

}

func (im *InstanceManager) Commit(ctx context.Context, req *comm.CommitRequest) error {
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		glog.Errorf("Invalid topic name. Req: %v", req)
		return makeHedwigError(KErrInvalidArg, nil, "Invalid topic name")
	}
	if req.GetPartitionId() < 0 {
		glog.Errorf("Invalid partition ID: %d. Req: %v", req.GetPartitionId(), req)
		return makeHedwigError(KErrInvalidArg, nil, "Invalid partition ID")
	}
	// TODO: This must go through the replication controller and we must be the leader.
	commitCmd := CommitMessage{
		TopicID:     topicID,
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
	if appErr != nil {
		retErr, err := appErr.(error)
		if !err {
			glog.Fatalf("Invalid return type for commit command. Expected error, got something else")
		}
		if retErr != nil {
			// Crash here since we cannot be sure that if the other nodes successfully applied this log or not.
			// This will require manual intervention.
			glog.Fatalf("Unable to apply to FSM due to err: %s", retErr.Error())
		}
	}
	return nil
}

func (im *InstanceManager) GetLastCommitted(ctx context.Context, req *comm.LastCommittedRequest) (base.Offset, error) {
	if len(req.GetSubscriberId()) == 0 {
		glog.Errorf("Invalid argument. Subscriber ID has not been defined")
		return -1, makeHedwigError(KErrInvalidArg, nil, "Invalid subscriber ID")
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		glog.Errorf("Invalid argument. Topic name is not defined")
		return -1, makeHedwigError(KErrInvalidArg, nil, "Invalid topic name")
	}
	if req.GetPartitionId() < 0 {
		glog.Errorf("Invalid partition ID: %d. Expected >= 0", req.GetPartitionId())
		return -1, makeHedwigError(KErrInvalidArg, nil, "Invalid partition ID")
	}
	cs := im.storageController.GetConsumerStore()
	offset, err := cs.GetLastCommitted(req.GetSubscriberId(), topicID, uint(req.GetPartitionId()))
	if err != nil {
		glog.Errorf("Unable to fetch last committed offset for subscriber: %s, topic: %d, partition: %d due "+
			"to err: %s", req.GetSubscriberId(), topicID, req.GetPartitionId(), err.Error())
		return -1, makeHedwigError(KErrBackendStorage, err, "Unable to get last committed offset")
	}
	return offset, nil
}

func (im *InstanceManager) AddTopic(ctx context.Context, req *comm.CreateTopicRequest) error {
	topic := req.GetTopic()
	if len(topic.GetTopicName()) == 0 {
		glog.Errorf("Invalid topic name. Req: %v", req)
		return makeHedwigError(KErrInvalidArg, nil, "Invalid topic name")
	}
	if len(topic.GetPartitionIds()) == 0 {
		glog.Errorf("No partitions provided while creating topic")
		return makeHedwigError(KErrInvalidArg, nil, "Invalid partition ID")
	}
	// TODO: This must go through the replication controller and we must be the leader.
	var prtIds []int
	for _, id := range topic.GetPartitionIds() {
		prtIds = append(prtIds, int(id))
	}
	var tc base.TopicConfig
	now := time.Now()
	tc.Name = topic.GetTopicName()
	tc.PartitionIDs = prtIds
	tc.TTLSeconds = int(topic.TtlSeconds)
	tc.CreatedAt = now
	tc.ID = im.lastTopicIDAssigned + 1
	addTopicMsg := AddTopicMessage{TopicConfig: tc}
	cmd := Command{
		CommandType:     KAddTopicCommand,
		AddTopicCommand: addTopicMsg,
	}
	data := Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(now.UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Time{},
	}
	appErr := im.fsm.Apply(&log)
	if appErr != nil {
		retErr, err := appErr.(error)
		if !err {
			glog.Fatalf("Invalid return type for add topic command. Expected error, got something else: %v", appErr)
		}
		if retErr != nil {
			// Crash here since we cannot be sure that if the other nodes successfully applied this log or not.
			// This will require manual intervention.
			glog.Fatalf("Unable to apply to FSM due to err: %s", retErr.Error())
		}
	}
	im.lastTopicIDAssigned++
	return nil
}

func (im *InstanceManager) RemoveTopic(ctx context.Context, req *comm.RemoveTopicRequest) error {
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		glog.Errorf("Invalid topic name. Req: %v", req)
		return makeHedwigError(KErrInvalidArg, nil, "Invalid topic name")
	}
	// TODO: This must go through the replication controller and we must be the leader.
	rmTopicMsg := RemoveTopicMessage{TopicID: topicID}
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
	if appErr != nil {
		retErr, err := appErr.(error)
		if !err {
			glog.Fatalf("Invalid return type for remove topic command. Expected error, "+
				"got something else: %v", appErr)
		}
		if retErr != nil {
			// Crash here since we cannot be sure that if the other nodes successfully applied this log or not.
			// This will require manual intervention.
			glog.Fatalf("Unable to apply to FSM due to err: %s", retErr.Error())
		}
	}
	return nil
}

func (im *InstanceManager) GetTopic(ctx context.Context, req *comm.GetTopicRequest) (*base.TopicConfig, error) {
	if len(req.GetTopicName()) == 0 {
		glog.Errorf("Invalid argument. Topic name not provided")
		return nil, makeHedwigError(KErrInvalidArg, nil, "Invalid topic name")
	}
	topic, err := im.storageController.GetTopicByName(req.GetTopicName())
	if err != nil {
		if err == storage.ErrTopicNotFound {
			return nil, makeHedwigError(KErrTopicNotFound, err, "Unable to fetch topic")
		}
		glog.Errorf("Unable to get topic: %s due to err: %s", req.GetTopicName(), err.Error())
		return nil, makeHedwigError(KErrBackendStorage, err, "Unable to fetch topic")
	}
	return &topic, nil
}

func (im *InstanceManager) GetAllTopics(ctx context.Context) []base.TopicConfig {
	topics := im.storageController.GetAllTopics()
	return topics
}

func (im *InstanceManager) RegisterSubscriber(ctx context.Context, req *comm.RegisterSubscriberRequest) error {
	if len(req.GetSubscriberId()) == 0 {
		glog.Errorf("Invalid argument. Subscriber ID must be provided")
		return makeHedwigError(KErrInvalidArg, nil, "Invalid subscriber ID")
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		glog.Errorf("Invalid argument. Topic name must be provided")
		return makeHedwigError(KErrInvalidArg, nil, "Invalid topic name")
	}
	if req.GetPartitionId() <= 0 {
		glog.Errorf("Invalid argument. Subscriber ID must be provided")
		return makeHedwigError(KErrInvalidArg, nil, "Invalid partition ID")
	}
	rgMsg := RegisterConsumerMessage{
		ConsumerID:  req.GetSubscriberId(),
		TopicID:     base.TopicIDType(req.GetTopicId()),
		PartitionID: int(req.GetPartitionId()),
	}
	cmd := Command{
		CommandType:             KRegisterConsumerCommand,
		RegisterConsumerCommand: rgMsg,
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
	if appErr != nil {
		retErr, ok := appErr.(error)
		if !ok {
			glog.Fatalf("Expected error type but got: %v", appErr)
		}
		if retErr != nil {
			glog.Fatalf("Unexpected error while registering consumer: %s", retErr.Error())
		}
	}
	return nil
}
