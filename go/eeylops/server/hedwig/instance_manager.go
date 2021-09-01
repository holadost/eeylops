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
	"fmt"
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
	fsm                   *InstanceFSM
	lastTopicIDAssigned   base.TopicIDType
	logger                *logging.PrefixLogger
}

func NewInstanceManager(opts *InstanceManagerOpts) *InstanceManager {
	im := new(InstanceManager)
	im.initialize(opts)
	return im
}

func (im *InstanceManager) initialize(opts *InstanceManagerOpts) {
	im.clusterID = opts.ClusterID
	im.logger = logging.NewPrefixLogger(im.clusterID)
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
	allTopics := im.getAllTopics()
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

	// Initialize InstanceFSM.
	fsm := NewFSM(im.storageController, logging.NewPrefixLogger(im.clusterID))
	im.fsm = fsm
	// TODO: Initialize replication controller.
}

func (im *InstanceManager) Produce(ctx context.Context, req *comm.ProduceRequest) *comm.ProduceResponse {
	makeResponse := func(ec comm.Error_ErrorCodes, err error, msg string) *comm.ProduceResponse {
		var resp comm.ProduceResponse
		resp.Error = makeErrorProto(ec, err, msg)
		return &resp
	}

	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		im.logger.VInfof(0, "Invalid topic ID. Req: %v", topicID)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid topic name")
	}
	if req.GetPartitionId() < 0 {
		im.logger.VInfof(0, "Invalid partition ID: %d. Req: %v", req.GetPartitionId(), req)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid partition ID")
	}
	// TODO: Go through replication controller.
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
	tmpResp := im.fsm.Apply(&log)
	fsmResp, ok := tmpResp.(*FSMResponse)
	if !ok {
		im.logger.Fatalf("Unable to cast produce response to InstanceFSM response. Received: %v", fsmResp)
	}
	if fsmResp.CommandType != KAppendCommand {
		im.logger.Fatalf("Got an unexpected command type for produce. Expected: %s(%d), Got: %s(%d)",
			KAppendCommand.ToString(), KAppendCommand, fsmResp.CommandType.ToString(), fsmResp.CommandType)
	}
	if fsmResp.Error != nil {
		im.logger.Infof("Got error: %s", fsmResp.Error.Error())
		if fsmResp.Error == storage.ErrPartitionClosed {
			return makeResponse(comm.Error_KErrTopicNotFound, fsmResp.Error,
				fmt.Sprintf("Partition: %d is closed. Has topic: %d been deleted?", req.GetPartitionId(),
					req.GetTopicId()))
		} else if fsmResp.Error == storage.ErrTopicNotFound {
			return makeResponse(comm.Error_KErrTopicNotFound, fsmResp.Error,
				fmt.Sprintf("Topic: %d, partition: %d was not found", req.GetPartitionId(), req.GetTopicId()))
		} else if fsmResp.Error == storage.ErrPartitionNotFound {
			return makeResponse(comm.Error_KErrPartitionNotFound, fsmResp.Error,
				fmt.Sprintf("Topic: %d, partition: %d was not found", req.GetPartitionId(), req.GetTopicId()))
		} else {
			im.logger.Fatalf("Hit an unexpected error: %s while attempting to produce entries to "+
				"topic: %d, partition: %d", fsmResp.Error.Error(), req.GetTopicId(), req.GetPartitionId())
		}
	}
	return makeResponse(comm.Error_KNoError, nil, "")
}

func (im *InstanceManager) Consume(ctx context.Context, req *comm.ConsumeRequest) *comm.ConsumeResponse {
	makeResponse := func(ret *sbase.ScanEntriesRet, ec comm.Error_ErrorCodes, err error, msg string) *comm.ConsumeResponse {
		var resp comm.ConsumeResponse
		ep := makeErrorProto(ec, err, msg)
		resp.Error = ep
		if ret != nil {
			resp.NextOffset = int64(ret.NextOffset)
			for _, val := range ret.Values {
				var protoVal comm.Value
				protoVal.Value = val.Value
				protoVal.Offset = int64(val.Offset)
				protoVal.Timestamp = val.Timestamp
				resp.Values = append(resp.Values, &protoVal)
			}
		}
		return &resp
	}

	// Sanity checks.
	if len(req.GetConsumerId()) == 0 {
		im.logger.VInfof(1, "Invalid argument. Subscriber id is not provided")
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil, "Subscriber ID is invalid")
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		im.logger.VInfof(1, "Invalid argument. Topic name is not provided")
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil, "Topic name is invalid")
	}
	if req.GetPartitionId() < 0 {
		im.logger.VInfof(1, "Invalid argument. Partition ID must be >= 0. Got: %d", req.GetPartitionId())
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil, "Partition ID is invalid")
	}
	// Either start offset, start timestamp or resume from last committed offset must be provided.
	if req.GetStartOffset() < 0 && req.GetStartTimestamp() <= 0 && !req.GetResumeFromLastCommittedOffset() {
		im.logger.VInfof(1,
			"Either StartOffset, StartTimestamp or ResumeFromLastCommittedOffset must be provided")
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil,
			"Either StartOffset, StartTimestamp or ResumeFromLastCommittedOffset must be provided")
	}

	// Fetch partition.
	prt, err := im.storageController.GetPartition(topicID, int(req.GetPartitionId()))
	if err != nil {
		if err == storage.ErrTopicNotFound {
			im.logger.VInfof(1, "Received request for topic: %d, partition: %d which does not exist",
				req.GetTopicId(), req.GetPartitionId())
			return makeResponse(nil, comm.Error_KErrTopicNotFound, err,
				fmt.Sprintf("Unable to find topic:partition: %d: %d", req.GetTopicId(), req.GetPartitionId()))
		} else if err == storage.ErrPartitionNotFound {
			im.logger.VInfof(1, "Received request for topic: %d, partition: %d which does not exist",
				req.GetTopicId(), req.GetPartitionId())
			return makeResponse(nil, comm.Error_KErrPartitionNotFound, err,
				fmt.Sprintf("Unable to find topic:partition: %d: %d", req.GetTopicId(), req.GetPartitionId()))
		}
		im.logger.Errorf("Unable to get partition: %d, topic: %d due to err: %s",
			req.GetPartitionId(), topicID, err.Error())
		return makeResponse(nil, comm.Error_KErrBackend, err, "Unable to find partition")
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

	// Set the starting point for the scan.
	if req.GetStartOffset() >= 0 {
		scanArg.StartOffset = base.Offset(req.GetStartOffset())
	} else if req.GetStartTimestamp() > 0 {
		scanArg.StartTimestamp = req.GetStartTimestamp()
	} else if req.GetResumeFromLastCommittedOffset() {
		// Resume from last committed offset.
		// TODO: Ensure that we are leader before we do this or it could lead to inconsistencies.
		off, err := im.storageController.GetConsumerStore().GetLastCommitted(
			req.GetConsumerId(), topicID, uint(req.GetPartitionId()))
		if err != nil {
			if err == storage.ErrConsumerNotRegistered {
				im.logger.Warningf("Received request to resume from last committed offset but did not find "+
					"the subscriber: %s registered for topic ID: %d, Partition ID: %d due to err: %s",
					req.GetConsumerId(), topicID, req.GetPartitionId(), err.Error())
				return makeResponse(nil, comm.Error_KErrInvalidArg, err, "Unable to determine last committed offset")
			}
			im.logger.Errorf("Unable to determine last committed offset for subscriber: %s, topic: %d, "+
				"partition: %d due to err: %s", req.GetConsumerId(), topicID, req.GetPartitionId(), err.Error())
			return makeResponse(nil, comm.Error_KErrBackend, err, "Unable to determine last committed offset")
		}
		scanArg.StartOffset = off
	} else {
		im.logger.VInfof(1, "Received request that does not specify startOffset, start timestamp or resume "+
			"from last committed for topic: %d, partition: %d, consumer: %s", req.GetTopicId(), req.GetPartitionId(),
			req.GetConsumerId())
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil,
			"Either start offset, start timestamp or resume from last committed must be set")
	}

	// Scan and return the values(if any).
	ret := prt.Scan(ctx, &scanArg)
	if ret.Error != nil {
		im.logger.Errorf("Scan failed for subscriber: %s, topic: %d, partition: %d due to err: %s",
			req.GetConsumerId(), topicID, req.GetPartitionId(), ret.Error.Error())
		return makeResponse(nil, comm.Error_KErrBackend, err, "Unable to consume messages")
	}
	if !req.GetAutoCommit() || req.GetStartOffset() == 0 {
		return makeResponse(ret, comm.Error_KNoError, nil, "")
	}
	// Autocommit the start offset -1 as we can now be sure that the prev message was delivered.
	cm := CommitMessage{
		TopicID:     base.TopicIDType(req.GetTopicId()),
		PartitionID: int(req.GetPartitionId()),
		Offset:      base.Offset(req.GetStartOffset() - 1), // Commit the offset before the requested one.
		ConsumerID:  req.GetConsumerId(),
	}
	resp := im.internalCommit(ctx, cm)
	return makeResponse(ret, resp.GetError().GetErrorCode(), nil, resp.GetError().GetErrorMsg())
}

func (im *InstanceManager) Publish() {

}

func (im *InstanceManager) Subscribe() {

}

func (im *InstanceManager) Commit(ctx context.Context, req *comm.CommitRequest) *comm.CommitResponse {
	// TODO: This must go through the replication controller and we must be the leader.
	commitCmd := CommitMessage{
		TopicID:     base.TopicIDType(req.GetTopicId()),
		PartitionID: int(req.GetPartitionId()),
		Offset:      base.Offset(req.GetOffset()),
		ConsumerID:  req.GetConsumerId(),
	}
	return im.internalCommit(ctx, commitCmd)
}

func (im *InstanceManager) internalCommit(ctx context.Context, cm CommitMessage) *comm.CommitResponse {
	makeResponse := func(ec comm.Error_ErrorCodes, err error, msg string) *comm.CommitResponse {
		var resp comm.CommitResponse
		resp.Error = makeErrorProto(ec, err, msg)
		return &resp
	}
	topicID := cm.TopicID
	if topicID == 0 {
		im.logger.VInfof(1, "Invalid topic name. Req: %v", cm)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid topic ID")
	}
	if cm.PartitionID < 0 {
		im.logger.VInfof(1, "Invalid partition ID: %d. Req: %v", cm.PartitionID, cm)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid partition ID")
	}
	// TODO: This must go through the replication controller and we must be the leader.
	cmd := Command{
		CommandType:   KCommitCommand,
		CommitCommand: cm,
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
	tmpResp := im.fsm.Apply(&log)
	if tmpResp == nil {
		glog.Fatalf("Fatal")
	}
	fsmResp, ok := tmpResp.(*FSMResponse)
	if !ok {
		im.logger.Fatalf("Unable to cast response from InstanceFSM to FSMResponse. Received: %v", tmpResp)
	}
	if fsmResp.Error != nil {
		if fsmResp.Error == storage.ErrConsumerNotRegistered {
			return makeResponse(comm.Error_KErrConsumerNotRegistered, fsmResp.Error,
				fmt.Sprintf("Given consumer: %s is not registered for topic: %d, partition: %d",
					cm.ConsumerID, cm.TopicID, cm.PartitionID))
		} else if fsmResp.Error == storage.ErrPartitionNotFound || fsmResp.Error == storage.ErrTopicNotFound {
			return makeResponse(comm.Error_KErrTopicNotFound, fsmResp.Error,
				fmt.Sprintf("Topic: %d, partition: %d was not found", cm.TopicID, cm.PartitionID))
		} else {
			im.logger.Fatalf("Unexpected error while committing offset from consumer: %s, topic: %d, "+
				"partition: %d, %v", cm.ConsumerID, cm.TopicID, cm.PartitionID, fsmResp.Error)
		}
	}
	return makeResponse(comm.Error_KNoError, nil, "")
}

func (im *InstanceManager) GetLastCommitted(ctx context.Context,
	req *comm.LastCommittedRequest) *comm.LastCommittedResponse {
	makeResponse := func(offset base.Offset, ec comm.Error_ErrorCodes, err error, msg string) *comm.LastCommittedResponse {
		var resp comm.LastCommittedResponse
		resp.Error = makeErrorProto(ec, err, msg)
		resp.Offset = int64(offset)
		return &resp
	}
	// Sanity checks.
	if len(req.GetConsumerId()) == 0 {
		im.logger.VInfof(1, "Invalid argument. Subscriber ID has not been defined")
		return makeResponse(-1, comm.Error_KErrInvalidArg, nil, "Invalid subscriber ID")
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		im.logger.VInfof(1, "Invalid argument. Topic name is not defined")
		return makeResponse(-1, comm.Error_KErrInvalidArg, nil, "Invalid topic")
	}
	if req.GetPartitionId() < 0 {
		im.logger.VInfof(1, "Invalid partition ID: %d. Expected >= 0", req.GetPartitionId())
		return makeResponse(-1, comm.Error_KErrInvalidArg, nil, "Invalid partition")
	}

	// Sync if required.
	if req.GetSync() {
		err := im.doSyncOp()
		if err != nil {
			im.logger.Errorf("Unable to doSyncOp before get last committed for consumer: %s, topic ID: %d, "+
				"partition ID: %d due to err: %s", req.GetConsumerId(), req.GetTopicId(), req.GetPartitionId(),
				err.Error())
			return makeResponse(-1, comm.Error_KErrReplication, err,
				"Unable to sync before getting last committed offset")
		}
	}

	// Check if topic and partition exist.
	tpc, ok := doesTopicExist(topicID, im.storageController)
	if !ok {
		return makeResponse(-1, comm.Error_KErrTopicNotFound, nil,
			fmt.Sprintf("Topic: %d does not found", topicID))
	}
	if !doesPartitionExist(tpc, int(req.GetPartitionId())) {
		return makeResponse(-1, comm.Error_KErrTopicNotFound, nil,
			fmt.Sprintf("Topic: %d, Partition: %d not found", topicID, req.GetPartitionId()))
	}

	// Fetch the last committed offset.
	cs := im.storageController.GetConsumerStore()
	offset, err := cs.GetLastCommitted(req.GetConsumerId(), topicID, uint(req.GetPartitionId()))
	if err != nil {
		if err == storage.ErrConsumerNotRegistered {
			im.logger.VInfof(1, "Unable to fetch last committed offset for subscriber: %s, topic: %d, "+
				"partition: %d due to err: %s", req.GetConsumerId(), topicID, req.GetPartitionId(), err.Error())
			return makeResponse(-1, comm.Error_KErrConsumerNotRegistered, nil,
				fmt.Sprintf("Consumer: %s is not registered for topic: %d, partition: %d",
					req.GetConsumerId(), req.GetTopicId(), req.GetPartitionId()))
		} else {
			im.logger.Errorf("Unable to fetch last committed offset for subscriber: %s, topic: %d, "+
				"partition: %d due to err: %s", req.GetConsumerId(), topicID, req.GetPartitionId(), err.Error())
			return makeResponse(-1, comm.Error_KErrBackend, err, "Backend storage error")
		}
	}
	return makeResponse(offset, comm.Error_KNoError, nil, "")
}

func (im *InstanceManager) RegisterConsumer(ctx context.Context,
	req *comm.RegisterConsumerRequest) *comm.RegisterConsumerResponse {
	makeResponse := func(ec comm.Error_ErrorCodes, err error, msg string) *comm.RegisterConsumerResponse {
		var resp comm.RegisterConsumerResponse
		resp.Error = makeErrorProto(ec, err, msg)
		return &resp
	}
	// Sanity checks.
	if len(req.GetConsumerId()) == 0 {
		im.logger.VInfof(1, "Invalid argument. Subscriber ID must be provided")
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid subscriber ID")
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		im.logger.VInfof(1, "Invalid argument. Topic name must be provided")
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid topic name")
	}
	if req.GetPartitionId() <= 0 {
		im.logger.VInfof(1, "Invalid argument. Subscriber ID must be provided")
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid partition ID")
	}

	// Populate command and log.
	rgMsg := RegisterConsumerMessage{
		ConsumerID:  req.GetConsumerId(),
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

	// Apply to InstanceFSM, wait for the response and handle errors.
	tmpResp := im.fsm.Apply(&log)
	fsmResp, ok := tmpResp.(*FSMResponse)
	if !ok {
		im.logger.Fatalf("Unable to cast response to InstanceFSM response. Received: %v", tmpResp)
	}
	if fsmResp.Error != nil {
		if fsmResp.Error == storage.ErrTopicNotFound {
			return makeResponse(comm.Error_KErrTopicNotFound, nil,
				fmt.Sprintf("Topic: %d was not found", req.GetTopicId()))
		} else if fsmResp.Error == storage.ErrPartitionNotFound {
			return makeResponse(comm.Error_KErrTopicNotFound, nil,
				fmt.Sprintf("Topic: %d, partition: %d was not found", req.GetTopicId(), req.GetPartitionId()))
		} else {
			im.logger.Fatalf("Unexpected error from InstanceFSM when attempting to register consumer: %s for "+
				"topic: %d, partition: %d. Error: %v", req.GetConsumerId(), req.GetTopicId(), req.GetPartitionId(),
				fsmResp.Error)
		}
	}
	return makeResponse(comm.Error_KNoError, nil, "")
}

func (im *InstanceManager) doSyncOp() error {
	// TODO: Do this via the replication controller!
	cmd := Command{
		CommandType: KNoOpCommand,
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
		// Crash here since we cannot be sure that if the other nodes successfully applied this log or not.
		// This will require manual intervention.
		im.logger.Fatalf("Unable to apply to InstanceFSM due to err: %v", appErr)
	}
	return nil
}

func (im *InstanceManager) getAllTopics() []base.TopicConfig {
	return im.storageController.GetAllTopics()
}
