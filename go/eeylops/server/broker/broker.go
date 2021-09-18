package broker

import (
	"context"
	"eeylops/comm"
	"eeylops/server/base"
	"eeylops/server/replication"
	"eeylops/server/storage"
	storagebase "eeylops/server/storage/base"
	"eeylops/util"
	"eeylops/util/logging"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.com/hashicorp/raft"
	"path"
	"time"
)

var (
	FlagBrokerStoreGCIntervalSecs = flag.Int("broker_store_gc_interval_secs", 3600,
		"The interval at which the broker store is scanned to reclaim garbage")
)

type PeerAddress struct {
	Host string // Host name.
	Port int    // Port number.
}

type BrokerOpts struct {
	DataDirectory string        // Data directory for this Broker.
	PeerAddresses []PeerAddress // List of peer addresses. The first address must be the current host's address.
	BrokerID      string        // Broker ID.
}

// Broker handles the storage and replication for one or more partitions.
type Broker struct {
	instanceDir           string
	brokerID              string
	peerAddresses         []PeerAddress
	replicationController *replication.RaftController
	brokerStore           *BrokerStore
	fsm                   *BrokerFSM
	lastTopicIDAssigned   base.TopicIDType
	logger                *logging.PrefixLogger
}

func NewBroker(opts *BrokerOpts) *Broker {
	broker := new(Broker)
	broker.initialize(opts)
	return broker
}

func (broker *Broker) initialize(opts *BrokerOpts) {
	broker.logger = logging.NewPrefixLogger(broker.brokerID)
	broker.peerAddresses = opts.PeerAddresses
	broker.brokerID = opts.BrokerID
	if broker.brokerID == "" {
		broker.logger.Fatalf("Invalid broker ID: %s", broker.brokerID)
	}
	// Create a root directory for this instance.
	brokerRootDir := path.Join(opts.DataDirectory, broker.brokerID)
	util.CreateDir(brokerRootDir)

	// Initialize broker store.
	var stopts BrokerStoreOpts
	stopts.StoreGCScanIntervalSecs = *FlagBrokerStoreGCIntervalSecs
	stopts.RootDirectory = path.Join(brokerRootDir, "storage")
	stopts.BrokerID = broker.brokerID
	broker.brokerStore = NewBrokerStore(stopts)
	allTopics := broker.getAllTopics()
	max := base.TopicIDType(-1)
	for _, topic := range allTopics {
		if topic.ID > max {
			max = topic.ID
		}
	}
	broker.lastTopicIDAssigned = max
	if broker.lastTopicIDAssigned == -1 {
		broker.lastTopicIDAssigned = 0
	}

	// Initialize BrokerFSM.
	fsm := NewBrokerFSM(broker.brokerStore, logging.NewPrefixLogger(broker.brokerID))
	broker.fsm = fsm
	// TODO: Initialize replication controller.
}

func (broker *Broker) Produce(ctx context.Context, req *comm.ProduceRequest) *comm.ProduceResponse {
	makeResponse := func(ec comm.Error_ErrorCodes, err error, msg string) *comm.ProduceResponse {
		var resp comm.ProduceResponse
		resp.Error = base.MakeErrorProto(ec, err, msg)
		return &resp
	}

	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		broker.logger.VInfof(0, "Invalid topic ID. Req: %v", topicID)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "invalid topic id")
	}
	if req.GetPartitionId() < 0 {
		broker.logger.VInfof(0, "Invalid partition ID: %d. Req: %v", req.GetPartitionId(), req)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "invalid partition ID")
	}
	// TODO: Go through replication controller.
	appendCmd := base.AppendMessage{
		TopicID:     topicID,
		PartitionID: int(req.GetPartitionId()),
		Data:        req.GetValues(),
		Timestamp:   time.Now().UnixNano(),
	}
	cmd := base.Command{
		CommandType:   base.KAppendCommand,
		AppendCommand: appendCmd,
	}
	data := base.Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(time.Now().UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Time{},
	}
	tmpResp := broker.fsm.Apply(&log)
	fsmResp, ok := tmpResp.(*base.FSMResponse)
	if !ok {
		broker.logger.Fatalf("Unable to cast produce response to BrokerFSM response. Received: %v", fsmResp)
	}
	if fsmResp.CommandType != base.KAppendCommand {
		broker.logger.Fatalf("Got an unexpected command type for produce. Expected: %s(%d), Got: %s(%d)",
			base.KAppendCommand.ToString(), base.KAppendCommand, fsmResp.CommandType.ToString(), fsmResp.CommandType)
	}
	if fsmResp.Error != nil {
		if fsmResp.Error == storage.ErrPartitionClosed {
			return makeResponse(comm.Error_KErrTopicNotFound, fsmResp.Error,
				fmt.Sprintf("partition: %d is closed", req.GetPartitionId()))
		} else if fsmResp.Error == storage.ErrTopicNotFound {
			return makeResponse(comm.Error_KErrTopicNotFound, fsmResp.Error,
				fmt.Sprintf("topic: %d, partition: %d was not found", req.GetPartitionId(), req.GetTopicId()))
		} else if fsmResp.Error == storage.ErrPartitionNotFound {
			return makeResponse(comm.Error_KErrPartitionNotFound, fsmResp.Error,
				fmt.Sprintf("topic: %d, partition: %d was not found", req.GetPartitionId(), req.GetTopicId()))
		} else {
			broker.logger.Fatalf("Hit an unexpected error: %s while attempting to produce entries to "+
				"topic: %d, partition: %d", fsmResp.Error.Error(), req.GetTopicId(), req.GetPartitionId())
		}
	}
	return makeResponse(comm.Error_KNoError, nil, "")
}

func (broker *Broker) Consume(ctx context.Context, req *comm.ConsumeRequest) *comm.ConsumeResponse {
	makeResponse := func(ret *storagebase.ScanEntriesRet, ec comm.Error_ErrorCodes, err error,
		msg string) *comm.ConsumeResponse {
		var resp comm.ConsumeResponse
		ep := base.MakeErrorProto(ec, err, msg)
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
		broker.logger.VInfof(1, "Invalid argument. Consumer id is not provided")
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil, "consumer ID is invalid")
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		broker.logger.VInfof(1, "Invalid argument. Topic name is not provided")
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil, "topic name is invalid")
	}
	if req.GetPartitionId() < 0 {
		broker.logger.VInfof(1, "Invalid argument. Partition ID must be >= 0. Got: %d", req.GetPartitionId())
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil, "partition ID is invalid")
	}
	// Either start offset, start timestamp or resume from last committed offset must be provided.
	if req.GetStartOffset() < 0 && req.GetStartTimestamp() <= 0 && !req.GetResumeFromLastCommittedOffset() {
		broker.logger.VInfof(1,
			"Either StartOffset, StartTimestamp or ResumeFromLastCommittedOffset must be provided")
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil,
			"either StartOffset, StartTimestamp or ResumeFromLastCommittedOffset must be provided")
	}

	// Fetch partition.
	prt, err := broker.brokerStore.GetPartition(topicID, int(req.GetPartitionId()))
	if err != nil {
		if err == storage.ErrTopicNotFound {
			broker.logger.VInfof(1, "Received request for topic: %d, partition: %d which does not exist",
				req.GetTopicId(), req.GetPartitionId())
			return makeResponse(nil, comm.Error_KErrTopicNotFound, err,
				fmt.Sprintf("unable to find topic:partition: %d: %d", req.GetTopicId(), req.GetPartitionId()))
		} else if err == storage.ErrPartitionNotFound {
			broker.logger.VInfof(1, "Received request for topic: %d, partition: %d which does not exist",
				req.GetTopicId(), req.GetPartitionId())
			return makeResponse(nil, comm.Error_KErrPartitionNotFound, err,
				fmt.Sprintf("unable to find topic:partition: %d: %d", req.GetTopicId(), req.GetPartitionId()))
		}
		broker.logger.Errorf("Unable to get partition: %d, topic: %d due to err: %s",
			req.GetPartitionId(), topicID, err.Error())
		return makeResponse(nil, comm.Error_KErrBackend, err, "unable to find partition")
	}
	batchSize := uint64(1)
	if req.GetBatchSize() > 0 {
		batchSize = uint64(req.GetBatchSize())
	}
	var scanArg storagebase.ScanEntriesArg
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
		off, err := broker.brokerStore.GetConsumerStore().GetLastCommitted(
			req.GetConsumerId(), topicID, uint(req.GetPartitionId()))
		if err != nil {
			if err == storage.ErrConsumerNotRegistered {
				broker.logger.Warningf("Received request to resume from last committed offset but did not find "+
					"the consumer: %s registered for topic ID: %d, Partition ID: %d due to err: %s",
					req.GetConsumerId(), topicID, req.GetPartitionId(), err.Error())
				return makeResponse(nil, comm.Error_KErrInvalidArg, err,
					"unable to determine last committed offset")
			}
			broker.logger.Errorf("Unable to determine last committed offset for consumer: %s, topic: %d, "+
				"partition: %d due to err: %s", req.GetConsumerId(), topicID, req.GetPartitionId(), err.Error())
			return makeResponse(nil, comm.Error_KErrBackend, err, "unable to determine last committed offset")
		}
		scanArg.StartOffset = off
	} else {
		broker.logger.VInfof(1, "Received request that does not specify startOffset, "+
			"start timestamp or resume from last committed for topic: %d, partition: %d, consumer: %s",
			req.GetTopicId(), req.GetPartitionId(), req.GetConsumerId())
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil,
			"either start offset, start timestamp or resume from last committed must be set")
	}

	// Scan and return the values(if any).
	ret := prt.Scan(ctx, &scanArg)
	if ret.Error != nil {
		broker.logger.Errorf("Scan failed for consumer: %s, topic: %d, partition: %d due to err: %s",
			req.GetConsumerId(), topicID, req.GetPartitionId(), ret.Error.Error())
		return makeResponse(nil, comm.Error_KErrBackend, err, "unable to consume messages")
	}
	if !req.GetAutoCommit() || req.GetStartOffset() == 0 {
		return makeResponse(ret, comm.Error_KNoError, nil, "")
	}
	// Autocommit (start_offset-1) as we can now be sure that the prev message was delivered.
	cm := base.CommitMessage{
		TopicID:     base.TopicIDType(req.GetTopicId()),
		PartitionID: int(req.GetPartitionId()),
		Offset:      base.Offset(req.GetStartOffset() - 1), // Commit the offset before the requested one.
		ConsumerID:  req.GetConsumerId(),
	}
	resp := broker.internalCommit(ctx, cm)
	return makeResponse(ret, resp.GetError().GetErrorCode(), nil, resp.GetError().GetErrorMsg())
}

func (broker *Broker) Publish() {

}

func (broker *Broker) Subscribe() {

}

func (broker *Broker) Commit(ctx context.Context, req *comm.CommitRequest) *comm.CommitResponse {
	// TODO: This must go through the replication controller and we must be the leader.
	commitCmd := base.CommitMessage{
		TopicID:     base.TopicIDType(req.GetTopicId()),
		PartitionID: int(req.GetPartitionId()),
		Offset:      base.Offset(req.GetOffset()),
		ConsumerID:  req.GetConsumerId(),
	}
	return broker.internalCommit(ctx, commitCmd)
}

func (broker *Broker) internalCommit(ctx context.Context, cm base.CommitMessage) *comm.CommitResponse {
	makeResponse := func(ec comm.Error_ErrorCodes, err error, msg string) *comm.CommitResponse {
		var resp comm.CommitResponse
		resp.Error = base.MakeErrorProto(ec, err, msg)
		return &resp
	}
	topicID := cm.TopicID
	if topicID == 0 {
		broker.logger.VInfof(1, "Invalid topic name. Req: %v", cm)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "invalid topic ID")
	}
	if cm.PartitionID < 0 {
		broker.logger.VInfof(1, "Invalid partition ID: %d. Req: %v", cm.PartitionID, cm)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "invalid partition ID")
	}
	// TODO: This must go through the replication controller and we must be the leader.
	cmd := base.Command{
		CommandType:   base.KCommitCommand,
		CommitCommand: cm,
	}
	data := base.Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(time.Now().UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Time{},
	}
	tmpResp := broker.fsm.Apply(&log)
	if tmpResp == nil {
		glog.Fatalf("Fatal")
	}
	fsmResp, ok := tmpResp.(*base.FSMResponse)
	if !ok {
		broker.logger.Fatalf("Unable to cast response from BrokerFSM to FSMResponse. Received: %v", tmpResp)
	}
	if fsmResp.CommandType != base.KCommitCommand {
		broker.logger.Fatalf("Got an unexpected command type for commit. Expected: %s(%d), Got: %s(%d)",
			base.KCommitCommand.ToString(), base.KCommitCommand, fsmResp.CommandType.ToString(), fsmResp.CommandType)
	}
	if fsmResp.Error != nil {
		if fsmResp.Error == storage.ErrConsumerNotRegistered {
			return makeResponse(comm.Error_KErrConsumerNotRegistered, fsmResp.Error,
				fmt.Sprintf("consumer: %s is not registered for topic: %d, partition: %d",
					cm.ConsumerID, cm.TopicID, cm.PartitionID))
		} else if fsmResp.Error == storage.ErrPartitionNotFound || fsmResp.Error == storage.ErrTopicNotFound {
			return makeResponse(comm.Error_KErrTopicNotFound, fsmResp.Error,
				fmt.Sprintf("topic: %d, partition: %d was not found", cm.TopicID, cm.PartitionID))
		} else {
			broker.logger.Fatalf("Unexpected error while committing offset from consumer: %s, topic: %d, "+
				"partition: %d, %v", cm.ConsumerID, cm.TopicID, cm.PartitionID, fsmResp.Error)
		}
	}
	return makeResponse(comm.Error_KNoError, nil, "")
}

func (broker *Broker) GetLastCommitted(ctx context.Context, req *comm.LastCommittedRequest) *comm.LastCommittedResponse {
	makeResponse := func(offset base.Offset, ec comm.Error_ErrorCodes, err error,
		msg string) *comm.LastCommittedResponse {
		var resp comm.LastCommittedResponse
		resp.Error = base.MakeErrorProto(ec, err, msg)
		resp.Offset = int64(offset)
		return &resp
	}
	// Sanity checks.
	if len(req.GetConsumerId()) == 0 {
		broker.logger.VInfof(1, "Invalid argument. Consumer ID has not been defined")
		return makeResponse(-1, comm.Error_KErrInvalidArg, nil, "invalid consumer ID")
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		broker.logger.VInfof(1, "Invalid argument. Topic name is not defined")
		return makeResponse(-1, comm.Error_KErrInvalidArg, nil, "invalid topic")
	}
	if req.GetPartitionId() < 0 {
		broker.logger.VInfof(1, "Invalid partition ID: %d. Expected >= 0", req.GetPartitionId())
		return makeResponse(-1, comm.Error_KErrInvalidArg, nil, "invalid partition")
	}

	// Sync if required.
	if req.GetSync() {
		err := broker.doSyncOp()
		if err != nil {
			broker.logger.Errorf("Unable to doSyncOp before get last committed for consumer: %s, topic ID: %d, "+
				"partition ID: %d due to err: %s", req.GetConsumerId(), req.GetTopicId(), req.GetPartitionId(),
				err.Error())
			return makeResponse(-1, comm.Error_KErrReplication, err,
				"unable to sync before getting last committed offset")
		}
	}

	// TODO: Check if topic and partition exist.
	topic, err := broker.brokerStore.GetTopicByID(topicID)
	if err != nil {
		broker.logger.Errorf("Unable to get topic: %d due to err: %s", topicID, err.Error())
		if err == storage.ErrTopicNotFound {
			return makeResponse(-1, comm.Error_KErrTopicNotFound, nil,
				fmt.Sprintf("topic: %d does not found", topicID))
		} else {
			return makeResponse(-1, comm.Error_KErrBackend, err, "")
		}
	}
	if !util.ContainsInt(topic.PartitionIDs, int(req.GetPartitionId())) {
		return makeResponse(-1, comm.Error_KErrTopicNotFound, nil,
			fmt.Sprintf("topic: %d does not found", topicID))
	}

	// Fetch the last committed offset.
	cs := broker.brokerStore.GetConsumerStore()
	offset, err := cs.GetLastCommitted(req.GetConsumerId(), topicID, uint(req.GetPartitionId()))
	if err != nil {
		if err == storage.ErrConsumerNotRegistered {
			broker.logger.VInfof(1, "Unable to fetch last committed offset for consumer: %s, topic: %d, "+
				"partition: %d due to err: %s", req.GetConsumerId(), topicID, req.GetPartitionId(), err.Error())
			return makeResponse(-1, comm.Error_KErrConsumerNotRegistered, nil,
				fmt.Sprintf("consumer: %s is not registered for topic: %d, partition: %d",
					req.GetConsumerId(), req.GetTopicId(), req.GetPartitionId()))
		} else {
			broker.logger.Errorf("Unable to fetch last committed offset for consumer: %s, topic: %d, "+
				"partition: %d due to err: %s", req.GetConsumerId(), topicID, req.GetPartitionId(), err.Error())
			return makeResponse(-1, comm.Error_KErrBackend, err, "backend storage error")
		}
	}
	return makeResponse(offset, comm.Error_KNoError, nil, "")
}

func (broker *Broker) RegisterConsumer(ctx context.Context, req *comm.RegisterConsumerRequest) *comm.RegisterConsumerResponse {
	makeResponse := func(ec comm.Error_ErrorCodes, err error, msg string) *comm.RegisterConsumerResponse {
		var resp comm.RegisterConsumerResponse
		resp.Error = base.MakeErrorProto(ec, err, msg)
		return &resp
	}
	// Sanity checks.
	if len(req.GetConsumerId()) == 0 {
		broker.logger.VInfof(1, "Invalid argument. Consumer ID must be provided")
		return makeResponse(comm.Error_KErrInvalidArg, nil, "invalid consumer ID")
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		broker.logger.VInfof(1, "Invalid argument. Topic name must be provided")
		return makeResponse(comm.Error_KErrInvalidArg, nil, "invalid topic name")
	}
	if req.GetPartitionId() <= 0 {
		broker.logger.VInfof(1, "Invalid argument. Consumer ID must be provided")
		return makeResponse(comm.Error_KErrInvalidArg, nil, "invalid partition ID")
	}

	// Populate command and log.
	rgMsg := base.RegisterConsumerMessage{
		ConsumerID:  req.GetConsumerId(),
		TopicID:     base.TopicIDType(req.GetTopicId()),
		PartitionID: int(req.GetPartitionId()),
	}
	cmd := base.Command{
		CommandType:             base.KRegisterConsumerCommand,
		RegisterConsumerCommand: rgMsg,
	}
	data := base.Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(time.Now().UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Time{},
	}

	// Apply to BrokerFSM, wait for the response and handle errors.
	tmpResp := broker.fsm.Apply(&log)
	fsmResp, ok := tmpResp.(*base.FSMResponse)
	if !ok {
		broker.logger.Fatalf("Unable to cast response to BrokerFSM response. Received: %v", tmpResp)
	}
	if fsmResp.CommandType != base.KRegisterConsumerCommand {
		broker.logger.Fatalf("Got an unexpected command type for register consumer. Expected: %s(%d), "+
			"Got: %s(%d)", base.KRegisterConsumerCommand.ToString(), base.KRegisterConsumerCommand,
			fsmResp.CommandType.ToString(), fsmResp.CommandType)
	}
	if fsmResp.Error != nil {
		if fsmResp.Error == storage.ErrTopicNotFound {
			broker.logger.VInfof(1, "Topic: %d not found", req.GetTopicId())
			return makeResponse(comm.Error_KErrTopicNotFound, nil,
				fmt.Sprintf("Topic: %d was not found", req.GetTopicId()))
		} else if fsmResp.Error == storage.ErrPartitionNotFound {
			broker.logger.VInfof(1, "Topic: %d, Partition: %d not found", req.GetTopicId(),
				req.GetPartitionId())
			return makeResponse(comm.Error_KErrPartitionNotFound, nil,
				fmt.Sprintf("Topic: %d, partition: %d was not found", req.GetTopicId(), req.GetPartitionId()))
		} else {
			broker.logger.Fatalf("Unexpected error from BrokerFSM when attempting to "+
				"register consumer: %s for topic: %d, partition: %d. Error: %v", req.GetConsumerId(), req.GetTopicId(),
				req.GetPartitionId(), fsmResp.Error)
		}
	}
	return makeResponse(comm.Error_KNoError, nil, "")
}

func (broker *Broker) AddTopic(ctx context.Context, req *comm.CreateTopicRequest) *comm.CreateTopicResponse {
	makeResponse := func(ec comm.Error_ErrorCodes, err error, msg string) *comm.CreateTopicResponse {
		var resp comm.CreateTopicResponse
		resp.Error = base.MakeErrorProto(ec, err, msg)
		return &resp
	}
	// Sanity checks.
	topic := req.GetTopic()
	if len(topic.GetTopicName()) == 0 {
		broker.logger.Errorf("Invalid topic name. Req: %v", req)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid topic name")
	}
	if topic.GetTopicId() <= 0 {
		broker.logger.Errorf("No topic id provided")
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid topic ID")
	}
	if len(topic.GetPartitionIds()) == 0 {
		broker.logger.Errorf("No partitions provided while creating topic")
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid partition ID")
	}
	// TODO: This must go through the replication controller and we must be the leader.
	// Populate arg, command and log.
	var prtIds []int
	for _, id := range topic.GetPartitionIds() {
		prtIds = append(prtIds, int(id))
	}
	var tc base.TopicConfig
	now := time.Now()
	tc.Name = topic.GetTopicName()
	tc.ID = base.TopicIDType(topic.TopicId)
	tc.PartitionIDs = prtIds
	tc.TTLSeconds = int(topic.TtlSeconds)
	tc.CreatedAt = now
	addTopicMsg := base.AddTopicMessage{TopicConfig: tc}
	cmd := base.Command{
		CommandType:     base.KAddTopicCommand,
		AddTopicCommand: addTopicMsg,
	}
	data := base.Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(now.UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Now(),
	}

	// Apply to BrokerFSM, wait for response and handle errors.
	tmpResp := broker.fsm.Apply(&log)
	fsmResp, ok := tmpResp.(*base.FSMResponse)
	if !ok {
		broker.logger.Fatalf("Invalid response from BrokerFSM. Received: %v", tmpResp)
	}
	if fsmResp.CommandType != base.KAddTopicCommand {
		broker.logger.Fatalf("Got an unexpected command type for add topic. Expected: %s(%d), "+
			"Got: %s(%d)", base.KAddTopicCommand.ToString(), base.KAddTopicCommand, fsmResp.CommandType.ToString(),
			fsmResp.CommandType)
	}
	if fsmResp.Error != nil {
		if fsmResp.Error == storage.ErrTopicExists {
			broker.logger.VInfof(1, "Topic: %s already exists", req.GetTopic().GetTopicName())
			return makeResponse(comm.Error_KErrTopicExists, nil,
				fmt.Sprintf("Topic: %s already exists", req.GetTopic().GetTopicName()))
		}
		broker.logger.Fatalf("Unexpected error while creating topic: %s", fsmResp.Error.Error())
	}
	return makeResponse(comm.Error_KNoError, nil, "")
}

func (broker *Broker) RemoveTopic(ctx context.Context, req *comm.RemoveTopicRequest) *comm.RemoveTopicResponse {
	// Sanity checks.
	makeResponse := func(ec comm.Error_ErrorCodes, err error, msg string) *comm.RemoveTopicResponse {
		var resp comm.RemoveTopicResponse
		resp.Error = base.MakeErrorProto(ec, err, msg)
		return &resp
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		broker.logger.VInfof(1, "Invalid topic ID. Req: %v", req)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid topic ID")
	}

	// TODO: This must go through the replication controller and we must be the leader.
	rmTopicMsg := base.RemoveTopicMessage{TopicID: topicID}
	cmd := base.Command{
		CommandType:        base.KRemoveTopicCommand,
		RemoveTopicCommand: rmTopicMsg,
	}
	data := base.Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(time.Now().UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Now(),
	}

	// Apply to BrokerFSM, wait for response and handle errors.
	tmpResp := broker.fsm.Apply(&log)
	fsmResp, ok := tmpResp.(*base.FSMResponse)
	if !ok {
		broker.logger.Fatalf("Unable to cast to BrokerFSM response. Received: %v", tmpResp)
	}
	if fsmResp.CommandType != base.KRemoveTopicCommand {
		broker.logger.Fatalf("Got an unexpected command type for add topic. Expected: %s(%d), "+
			"Got: %s(%d)", base.KRemoveTopicCommand.ToString(), base.KRemoveTopicCommand, fsmResp.CommandType.ToString(),
			fsmResp.CommandType)
	}
	if fsmResp.Error != nil {
		if fsmResp.Error == storage.ErrTopicNotFound {
			broker.logger.VInfof(1, "Topic: %d not found", req.GetTopicId())
			return makeResponse(comm.Error_KErrTopicNotFound, nil,
				fmt.Sprintf("Topic: %d does not exist", topicID))
		}
		broker.logger.Fatalf("Unexpected error from BrokerFSM while attempting to remove topic: %d. Error: %s",
			req.GetTopicId(), fsmResp.Error.Error())
	}
	return makeResponse(comm.Error_KNoError, nil, "")
}

func (broker *Broker) doSyncOp() error {
	// TODO: Do this via the replication controller!
	cmd := base.Command{
		CommandType: base.KNoOpCommand,
	}
	data := base.Serialize(&cmd)
	log := raft.Log{
		Index:      uint64(time.Now().UnixNano()),
		Term:       0,
		Type:       0,
		Data:       data,
		Extensions: nil,
		AppendedAt: time.Time{},
	}
	appErr := broker.fsm.Apply(&log)
	if appErr != nil {
		// Crash here since we cannot be sure that if the other nodes successfully applied this log or not.
		// This will require manual intervention.
		broker.logger.Fatalf("Unable to apply to BrokerFSM due to err: %v", appErr)
	}
	return nil
}

func (broker *Broker) getAllTopics() []base.TopicConfig {
	return broker.brokerStore.GetAllTopics()
}
