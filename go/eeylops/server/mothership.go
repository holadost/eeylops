package server

import (
	"context"
	"eeylops/comm"
	"eeylops/server/base"
	"eeylops/server/storage"
	"eeylops/util"
	"eeylops/util/logging"
	"fmt"
	"github.com/hashicorp/raft"
	"path"
	"time"
	"unicode"
)

const kMotherShipDirName = "mothership"

type MotherShip struct {
	logger            *logging.PrefixLogger
	fsm               *MotherShipFSM
	topicsConfigStore *storage.TopicsConfigStore
	rootDir           string
}

func NewMotherShip(rootDir string) *MotherShip {
	ms := MotherShip{logger: logging.NewPrefixLogger("mothership")}
	msRootPath := path.Join(rootDir, kMotherShipDirName)
	util.CreateDir(msRootPath)
	ms.rootDir = msRootPath
	ms.topicsConfigStore = storage.NewTopicsConfigStoreWithTopicIDGenerationEnabled(msRootPath)
	ms.fsm = NewMotherShipFSM(ms.topicsConfigStore, ms.logger)
	return &ms
}

func (ms *MotherShip) initialize() {
}

func (ms *MotherShip) AddTopic(ctx context.Context, req *comm.CreateTopicRequest) *comm.CreateTopicResponse {
	makeResponse := func(ec comm.Error_ErrorCodes, err error, msg string) *comm.CreateTopicResponse {
		var resp comm.CreateTopicResponse
		resp.Error = makeErrorProto(ec, err, msg)
		return &resp
	}
	// Sanity checks.
	topic := req.GetTopic()
	if len(topic.GetTopicName()) == 0 {
		ms.logger.Errorf("Invalid topic name. Req: %v", req)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid topic name")
	}
	if !isTopicNameValid(topic.GetTopicName()) {
		ms.logger.Errorf("Invalid topic name: %s. Topic names must have only digits, letters or _", req)
		return makeResponse(comm.Error_KErrInvalidArg, nil,
			"Invalid characters in topic name. Only digits, letters and _ are allowed")
	}
	if len(topic.GetPartitionIds()) == 0 {
		ms.logger.Errorf("No partitions provided while creating topic")
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
	tc.PartitionIDs = prtIds
	tc.TTLSeconds = int(topic.TtlSeconds)
	tc.CreatedAt = now
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
		AppendedAt: time.Now(),
	}

	// Apply to MothershipFSM, wait for response and handle errors.
	tmpResp := ms.fsm.Apply(&log)
	fsmResp, ok := tmpResp.(*FSMResponse)
	if !ok {
		ms.logger.Fatalf("Invalid response from MothershipFSM. Received: %v", tmpResp)
	}
	if fsmResp.Error != nil {
		if fsmResp.Error == storage.ErrTopicExists {
			return makeResponse(comm.Error_KErrTopicExists, nil,
				fmt.Sprintf("Topic: %s already exists", req.GetTopic().GetTopicName()))
		}
		ms.logger.Fatalf("Unexpected error while creating topic: %s", fsmResp.Error.Error())
	}
	return makeResponse(comm.Error_KNoError, nil, "")
}

func (ms *MotherShip) RemoveTopic(ctx context.Context, req *comm.RemoveTopicRequest) *comm.RemoveTopicResponse {
	// Sanity checks.
	makeResponse := func(ec comm.Error_ErrorCodes, err error, msg string) *comm.RemoveTopicResponse {
		var resp comm.RemoveTopicResponse
		resp.Error = makeErrorProto(ec, err, msg)
		return &resp
	}
	topicID := base.TopicIDType(req.GetTopicId())
	if topicID == 0 {
		ms.logger.VInfof(1, "Invalid topic name. Req: %v", req)
		return makeResponse(comm.Error_KErrInvalidArg, nil, "Invalid topic name")
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
		AppendedAt: time.Now(),
	}

	// Apply to MothershipFSM, wait for response and handle errors.
	tmpResp := ms.fsm.Apply(&log)
	fsmResp, ok := tmpResp.(*FSMResponse)
	if !ok {
		ms.logger.Fatalf("Unable to cast to MothershipFSM response. Received: %v", tmpResp)
	}
	if fsmResp.Error != nil {
		if fsmResp.Error == storage.ErrTopicNotFound {
			return makeResponse(comm.Error_KErrTopicNotFound, nil,
				fmt.Sprintf("Topic: %d does not exist", topicID))
		}
		ms.logger.Fatalf("Unexpected error from MothershipFSM while attempting to remove topic: %d. Error: %s",
			req.GetTopicId(), fsmResp.Error.Error())
	}
	return makeResponse(comm.Error_KNoError, nil, "")
}

func (ms *MotherShip) GetTopic(ctx context.Context, req *comm.GetTopicRequest) *comm.GetTopicResponse {
	makeResponse := func(tpc *base.TopicConfig, ec comm.Error_ErrorCodes, err error,
		msg string) *comm.GetTopicResponse {
		var resp comm.GetTopicResponse
		resp.Error = makeErrorProto(ec, err, msg)
		if tpc != nil {
			var topicProto comm.Topic
			topicProto.TopicId = int32(tpc.ID)
			topicProto.TopicName = tpc.Name
			for _, prtID := range tpc.PartitionIDs {
				topicProto.PartitionIds = append(topicProto.PartitionIds, int32(prtID))
			}
			topicProto.TtlSeconds = int32(tpc.TTLSeconds)
			resp.Topic = &topicProto
		}
		return &resp
	}
	// Sanity checks.
	if len(req.GetTopicName()) == 0 {
		ms.logger.Errorf("Invalid argument. Topic name not provided")
		return makeResponse(nil, comm.Error_KErrInvalidArg, nil, "Invalid topic name")
	}

	// Sync if required. Do this only if we are the leader!!
	if req.GetSync() {
		// TODO: Do a sync op.
	}

	// Fetch topic.
	topic, err := ms.topicsConfigStore.GetTopicByName(req.GetTopicName())
	if err != nil {
		if err == storage.ErrTopicNotFound {
			return makeResponse(nil, comm.Error_KErrTopicNotFound, err,
				fmt.Sprintf("Topic: %s does not exist", req.GetTopicName()))
		}
		ms.logger.Errorf("Unable to get topic: %s due to err: %s", req.GetTopicName(), err.Error())
		return makeResponse(nil, comm.Error_KErrBackend, err, "Unable to fetch topic")
	}
	return makeResponse(&topic, comm.Error_KNoError, nil, "")
}

func (ms *MotherShip) GetAllTopics(ctx context.Context) *comm.GetAllTopicsResponse {
	// TODO: Only if we are the leader.
	topics := ms.topicsConfigStore.GetAllTopics()
	var resp comm.GetAllTopicsResponse
	resp.Error = makeErrorProto(comm.Error_KNoError, nil, "")
	for _, tpc := range topics {
		var topicProto comm.Topic
		topicProto.TopicId = int32(tpc.ID)
		topicProto.TopicName = tpc.Name
		for _, prtID := range tpc.PartitionIDs {
			topicProto.PartitionIds = append(topicProto.PartitionIds, int32(prtID))
		}
		topicProto.TtlSeconds = int32(tpc.TTLSeconds)
		resp.Topics = append(resp.Topics, &topicProto)
	}
	return &resp
}

func isTopicNameValid(name string) bool {
	for _, cc := range name {
		if unicode.IsDigit(cc) || unicode.IsLetter(cc) || cc == '_' {
			continue
		}
		return false
	}
	return true
}
