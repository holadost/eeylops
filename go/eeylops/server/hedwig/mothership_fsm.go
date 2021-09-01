package hedwig

import (
	"eeylops/server/storage"
	"eeylops/util/logging"
	"github.com/hashicorp/raft"
	"io"
)

type MotherShipFSM struct {
	topicsConfigStore *storage.TopicsConfigStore
	logger            *logging.PrefixLogger
}

func NewMotherShipFSM(tcs *storage.TopicsConfigStore, logger *logging.PrefixLogger) *MotherShipFSM {
	fsm := MotherShipFSM{
		topicsConfigStore: tcs,
		logger:            logger,
	}
	return &fsm
}

func (fsm *MotherShipFSM) Apply(log *raft.Log) interface{} {
	if log.Data == nil || len(log.Data) == 0 {
		fsm.logger.Fatalf("Failed to apply log message as no data was found")
	}
	cmd := Deserialize(log.Data)
	switch cmd.CommandType {
	case KNoOpCommand:
		// Do nothing.
		resp := FSMResponse{
			CommandType: KNoOpCommand,
			Error:       nil,
		}
		return &resp
	case KAddTopicCommand:
		return fsm.addTopic(cmd, log)
	case KRemoveTopicCommand:
		return fsm.removeTopic(cmd, log)
	default:
		fsm.logger.Fatalf("Invalid command type: %d", cmd.CommandType)
		return nil
	}
}

func (fsm *MotherShipFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (fsm *MotherShipFSM) Restore(closer io.ReadCloser) error {
	return nil
}

func (fsm *MotherShipFSM) addTopic(cmd *Command, log *raft.Log) *FSMResponse {
	// Sanity check command.
	topic := &cmd.AddTopicCommand.TopicConfig
	if topic.ID <= 0 {
		fsm.logger.Fatalf("Invalid topic ID. Log Index: %d, Log Term: %d", log.Index, log.Term)
	}
	if len(topic.Name) == 0 {
		fsm.logger.Fatalf("Invalid topic name. Log Index: %d, Log Term: %d", log.Index, log.Term)
	}
	if len(topic.PartitionIDs) == 0 {
		fsm.logger.Fatalf("Invalid partition IDs provided. Log Index: %d, Log Term: %d", log.Index, log.Term)
	}
	var resp FSMResponse
	resp.CommandType = cmd.CommandType
	resp.Error = nil
	// Add topic.
	if err := fsm.topicsConfigStore.AddTopic(cmd.AddTopicCommand.TopicConfig, int64(log.Index)); err != nil {
		if err == storage.ErrTopicExists {
			fsm.logger.Warningf("Unable to add topic as it already exists. Topic Details: %s, "+
				"Log Index: %d, Log Term: %d", cmd.AddTopicCommand.TopicConfig.ToString(), log.Index,
				log.Term)
			resp.Error = err
			return &resp
		} else if err == storage.ErrInvalidRLogIdx {
			fsm.logger.Warningf("An older replicated log index was attempted to create a topic. "+
				"Log Index: %d, Log Term: %d. Skipping this index as it has already been applied", log.Index, log.Term)
			return &resp
		} else {
			fsm.logger.Fatalf("Unable to add topic due to err: %s. Topic details: %s, "+
				"Log Index: %d, Term: %d", err.Error(), cmd.AddTopicCommand.TopicConfig.ToString(),
				log.Index, log.Term)
		}
	}
	return &resp
}

func (fsm *MotherShipFSM) removeTopic(cmd *Command, log *raft.Log) *FSMResponse {
	// Sanity checks.
	if cmd.RemoveTopicCommand.TopicID == 0 {
		fsm.logger.Fatalf("Invalid topic ID")
	}
	var resp FSMResponse
	resp.CommandType = cmd.CommandType
	resp.Error = nil

	// Remove topic.
	if err := fsm.topicsConfigStore.RemoveTopic(cmd.RemoveTopicCommand.TopicID, int64(log.Index)); err != nil {
		if err == storage.ErrTopicNotFound {
			fsm.logger.Warningf("Unable to remove topic: %d as topic does not exist. Log Index: %d, "+
				"Log Term: %d", cmd.RemoveTopicCommand.TopicID, log.Index, log.Term)
			resp.Error = storage.ErrTopicNotFound
			return &resp
		} else if err == storage.ErrInvalidRLogIdx {
			fsm.logger.Warningf("An older replicated log index was attempted to remove topic: %d. "+
				"Log Index: %d, Log Term: %d. Skipping this log as it has already been applied",
				cmd.RemoveTopicCommand.TopicID, log.Index, log.Term)
			return &resp
		} else {
			fsm.logger.Fatalf("Unable to remove topic: %d due to err: %s, Log Index: %d, Log Term: %d",
				cmd.RemoveTopicCommand.TopicID, err.Error(), log.Index, log.Term)
		}
	}
	return &resp
}
