package hedwig

import (
	"context"
	"eeylops/server/storage"
	sbase "eeylops/server/storage/base"
	"eeylops/util/logging"
	"github.com/hashicorp/raft"
	"io"
)

type InstanceFSM struct {
	storageController *storage.StorageController
	logger            *logging.PrefixLogger
}

func NewFSM(controller *storage.StorageController, logger *logging.PrefixLogger) *InstanceFSM {
	fsm := InstanceFSM{
		storageController: controller,
		logger:            logger,
	}
	return &fsm
}

func (fsm *InstanceFSM) Apply(log *raft.Log) interface{} {
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
	case KAppendCommand:
		return fsm.append(cmd, log)
	case KCommitCommand:
		return fsm.commit(cmd, log)
	case KRegisterConsumerCommand:
		return fsm.registerConsumer(cmd, log)
	default:
		fsm.logger.Fatalf("Invalid command type: %d", cmd.CommandType)
		return nil
	}
}

func (fsm *InstanceFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (fsm *InstanceFSM) Restore(closer io.ReadCloser) error {
	return nil
}

func (fsm *InstanceFSM) append(cmd *Command, log *raft.Log) *FSMResponse {
	if len(cmd.AppendCommand.Data) == 0 {
		fsm.logger.Fatalf("Received an append command with nothing to append. Log Index: %d, Log Term: %d",
			log.Index, log.Term)
	}
	var resp FSMResponse
	resp.CommandType = cmd.CommandType
	resp.Error = nil
	prt, err := fsm.storageController.GetPartition(cmd.AppendCommand.TopicID, cmd.AppendCommand.PartitionID)
	if err != nil {
		if err == storage.ErrPartitionNotFound {
			resp.Error = err
			return &resp
		} else if err == storage.ErrTopicNotFound {
			resp.Error = err
			return &resp
		} else {
			fsm.logger.Fatalf("Unable to get partition: %d, topic ID: %d due to err: %s",
				cmd.AppendCommand.PartitionID, cmd.AppendCommand.TopicID, err.Error())
		}
	}
	arg := sbase.AppendEntriesArg{
		Entries:   cmd.AppendCommand.Data,
		Timestamp: cmd.AppendCommand.Timestamp,
		RLogIdx:   int64(log.Index),
	}
	ret := prt.Append(context.Background(), &arg)
	if ret.Error != nil {
		if ret.Error == storage.ErrPartitionClosed {
			fsm.logger.Warningf("The partition has been closed. This could have happened only if the topic"+
				"was recently deleted. Skipping append for partition: %d, topic: %d, log index: %d, term: %d",
				cmd.AppendCommand.PartitionID, cmd.AppendCommand.TopicID, log.Index, log.Term)
			resp.Error = ret.Error
			return &resp
		} else if ret.Error == storage.ErrInvalidRLogIdx {
			fsm.logger.Warningf("An older replicated log index was attempted on for Partition ID: %d, "+
				"Topic ID: %d. Log Index: %d, Log Term: %d. Skipping this append as the partition already"+
				"has this message", cmd.AppendCommand.PartitionID, cmd.AppendCommand.TopicID, log.Index, log.Term)
			return &resp
		} else {
			fsm.logger.Fatalf("Unable to append entries to partition: %d, topic: %d, log index: %d, "+
				"term: %d due to err: %s", cmd.AppendCommand.PartitionID, cmd.AppendCommand.TopicID, log.Index,
				log.Term, ret.Error.Error())
		}
	}
	return &resp
}

func (fsm *InstanceFSM) registerConsumer(cmd *Command, log *raft.Log) *FSMResponse {
	if len(cmd.RegisterConsumerCommand.ConsumerID) == 0 {
		fsm.logger.Fatalf("Invalid consumer ID, Log Index: %d, Log Term: %d", log.Index, log.Term)
	}
	if cmd.RegisterConsumerCommand.TopicID <= 0 {
		fsm.logger.Fatalf("Invalid topic ID, Log Index: %d, Log Term: %d", log.Index, log.Term)
	}
	if cmd.RegisterConsumerCommand.PartitionID <= 0 {
		fsm.logger.Fatalf("Invalid partition ID. Log Index: %d, Log Term: %d", log.Index, log.Term)
	}
	var resp FSMResponse
	resp.CommandType = cmd.CommandType
	resp.Error = nil

	// Check if topic exists before registering consumer.
	tpc, exists := doesTopicExist(cmd.RegisterConsumerCommand.TopicID, fsm.storageController)
	if !exists {
		fsm.logger.Warningf("Unable to register consumer: %s as topic: %d does not exist. Log Index: %d, "+
			"Log Term: %d", cmd.RegisterConsumerCommand.ConsumerID, cmd.RegisterConsumerCommand.TopicID, log.Index,
			log.Term)
		resp.Error = storage.ErrTopicNotFound
		return &resp
	}

	// Check if partition exists before registering consumer.
	if !doesPartitionExist(tpc, cmd.RegisterConsumerCommand.PartitionID) {
		fsm.logger.Warningf("Unable to register consumer: %s as topic: %d, partition: %d does not exist. "+
			"Log Index: %d, Log Term: %d", cmd.RegisterConsumerCommand.ConsumerID, cmd.RegisterConsumerCommand.TopicID,
			cmd.RegisterConsumerCommand.PartitionID, log.Index, log.Term)
		resp.Error = storage.ErrPartitionNotFound
		return &resp
	}

	cs := fsm.storageController.GetConsumerStore()
	err := cs.RegisterConsumer(cmd.RegisterConsumerCommand.ConsumerID, cmd.RegisterConsumerCommand.TopicID,
		uint(cmd.RegisterConsumerCommand.PartitionID), int64(log.Index))
	if err != nil {
		if err == storage.ErrInvalidRLogIdx {
			fsm.logger.Warningf("An older replicated log index was attempted to register consumer: %s, "+
				"topic: %d, Partition: %d, Log Index: %d, Log Term: %d. Skipping this log as it has already "+
				"been applied", cmd.RegisterConsumerCommand.ConsumerID, cmd.RegisterConsumerCommand.TopicID,
				cmd.RegisterConsumerCommand.PartitionID, log.Index, log.Term)
			return &resp
		}
		fsm.logger.Fatalf("Unable to register consumer: %s for topic: %d, partition: %d due to err: %s. "+
			"Log Index: %d, Log Term: %d", cmd.RegisterConsumerCommand.ConsumerID, cmd.RegisterConsumerCommand.TopicID,
			cmd.RegisterConsumerCommand.PartitionID, err.Error(), log.Index, log.Term)
	}
	return &resp
}

func (fsm *InstanceFSM) commit(cmd *Command, log *raft.Log) *FSMResponse {
	if cmd.CommitCommand.TopicID == 0 {
		fsm.logger.Fatalf("No topic name provided when for commit command")
	}
	if cmd.CommitCommand.PartitionID < 0 {
		fsm.logger.Fatalf("Invalid partition ID provided for commit command. Partition ID: %d",
			cmd.CommitCommand.PartitionID)
	}
	var resp FSMResponse
	resp.CommandType = cmd.CommandType
	resp.Error = nil
	tpc, exists := doesTopicExist(cmd.CommitCommand.TopicID, fsm.storageController)
	if !exists {
		fsm.logger.VInfof(1, "Topic: %d does not exist. Skipping this command. "+
			"Log Index: %d, Log Term: %d", cmd.CommitCommand.TopicID, log.Index, log.Term)
		resp.Error = storage.ErrTopicNotFound
		return &resp
	}
	if !doesPartitionExist(tpc, cmd.CommitCommand.PartitionID) {
		fsm.logger.VInfof(1, "Topic: %d, Partition: %d does not exist. Skipping this command. "+
			"Log Index: %d, Log Term: %d", cmd.CommitCommand.TopicID, cmd.CommitCommand.PartitionID,
			log.Index, log.Term)
		resp.Error = storage.ErrPartitionNotFound
		return &resp
	}
	cs := fsm.storageController.GetConsumerStore()
	if err := cs.Commit(cmd.CommitCommand.ConsumerID, cmd.CommitCommand.TopicID,
		uint(cmd.CommitCommand.PartitionID), cmd.CommitCommand.Offset, int64(log.Index)); err != nil {
		if err == storage.ErrConsumerNotRegistered {
			fsm.logger.VInfof(1, "Consumer: %s is not registered for partition: %d, topic: %d. "+
				"Skipping this command. Log Index: %d, Log Term: %d",
				cmd.CommitCommand.ConsumerID, cmd.CommitCommand.PartitionID, cmd.CommitCommand.TopicID,
				log.Index, log.Term)
			resp.Error = err
			return &resp
		} else if err == storage.ErrInvalidRLogIdx {
			fsm.logger.Warningf("An older replicated log index was attempted to commit consumer offset: "+
				"Consumer ID: %s, Topic: %d, Partition: %d, Offset: %d, Log Index: %d, Log Term: %d. Skipping this "+
				"log as it has already been applied",
				cmd.CommitCommand.ConsumerID, cmd.CommitCommand.TopicID, cmd.CommitCommand.PartitionID,
				cmd.CommitCommand.Offset, log.Index, log.Term)
			return &resp
		} else {
			fsm.logger.Fatalf("Unable to add consumer commit for subscriber: %s, partition: %d, topic: %d, "+
				"Log Index: %d, Log Term: %d due to err: %s", cmd.CommitCommand.ConsumerID, cmd.CommitCommand.PartitionID,
				cmd.CommitCommand.TopicID, log.Index, log.Term, err.Error())
		}
	}
	return &resp
}
