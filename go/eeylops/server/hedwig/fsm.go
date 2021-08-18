package hedwig

import (
	"context"
	"eeylops/server/storage"
	sbase "eeylops/server/storage/base"
	"eeylops/util/logging"
	"github.com/golang/glog"
	"github.com/hashicorp/raft"
	"io"
)

type FSM struct {
	storageController *storage.StorageController
	logger            *logging.PrefixLogger
}

func NewFSM(controller *storage.StorageController, logger *logging.PrefixLogger) *FSM {
	fsm := FSM{
		storageController: controller,
		logger:            logger,
	}
	return &fsm
}

func (fsm *FSM) Apply(log *raft.Log) interface{} {
	if log.Data == nil || len(log.Data) == 0 {
		fsm.logger.Fatalf("Failed to apply log message as no data was found")
	}
	cmd := Deserialize(log.Data)
	switch cmd.CommandType {
	case KNoOpCommand:
		// Do nothing.
		return nil
	case KAppendCommand:
		return fsm.append(cmd, log)
	case KCommitCommand:
		return fsm.commit(cmd, log)
	case KAddTopicCommand:
		return fsm.addTopic(cmd, log)
	case KRemoveTopicCommand:
		return fsm.removeTopic(cmd, log)
	case KRegisterConsumerCommand:
		return fsm.registerConsumer(cmd, log)
	default:
		fsm.logger.Fatalf("Invalid command type: %d", cmd.CommandType)
		return nil
	}
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (fsm *FSM) Restore(closer io.ReadCloser) error {
	return nil
}

func (fsm *FSM) append(cmd *Command, log *raft.Log) error {
	if len(cmd.AppendCommand.Data) == 0 {
		fsm.logger.Fatalf("Received an append command with nothing to append. Log Index: %d, Log Term: %d",
			log.Index, log.Term)
		return nil
	}
	prt, err := fsm.storageController.GetPartition(cmd.AppendCommand.TopicID, cmd.AppendCommand.PartitionID)
	if err != nil {
		if err == storage.ErrPartitionNotFound {
			return err
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
			glog.Warningf("The partition has been closed. This could have happened only if the topic"+
				"was recently deleted. Skipping append for partition: %d, topic: %d, log index: %d, term: %d",
				cmd.AppendCommand.PartitionID, cmd.AppendCommand.TopicID, log.Index, log.Term)
			return nil
		} else {
			fsm.logger.Fatalf("Unable to append entries to partition: %d, topic: %d, log index: %d, "+
				"term: %d due to err: %s", cmd.AppendCommand.PartitionID, cmd.AppendCommand.TopicID, log.Index,
				log.Term, ret.Error.Error())
			return ret.Error
		}
	}
	return nil
}

func (fsm *FSM) addTopic(cmd *Command, log *raft.Log) error {
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
	// Add topic.
	if err := fsm.storageController.AddTopic(cmd.AddTopicCommand.TopicConfig); err != nil {
		if err == storage.ErrTopicExists {
			fsm.logger.Warningf("Unable to add topic as it already exists. Topic Details: %s, "+
				"Log Index: %d, Log Term: %d", err.Error(), cmd.AddTopicCommand.TopicConfig.ToString(), log.Index,
				log.Term)
			return nil
		} else {
			fsm.logger.Fatalf("Unable to add topic due to err: %s. Topic details: %s, "+
				"Log Index: %d, Term: %d", err.Error(), cmd.AddTopicCommand.TopicConfig.ToString(),
				log.Index, log.Term)
			return err
		}
	}
	return nil
}

func (fsm *FSM) removeTopic(cmd *Command, log *raft.Log) error {
	// Sanity checks.
	if cmd.RemoveTopicCommand.TopicID == 0 {
		fsm.logger.Fatalf("Invalid topic ID")
	}
	// Remove topic.
	if err := fsm.storageController.RemoveTopic(cmd.RemoveTopicCommand.TopicID); err != nil {
		if err == storage.ErrTopicNotFound {
			fsm.logger.Warningf("Unable to remove topic: %d as topic does not exist. Log Index: %d, "+
				"Log Term: %d", cmd.RemoveTopicCommand.TopicID, err.Error(), log.Index, log.Term)
			return nil
		} else {
			fsm.logger.Fatalf("Unable to remove topic: %d due to err: %s, Log Index: %d, Log Term: %d",
				cmd.RemoveTopicCommand.TopicID, err.Error(), log.Index, log.Term)
			return err
		}
	}
	return nil
}

func (fsm *FSM) registerConsumer(cmd *Command, log *raft.Log) error {
	if len(cmd.RegisterConsumerCommand.ConsumerID) == 0 {
		fsm.logger.Fatalf("Invalid consumer ID, Log Index: %d, Log Term: %d", log.Index, log.Term)
	}
	if cmd.RegisterConsumerCommand.TopicID <= 0 {
		fsm.logger.Fatalf("Invalid topic ID, Log Index: %d, Log Term: %d", log.Index, log.Term)
	}
	if cmd.RegisterConsumerCommand.PartitionID <= 0 {
		fsm.logger.Fatalf("Invalid partition ID. Log Index: %d, Log Term: %d", log.Index, log.Term)
	}
	cs := fsm.storageController.GetConsumerStore()
	err := cs.RegisterConsumer(cmd.RegisterConsumerCommand.ConsumerID, cmd.RegisterConsumerCommand.TopicID,
		uint(cmd.RegisterConsumerCommand.PartitionID))
	if err != nil {
		fsm.logger.Fatalf("Unable to register consumer: %s for topic: %d, partition: %d due to err: %s. "+
			"Log Index: %d, Log Term: %d", cmd.RegisterConsumerCommand.ConsumerID, cmd.RegisterConsumerCommand.TopicID,
			cmd.RegisterConsumerCommand.PartitionID, err.Error())
		return err
	}
	return nil
}

func (fsm *FSM) commit(cmd *Command, log *raft.Log) error {
	if cmd.CommitCommand.TopicID == 0 {
		fsm.logger.Fatalf("No topic name provided when for commit command")
	}
	if cmd.CommitCommand.PartitionID < 0 {
		fsm.logger.Fatalf("Invalid partition ID provided for commit command. Partition ID: %d",
			cmd.CommitCommand.PartitionID)
	}
	cs := fsm.storageController.GetConsumerStore()
	if err := cs.Commit(cmd.CommitCommand.ConsumerID, cmd.CommitCommand.TopicID,
		uint(cmd.CommitCommand.PartitionID), cmd.CommitCommand.Offset); err != nil {
		if err == storage.ErrConsumerNotRegistered {
			glog.Warningf("Subscriber: %s is not registered for partition: %d, topic: %d. "+
				"Skipping this command. Log Index: %d, Log Term: %d",
				cmd.CommitCommand.ConsumerID, cmd.CommitCommand.PartitionID, cmd.CommitCommand.TopicID,
				log.Index, log.Term)
			return nil
		}
		fsm.logger.Fatalf("Unable to add consumer commit for subscriber: %s, partition: %d, topic: %d, "+
			"Log Index: %d, Log Term: %d due to err: %s", cmd.CommitCommand.ConsumerID, cmd.CommitCommand.PartitionID,
			cmd.CommitCommand.TopicID, log.Index, log.Term, err.Error())
		return err
	}
	return nil
}
