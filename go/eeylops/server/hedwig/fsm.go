package hedwig

import (
	"context"
	"eeylops/server/storage"
	sbase "eeylops/server/storage/base"
	"eeylops/util/logging"
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
	case KAppendCommand:
		if len(cmd.AppendCommand.Data) == 0 {
			fsm.logger.Fatalf("Received an append command with nothing to append")
		}
		prt, err := fsm.storageController.GetPartition(cmd.AppendCommand.TopicName, cmd.AppendCommand.PartitionID)
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
			fsm.logger.Errorf("Unable to append entries due to err: %s", ret.Error.Error())
			return ret.Error
		}
		break
	case KCommitCommand:
		if len(cmd.CommitCommand.TopicName) == 0 {
			fsm.logger.Fatalf("No topic name provided when for commit command")
		}
		if cmd.CommitCommand.PartitionID < 0 {
			fsm.logger.Fatalf("Invalid partition ID provided for commit command. Partition ID: %d",
				cmd.CommitCommand.PartitionID)
		}
		cs := fsm.storageController.GetConsumerStore()
		if err := cs.Commit(cmd.CommitCommand.ConsumerID, cmd.CommitCommand.TopicName,
			uint(cmd.CommitCommand.PartitionID), cmd.CommitCommand.Offset); err != nil {
			fsm.logger.Errorf("Unable to add consumer commit due to err: %s", err.Error())
			return err
		}
		break
	case KAddTopicCommand:
		topic := &cmd.AddTopicCommand.Topic
		if len(topic.Name) == 0 {
			fsm.logger.Fatalf("Invalid topic name")
		}
		if len(topic.PartitionIDs) == 0 {
			fsm.logger.Fatalf("Invalid partition IDs provided")
		}
		if topic.ToRemove {
			fsm.logger.Fatalf("Cannot add a topic that has already been marked as to remove")
		}
		if err := fsm.storageController.AddTopic(cmd.AddTopicCommand.Topic); err != nil {
			fsm.logger.Errorf("Unable to add topic due to err: %s", err.Error())
			return err
		}
		break
	case KRemoveTopicCommand:
		if len(cmd.RemoveTopicCommand.TopicName) == 0 {
			fsm.logger.Fatalf("Invalid topic name")
		}
		if err := fsm.storageController.RemoveTopic(cmd.RemoveTopicCommand.TopicName); err != nil {
			fsm.logger.Errorf("Unable to remove topic due to err: %s", err.Error())
			return err
		}
		break
	case KNoOpCommand:
		fsm.logger.Infof("Received no op command. Log index: %d, Term: %d. Doing nothing")
		break
	default:
		fsm.logger.Fatalf("Invalid command type: %d", cmd.CommandType)
	}
	return nil
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (fsm *FSM) Restore(closer io.ReadCloser) error {
	return nil
}
