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
	case KNoOpCommand:
		// Do nothing.
		break
	case KAppendCommand:
		if len(cmd.AppendCommand.Data) == 0 {
			fsm.logger.Fatalf("Received an append command with nothing to append")
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
			fsm.logger.Errorf("Unable to append entries due to err: %s", ret.Error.Error())
			return ret.Error
		}
		break
	case KCommitCommand:
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
			fsm.logger.Errorf("Unable to add consumer commit due to err: %s", err.Error())
			return err
		}
		break
	case KAddTopicCommand:
		topic := &cmd.AddTopicCommand.TopicConfig
		if topic.ID <= 0 {
			fsm.logger.Fatalf("Invalid topic ID")
		}
		if len(topic.Name) == 0 {
			fsm.logger.Fatalf("Invalid topic name")
		}
		if len(topic.PartitionIDs) == 0 {
			fsm.logger.Fatalf("Invalid partition IDs provided")
		}
		if topic.ToRemove {
			fsm.logger.Fatalf("Cannot add a topic that has already been marked as to remove")
		}
		if err := fsm.storageController.AddTopic(cmd.AddTopicCommand.TopicConfig); err != nil {
			fsm.logger.Errorf("Unable to add topic due to err: %s", err.Error())
			return err
		}
		break
	case KRemoveTopicCommand:
		if cmd.RemoveTopicCommand.TopicID == 0 {
			fsm.logger.Fatalf("Invalid topic ID")
		}
		if err := fsm.storageController.RemoveTopic(cmd.RemoveTopicCommand.TopicID); err != nil {
			fsm.logger.Errorf("Unable to remove topic due to err: %s", err.Error())
			return err
		}
		break
	case KRegisterConsumerCommand:
		if len(cmd.RegisterConsumerCommand.ConsumerID) == 0 {
			fsm.logger.Fatalf("Invalid consumer ID")
		}
		if cmd.RegisterConsumerCommand.TopicID <= 0 {
			fsm.logger.Fatalf("Invalid topic ID")
		}
		if cmd.RegisterConsumerCommand.PartitionID <= 0 {
			fsm.logger.Fatalf("Invalid partition ID")
		}
		cs := fsm.storageController.GetConsumerStore()
		err := cs.RegisterConsumer(cmd.RegisterConsumerCommand.ConsumerID, cmd.RegisterConsumerCommand.TopicID,
			uint(cmd.RegisterConsumerCommand.PartitionID))
		if err != nil {
			fsm.logger.Fatalf("Hit an unexpected error while attempting to register consumer: %s", err.Error())
		}

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
