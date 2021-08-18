package hedwig

import (
	"bytes"
	"eeylops/server/base"
	"encoding/gob"
	"github.com/golang/glog"
)

const (
	KNoOpCommand             = 1
	KAppendCommand           = 2
	KCommitCommand           = 3
	KAddTopicCommand         = 4
	KRemoveTopicCommand      = 5
	KRegisterConsumerCommand = 6
)

type Command struct {
	// Command type as defined above.
	CommandType int
	// Append command must be populated if CommandType is Append.
	AppendCommand AppendMessage
	// Commit command must be populated if CommandType is Commit.
	CommitCommand CommitMessage
	// Add topic message must be populated if CommandType is add topic.
	AddTopicCommand AddTopicMessage
	// Remove topic message must be populated if CommandType is remove topic.
	RemoveTopicCommand RemoveTopicMessage
	// Register consumer message must be populated if CommandType is register consumer.
	RegisterConsumerCommand RegisterConsumerMessage
}

type AppendMessage struct {
	TopicID     base.TopicIDType // Topic ID.
	PartitionID int              // Partition ID where the command is applied
	Data        [][]byte         // Data to be appended to the partition.
	Timestamp   int64            // Epoch/timestamp when messages were appended.
}

type CommitMessage struct {
	TopicID     base.TopicIDType // Topic ID.
	PartitionID int              // Partition ID.
	Offset      base.Offset      // Offset number.
	ConsumerID  string           // Consumer ID.
}

type AddTopicMessage struct {
	TopicConfig base.TopicConfig // Topic
}

type RemoveTopicMessage struct {
	TopicID base.TopicIDType // Topic ID.
}

type RegisterConsumerMessage struct {
	ConsumerID  string           // Consumer ID.
	TopicID     base.TopicIDType // Topic ID.
	PartitionID int              // Partition ID.
}

func Serialize(cmd *Command) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cmd)
	if err != nil {
		glog.Fatalf("Unable to serialize command due to err: %s", err.Error())
	}
	return buf.Bytes()
}

func Deserialize(data []byte) *Command {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var cmd Command
	if err := dec.Decode(&cmd); err != nil {
		glog.Fatalf("Unable to deserialize command due to err: %s", err.Error())
	}
	return &cmd
}
