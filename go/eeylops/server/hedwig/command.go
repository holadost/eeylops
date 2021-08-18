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
	CommandType        int                // Command type as defined above.
	AppendCommand      AppendMessage      // Append command must be populated if CommandType is Append.
	CommitCommand      CommitMessage      // Commit command must be populated if CommandType is Commit.
	AddTopicCommand    AddTopicMessage    // Add topic message must be populated if CommandType is add topic.
	RemoveTopicCommand RemoveTopicMessage // Remove topic message must be populated if CommandType is remove topic.
}

type AppendMessage struct {
	TopicName   string   // Name of the topic where the command will be applied.
	PartitionID int      // Partition ID where the command is applied
	Data        [][]byte // Data to be appended to the partition.
	Timestamp   int64    // Epoch/timestamp when messages were appended.
}

type CommitMessage struct {
	TopicName   string      // Name of the topic.
	PartitionID int         // Partition ID.
	Offset      base.Offset // Offset number.
	ConsumerID  string      // Consumer ID.
}

type AddTopicMessage struct {
	TopicConfig base.TopicConfig // Topic
}

type RemoveTopicMessage struct {
	TopicName string // Name of the topic.
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
