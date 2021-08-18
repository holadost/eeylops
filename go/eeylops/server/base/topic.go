package base

import (
	"fmt"
	"time"
)

type Topic struct {
	Name         string `json:"name"`
	PartitionIDs []int  `json:"partition_ids"`
	TTLSeconds   int    `json:"ttl_seconds"`
	ToRemove     bool   `json:"to_remove"`
}

func (topic *Topic) ToString() string {
	return fmt.Sprintf("\nTopic Name: %s\nPartition IDs: %v\nTTL Seconds: %d\nTo Remove: %v",
		topic.Name, topic.PartitionIDs, topic.TTLSeconds, topic.ToRemove)
}

type TopicConfig struct {
	Topic
	ID        TopicIDType // Topic ID.
	CreatedAt time.Time   // Time when the topic was created.
	RemovedAt time.Time   // Time when topic was deleted.
}

func (tc *TopicConfig) ToString() string {
	return fmt.Sprintf("\nTopic ID: %d\nTopic Name: %s\nPartition IDs: %v\nTTL Seconds: %d\nTo Remove: %v"+
		"\nCreated At: %v\nRemoved At: %v", tc.ID, tc.Name, tc.PartitionIDs, tc.TTLSeconds, tc.ToRemove, tc.CreatedAt,
		tc.RemovedAt)
}
