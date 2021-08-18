package base

import (
	"fmt"
	"time"
)

type Topic struct {
	Name         string `json:"name"`
	PartitionIDs []int  `json:"partition_ids"`
	TTLSeconds   int    `json:"ttl_seconds"`
}

func (topic *Topic) ToString() string {
	return fmt.Sprintf("\nTopic Name: %s\nPartition IDs: %v\nTTL Seconds: %d\nTo Remove: %v",
		topic.Name, topic.PartitionIDs, topic.TTLSeconds)
}

type TopicConfig struct {
	Topic
	ID        TopicIDType // Topic ID.
	CreatedAt time.Time   // Time when the topic was created.
}

func (tc *TopicConfig) ToString() string {
	return fmt.Sprintf("\nTopic ID: %d\nTopic Name: %s\nPartition IDs: %v\nTTL Seconds: %d"+
		"\nCreated At: %v", tc.ID, tc.Name, tc.PartitionIDs, tc.TTLSeconds, tc.CreatedAt)
}
