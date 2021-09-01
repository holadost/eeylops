package base

import (
	"fmt"
	"time"
)

type TopicConfig struct {
	// Name of the topic.
	Name string `json:"name"`
	// Partitions associated with this topic.
	PartitionIDs []int `json:"partition_ids"`
	// Time to live in seconds. If <= 0, the messages are never deleted.
	TTLSeconds int `json:"ttl_seconds"`
	// Topic ID.
	ID TopicIDType `json:"id"`
	// Time when the topic was created.
	CreatedAt time.Time `json:"created_at"`
	// If the topic creation was confirmed by the various brokers.
	ConfirmedByBrokers []bool `json:"confirmed_by_brokers"`
}

func (tc *TopicConfig) ToString() string {
	return fmt.Sprintf("\nTopic ID: %d\nTopic Name: %s\nPartition IDs: %v\nTTL Seconds: %d"+
		"\nCreated At: %v", tc.ID, tc.Name, tc.PartitionIDs, tc.TTLSeconds, tc.CreatedAt)
}
