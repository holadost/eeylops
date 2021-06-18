package base

import "fmt"

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
