package storage

import (
	"errors"
)

var (

	// ErrPartitionAppend is returned when there was an error while appending entries to the partition.
	ErrPartitionAppend = errors.New("ErrPartitionAppend: partition append error")

	// ErrPartitionScan is returned if there was an error while scanning entries from the partition.
	ErrPartitionScan = errors.New("ErrPartitionScan: partition scan error")

	// ErrPartitionClosed is returned if the partition was accessed after it was closed. A partition is closed
	// iff the topic was removed.
	ErrPartitionClosed = errors.New("ErrPartitionClosed: partition closed")

	// ErrConsumerStore is returned when we have unexpected consumer store error.
	// ErrConsumerStore = errors.New("ErrConsumerStore: consumer store error")

	// ErrConsumerStoreCommit is returned when we have an error while committing an offset for a consumer.
	ErrConsumerStoreCommit = errors.New("ErrConsumerStoreCommit: consumer store commit error")

	// ErrConsumerStoreFetch is returned when we have an error while fetching the last committed offset for a consumer.
	ErrConsumerStoreFetch = errors.New("ErrConsumerStoreFetch: consumer store fetch error")

	// ErrTopicStore is returned when we have unexpected topic store error.
	ErrTopicStore = errors.New("ErrTopicStore: topic store error")

	// ErrTopicNotFound is returned when the given topic is not found.
	ErrTopicNotFound = errors.New("ErrTopicNotFound: topic not found")
)
