package hedwig

import "errors"

var (
	// ErrTopicNotFound is returned when the given topic was not found.
	ErrTopicNotFound = errors.New("ErrTopicNotFound: topic not found")

	// ErrTopicExists is returned when a topic with the given name already exists.
	ErrTopicExists = errors.New("ErrTopicExists: topic already exists")

	// ErrInstanceTopicManager is a generic error that is returned for any unhandled errors.
	ErrInstanceTopicManager = errors.New("ErrInstanceTopicManager: topic manager error")

	// ErrPartitionNotFound is a returned when the given partition is not found by the instance topic manager.
	ErrPartitionNotFound = errors.New("ErrInstanceTopicManager: topic manager error")
)
