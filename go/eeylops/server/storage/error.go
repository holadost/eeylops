package storage

import "fmt"

type StoreErrorCode int64

const (
	KVStoreKeyNotFoundErr StoreErrorCode = 1
	KVStoreErr            StoreErrorCode = 2
	SegmentErr            StoreErrorCode = 3
	PartitionErr          StoreErrorCode = 4
	ConsumerStoreErr      StoreErrorCode = 5
	TopicStoreErr         StoreErrorCode = 6
)

func (sec StoreErrorCode) ToString() string {
	switch sec {
	case KVStoreKeyNotFoundErr:
		return "KVStoreKeyNotFoundErr"
	case KVStoreErr:
		return "KVStoreErr"
	case SegmentErr:
		return "SegmentErr"
	case PartitionErr:
		return "PartitionErr"
	case ConsumerStoreErr:
		return "ConsumerStoreErr"
	case TopicStoreErr:
		return "TopicStoreErr"
	default:
		return "Invalid error code!"
	}
}

type StoreError struct {
	msg string
	ec  StoreErrorCode
}

func NewStorageError(msg string, ec StoreErrorCode) *StoreError {
	se := &StoreError{}
	se.msg = msg
	se.ec = ec
	return se
}

func (se *StoreError) Error() string {
	return fmt.Sprintf("storage error[%s]: %s ", se.ec.ToString(), se.msg)
}
