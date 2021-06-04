package storage

type StorageError struct {
	msg string
}

func NewStorageError(msg string) *StorageError {
	se := &StorageError{}
	se.msg = msg
	return se
}

func (se *StorageError) Error() string {
	return se.msg
}

func (se *StorageError) Trace() string {
	return ""
}
