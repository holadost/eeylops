package storage

import "fmt"

type StorageError struct {
	msg       string
	wrappedSe *StorageError
}

func NewStorageError(msg string) *StorageError {
	se := &StorageError{}
	se.msg = msg
	se.wrappedSe = nil
	return se
}

func (se *StorageError) Error() string {
	return se.msg
}

func (se *StorageError) Trace() string {
	return getTrace(se)
}

func (se *StorageError) Wrap(nse *StorageError) {
	se.wrappedSe = nse
}

func getTrace(se *StorageError) string {
	msg := ""
	if se.msg != "" {
		msg += fmt.Sprintf("\n%s", se.msg)
	}
	if se.wrappedSe == nil {
		return msg
	}
	msg += getTrace(se.wrappedSe)
	return msg
}
