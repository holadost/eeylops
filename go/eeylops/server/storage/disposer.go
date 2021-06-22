package storage

import (
	"github.com/golang/glog"
	"os"
	"sync"
)

var defaultLock sync.Once
var defaultDisposer *StorageDisposer

type disposeEntry struct {
	dirPath string
	cb      func(error)
}

type StorageDisposer struct {
	numDisposers int
	disposeChan  chan *disposeEntry
}

func NewDisposer(numDisposers int) *StorageDisposer {
	if numDisposers <= 0 {
		glog.Fatalf("numDisposers must be > 0")
	}
	disposer := new(StorageDisposer)
	disposer.numDisposers = numDisposers
	disposer.disposeChan = make(chan *disposeEntry, 200)
	disposer.initialize()
	return disposer
}

func DefaultDisposer() *StorageDisposer {
	defaultLock.Do(func() {
		defaultDisposer = NewDisposer(2)
	})
	return defaultDisposer
}

func (ds *StorageDisposer) initialize() {
	for ii := 0; ii < ds.numDisposers; ii++ {
		go ds.dispose()
	}
}

func (ds *StorageDisposer) Dispose(dirPath string, cb func(error)) {
	entry := &disposeEntry{
		dirPath: dirPath,
		cb:      cb,
	}
	ds.disposeChan <- entry
}

func (ds *StorageDisposer) dispose() {
	for {
		e := <-ds.disposeChan
		glog.Infof("Deleting directory: %s", e.dirPath)
		err := os.RemoveAll(e.dirPath)
		if err != nil {
			glog.Errorf("Unable to delete directory: %s due to err: %s", e.dirPath, err.Error())
		}
		e.cb(err)
	}
}
