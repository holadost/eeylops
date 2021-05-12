package segment

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/dgraph-io/badger"
	"github.com/golang/glog"
	"os"
	"sync"
)

// BadgerSegment implements Segment where the data is backed using badger db.
type BadgerSegment struct {
	db         *badger.DB
	ctx        context.Context
	cancelFunc context.CancelFunc
	nextOffSet uint64
	writeLock  sync.Mutex
	immutable  bool
	closed     bool
	dataDir    string
}

// NewBadgerSegment initializes a new instance of badger segment store.
func NewBadgerSegment(dataDir string) (*BadgerSegment, error) {
	if err := os.MkdirAll(dataDir, 0774); err != nil {
		return nil, err
	}
	bds := new(BadgerSegment)
	bds.dataDir = dataDir
	err := bds.Initialize()
	if err != nil {
		glog.Errorf("Unable to create badger segment due to err: %s", err.Error())
		return nil, err
	}
	return bds, nil
}

// Initialize implements the Segment interface. It initializes the segment store.
func (bds *BadgerSegment) Initialize() error {
	glog.Infof("Initializing badger segment")
	opts := badger.DefaultOptions(bds.dataDir)
	opts.SyncWrites = true
	opts.NumMemtables = 3
	opts.VerifyValueChecksum = true
	var err error
	if bds.immutable {
		glog.Infof("Segment is marked as immutable. Opening DB in read-only mode")
		opts.ReadOnly = false
	}
	bds.db, err = badger.Open(opts)
	if err != nil {
		bds.db = nil
		return err
	}
	bds.ctx, bds.cancelFunc = context.WithCancel(context.Background())
	bds.closed = false
	bds.initializeNextOffset()
	return nil
}

// Close implements the Segment interface. It closes the connection to the underlying
// BadgerDB database as well as invoking the context's cancel function.
func (bds *BadgerSegment) Close() error {
	if bds.closed {
		return nil
	}
	glog.Infof("Closing segment located at: %s", bds.dataDir)
	bds.cancelFunc()
	err := bds.db.Close()
	bds.db = nil
	bds.closed = true
	return err
}

// Append implements the Segment interface. This method appends the given values to the segment.
func (bds *BadgerSegment) Append(values [][]byte) error {
	if bds.closed {
		return errors.New("segment store is closed")
	}
	if bds.immutable {
		return errors.New("segment is immutable. Append is disabled")
	}
	bds.writeLock.Lock()
	defer bds.writeLock.Unlock()
	keys := bds.generateKeys(bds.nextOffSet, uint64(len(values)))
	err := bds.db.Update(func(txn *badger.Txn) error {
		for ii := 0; ii < len(keys); ii++ {
			err := txn.Set(keys[ii], values[ii])
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		bds.nextOffSet = bds.nextOffSet + uint64(len(values))
	}
	return err
}

// Scan implements the Segment interface. It attempts to fetch numMessages starting from the given
// startOffset.
func (bds *BadgerSegment) Scan(startOffset uint64, numMessages uint64) (values [][]byte, errs []error) {
	if bds.closed {
		err := errors.New("segment is closed")
		for ii := 0; ii < int(startOffset+numMessages); ii++ {
			errs = append(errs, err)
		}
		return
	}
	// Compute the keys that need to be fetched.
	keys := bds.generateKeys(startOffset, numMessages)
	// Fetch values from DB.
	bds.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				errs = append(errs, err)
				values = append(values, nil)
				continue
			}
			tmpValue, err := item.ValueCopy(nil)
			if err != nil {
				errs = append(errs, err)
				values = append(values, nil)
				continue
			}
			errs = append(errs, nil)
			values = append(values, tmpValue)
		}
		return nil
	})
	return
}

// SetImmutable marks the segment as immutable.
func (bds *BadgerSegment) SetImmutable() {
	bds.writeLock.Lock()
	bds.immutable = true
	bds.writeLock.Unlock()
}

// Metadata returns a copy of the metadata associated with the segment.
func (bds *BadgerSegment) Metadata() SegmentMetadata {
	m := SegmentMetadata{
		NextOffset: bds.nextOffSet,
		DataDir:    bds.dataDir,
		Immutable:  bds.immutable,
	}
	return m
}

// generateKeys generates keys based on the given startOffset and numMessages.
func (bds *BadgerSegment) generateKeys(startOffset uint64, numMessages uint64) [][]byte {
	lastOffset := startOffset + numMessages
	var keys [][]byte
	for ii := startOffset; ii < lastOffset; ii++ {
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, ii)
		keys = append(keys, val)
	}
	return keys
}

// initializeOffsets initializes the start and last offset by scanning the underlying DB.
func (bds *BadgerSegment) initializeNextOffset() {
	glog.Infof("Initializing start and end offset")
	txn := bds.db.NewTransaction(true)
	opt := badger.DefaultIteratorOptions
	opt.Reverse = true
	itr := txn.NewIterator(opt)
	var hasVal bool
	for itr.Rewind(); itr.Valid(); itr.Next() {
		item := itr.Item()
		val := item.KeyCopy(nil)
		bds.nextOffSet = binary.BigEndian.Uint64(val) + 1
		hasVal = true
		break
	}
	if !hasVal {
		bds.nextOffSet = 0
	}
	glog.Infof("Next offset: %d", bds.nextOffSet)
	itr.Close()
	txn.Discard()
}
