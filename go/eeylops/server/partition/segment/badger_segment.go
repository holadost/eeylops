package segment

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger"
	"os"
	"sync"
)

// BadgerSegment implements Segment where the data is backed using badger db.
type BadgerSegment struct {
	db          *badger.DB
	ctx         context.Context
	cancelFunc  context.CancelFunc
	startOffset uint64
	lastOffset  uint64
	// writeLock protects the Append call allowing only one writer to append messages at a time.
	writeLock sync.Mutex
	immutable bool
	closed    bool
	dataDir   string
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
		return nil, err
	}
	return bds, nil
}

// Initialize implements the Segment interface. It initializes the segment store.
func (bds *BadgerSegment) Initialize() error {
	opts := badger.DefaultOptions(bds.dataDir)
	opts.SyncWrites = true
	if bds.immutable {
		opts.ReadOnly = true
	}
	opts.NumMemtables = 3
	opts.VerifyValueChecksum = true
	var err error
	bds.db, err = badger.Open(opts)
	if err != nil {
		bds.db = nil
		return err
	}
	bds.ctx, bds.cancelFunc = context.WithCancel(context.Background())
	bds.closed = false
	bds.initializeOffsets()
	return nil
}

// Close implements the Segment interface. It closes the connection to the underlying
// BadgerDB database as well as invoking the context's cancel function.
func (bds *BadgerSegment) Close() error {
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
	keys := bds.generateKeys(bds.startOffset, uint64(len(values)))
	err := bds.db.Update(func(txn *badger.Txn) error {
		for ii, key := range keys {
			err := txn.Set(key, values[ii])
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		bds.lastOffset = bds.lastOffset + uint64(len(values)) - 1
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
				return err
			}
			tmpValue, err := item.ValueCopy(nil)
			errs = append(errs, err)
			values = append(values, tmpValue)
		}
		return nil
	})

	return
}

// SetImmutable marks the segment as immutable.
func (bds *BadgerSegment) SetImmutable() {
	bds.immutable = true
}

// Metadata returns a copy of the metadata associated with the segment.
func (bds *BadgerSegment) Metadata() SegmentMetadata {
	m := SegmentMetadata{
		StartOffset: bds.startOffset,
		LastOffset:  bds.lastOffset,
		DataDir:     bds.dataDir,
		Immutable:   bds.immutable,
	}
	return m
}

// generateKeys generates keys based on the given startOffset and numMessages.
func (bds *BadgerSegment) generateKeys(startOffset uint64, numMessages uint64) [][]byte {
	lastOffset := startOffset + numMessages
	if bds.lastOffset < lastOffset {
		lastOffset = bds.lastOffset
	}
	keys := make([][]byte, lastOffset-startOffset)
	for ii := startOffset; ii < lastOffset; ii++ {
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, ii)
		keys = append(keys, val)
	}
	return keys
}

// initializeOffsets initializes the start and last offset by scanning the underlying DB.
func (bds *BadgerSegment) initializeOffsets() {
	txn := bds.db.NewTransaction(false)
	opt := badger.DefaultIteratorOptions
	itr := txn.NewIterator(opt)
	for itr.Rewind(); itr.Valid(); itr.Next() {
		item := itr.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			panic(fmt.Sprintf("Unable to fetch the first offset due to err: %v", err.Error()))
		}
		bds.startOffset = binary.BigEndian.Uint64(val)
		break
	}
	itr.Close()

	itr = txn.NewIterator(opt)
	opt.Reverse = true
	for itr.Rewind(); itr.Valid(); itr.Next() {
		item := itr.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			panic(fmt.Sprintf("Unable to fetch the last offset due to err: %v", err.Error()))
		}
		bds.lastOffset = binary.BigEndian.Uint64(val)
		break
	}
	itr.Close()
}
