package segment

import (
	"context"
	"encoding/binary"
	"github.com/dgraph-io/badger"
	"os"
)

// BadgerSegment implements Segment where the data is backed using badger db.
type BadgerSegment struct {
	db          *badger.DB
	ctx         context.Context
	cancelFunc  context.CancelFunc
	startOffset uint64
	lastOffset  uint64
	immutable   bool
	closed      bool
	dataDir     string
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
	return err
}

// Scan implements the Segment interface. It attempts to fetch numMessages starting from the given
// startOffset.
func (bds *BadgerSegment) Scan(startOffset uint64, numMessages uint64) (values [][]byte, errs []error) {
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

func (bds *BadgerSegment) SetImmutable() {
	bds.immutable = true
}

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
