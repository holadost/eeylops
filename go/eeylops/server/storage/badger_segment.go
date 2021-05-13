package storage

import (
	"context"
	"encoding/binary"
	"errors"
	badger "github.com/dgraph-io/badger/v3"
	"github.com/golang/glog"
	"os"
	"path"
	"sync"
)

// BadgerSegment implements Segment where the data is backed using badger db.
type BadgerSegment struct {
	db         *badger.DB         // Segment data db.
	mdb        *segmentMetadataDB // Segment metadata db.
	ctx        context.Context
	cancelFunc context.CancelFunc
	nextOffSet uint64     // Next start offset for new appends.
	writeLock  sync.Mutex // Lock to protect all writes to segment data.
	closed     bool
	rootDir    string
	metadata   *SegmentMetadata // Cached segment metadata.
}

// NewBadgerSegment initializes a new instance of badger segment store.
func NewBadgerSegment(rootDir string) (*BadgerSegment, error) {
	if err := os.MkdirAll(path.Join(rootDir, dataDirName), 0774); err != nil {
		return nil, err
	}
	bds := new(BadgerSegment)
	bds.rootDir = rootDir
	err := bds.Initialize()
	if err != nil {
		glog.Errorf("Unable to create badger segment due to err: %s", err.Error())
		return nil, err
	}
	return bds, nil
}

// Initialize implements the Segment interface. It initializes the segment store.
func (bds *BadgerSegment) Initialize() error {
	glog.Infof("Initializing badger segment located at: %s", bds.rootDir)
	// Initialize metadata db.
	bds.mdb = newSegmentMetadataDB(bds.rootDir)
	bds.metadata = bds.mdb.GetMetadata()
	if bds.metadata.ID == 0 {
		glog.Infof("Did not find any metadata associated with this segment")
	}
	opts := badger.DefaultOptions(path.Join(bds.rootDir, dataDirName))
	opts.SyncWrites = true
	opts.NumMemtables = 3
	opts.VerifyValueChecksum = true
	if bds.metadata.Immutable {
		opts.ReadOnly = true
	}
	var err error
	bds.db, err = badger.Open(opts)
	if err != nil {
		glog.Fatalf("Unable to open badger db due to err: %s", err.Error())
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
	glog.Infof("Closing segment located at: %s", bds.rootDir)
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
	bds.writeLock.Unlock()
}

// Metadata returns a copy of the metadata associated with the segment.
func (bds *BadgerSegment) Metadata() SegmentMetadata {
	return *bds.metadata
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
	glog.V(1).Infof("Initializing next offset")
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
	itr.Close()
	txn.Discard()
}
