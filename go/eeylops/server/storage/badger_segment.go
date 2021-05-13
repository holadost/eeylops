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

// BadgerSegment implements Segment where the data is backed using badger ddb.
type BadgerSegment struct {
	ddb        *badger.DB         // Segment data ddb.
	mdb        *segmentMetadataDB // Segment metadata ddb.
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
func (seg *BadgerSegment) Initialize() error {
	glog.Infof("Initializing badger segment located at: %s", seg.rootDir)
	// Initialize metadata ddb.
	seg.mdb = newSegmentMetadataDB(seg.rootDir)
	seg.metadata = seg.mdb.GetMetadata()
	if seg.metadata.ID == 0 {
		glog.Infof("Did not find any metadata associated with this segment")
	}
	opts := badger.DefaultOptions(path.Join(seg.rootDir, dataDirName))
	opts.SyncWrites = true
	opts.NumMemtables = 3
	opts.VerifyValueChecksum = true
	if seg.metadata.Immutable {
		opts.ReadOnly = true
	}
	var err error
	seg.ddb, err = badger.Open(opts)
	if err != nil {
		glog.Fatalf("Unable to open badger ddb due to err: %s", err.Error())
	}
	seg.ctx, seg.cancelFunc = context.WithCancel(context.Background())
	seg.closed = false
	seg.initializeNextOffset()
	return nil
}

// Close implements the Segment interface. It closes the connection to the underlying
// BadgerDB database as well as invoking the context's cancel function.
func (seg *BadgerSegment) Close() error {
	if seg.closed {
		return nil
	}
	glog.Infof("Closing segment located at: %s", seg.rootDir)
	seg.cancelFunc()
	err := seg.ddb.Close()
	seg.ddb = nil
	seg.closed = true
	return err
}

// Append implements the Segment interface. This method appends the given values to the segment.
func (seg *BadgerSegment) Append(values [][]byte) error {
	if seg.closed {
		return errors.New("segment store is closed")
	}
	seg.writeLock.Lock()
	defer seg.writeLock.Unlock()
	keys := seg.generateKeys(seg.nextOffSet, uint64(len(values)))
	err := seg.ddb.Update(func(txn *badger.Txn) error {
		for ii := 0; ii < len(keys); ii++ {
			err := txn.Set(keys[ii], values[ii])
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		seg.nextOffSet = seg.nextOffSet + uint64(len(values))
	}
	return err
}

// Scan implements the Segment interface. It attempts to fetch numMessages starting from the given
// startOffset.
func (seg *BadgerSegment) Scan(startOffset uint64, numMessages uint64) (values [][]byte, errs []error) {
	if seg.closed {
		err := errors.New("segment is closed")
		for ii := 0; ii < int(startOffset+numMessages); ii++ {
			errs = append(errs, err)
		}
		return
	}
	// Compute the keys that need to be fetched.
	keys := seg.generateKeys(startOffset, numMessages)
	// Fetch values from DB.
	seg.ddb.View(func(txn *badger.Txn) error {
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
func (seg *BadgerSegment) SetImmutable() {
	seg.writeLock.Lock()
	seg.writeLock.Unlock()
}

// Metadata returns a copy of the metadata associated with the segment.
func (seg *BadgerSegment) Metadata() SegmentMetadata {
	return *seg.metadata
}

// generateKeys generates keys based on the given startOffset and numMessages.
func (seg *BadgerSegment) generateKeys(startOffset uint64, numMessages uint64) [][]byte {
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
func (seg *BadgerSegment) initializeNextOffset() {
	glog.V(1).Infof("Initializing next offset")
	txn := seg.ddb.NewTransaction(true)
	opt := badger.DefaultIteratorOptions
	opt.Reverse = true
	itr := txn.NewIterator(opt)
	var hasVal bool
	for itr.Rewind(); itr.Valid(); itr.Next() {
		item := itr.Item()
		val := item.KeyCopy(nil)
		seg.nextOffSet = binary.BigEndian.Uint64(val) + 1
		hasVal = true
		break
	}
	if !hasVal {
		seg.nextOffSet = 0
	}
	itr.Close()
	txn.Discard()
}
