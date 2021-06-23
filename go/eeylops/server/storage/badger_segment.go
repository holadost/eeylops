package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	badger "github.com/dgraph-io/badger/v3"
	"github.com/golang/glog"
	"os"
	"path"
	"sync"
	"time"
)

// BadgerSegment implements Segment where the data is backed using badger db.
type BadgerSegment struct {
	ddb        *badger.DB         // Segment data db.
	mdb        *segmentMetadataDB // Segment metadata ddb.
	ctx        context.Context
	cancelFunc context.CancelFunc
	nextOffSet uint64           // Next start offset for new appends.
	segLock    sync.RWMutex     // A RW lock for the segment.
	closed     bool             // Flag that indicates whether the segment is closed.
	rootDir    string           // Root directory of this segment.
	metadata   *SegmentMetadata // Cached segment metadata.
	logStr     string           // Log string associated with the segment for easy debugging.
	appendLock sync.Mutex       // A lock for appends allowing only one append at a time.
}

// NewBadgerSegment initializes a new instance of badger segment.
func NewBadgerSegment(rootDir string) (*BadgerSegment, error) {
	if err := os.MkdirAll(path.Join(rootDir, dataDirName), 0774); err != nil {
		return nil, err
	}
	seg := new(BadgerSegment)
	seg.rootDir = rootDir
	seg.initialize()
	return seg, nil
}

// Initialize implements the Segment interface. It initializes the segment.
func (seg *BadgerSegment) initialize() {
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
		glog.Fatalf("Unable to open segment: %s due to err: %s", seg.logStr, err.Error())
	}
	seg.ctx, seg.cancelFunc = context.WithCancel(context.Background())
	seg.closed = false
	seg.initializeNextOffset()
	seg.updateLogStr()
}

// Close implements the Segment interface. It closes the connection to the underlying
// BadgerDB database as well as invoking the context's cancel function.
func (seg *BadgerSegment) Close() error {
	if seg.closed {
		return nil
	}
	glog.Infof("Closing segment: %s", seg.logStr)
	seg.cancelFunc()
	err := seg.ddb.Close()
	seg.mdb.Close()
	seg.ddb = nil
	seg.mdb = nil
	seg.closed = true
	return err
}

// Append implements the Segment interface. This method appends the given values to the segment.
func (seg *BadgerSegment) Append(values [][]byte) error {
	// Acquire a read lock on the segment as we are not changing the segment metadata.
	seg.segLock.RLock()
	defer seg.segLock.RUnlock()

	if seg.closed || seg.metadata.Expired || seg.metadata.Immutable {
		return ErrSegmentClosed
	}

	keys := seg.generateKeys(seg.nextOffSet, uint64(len(values)))
	wb := seg.ddb.NewWriteBatch()
	for ii := 0; ii < len(keys); ii++ {
		if err := wb.Set(keys[ii], values[ii]); err != nil {
			glog.Errorf("Unable to perform batch write to segment due to err: %s", err.Error())
			return ErrGenericSegment
		}
	}
	if err := wb.Flush(); err != nil {
		glog.Errorf("Unable to flush after batch write to segment due to err: %s", err.Error())
		return ErrGenericSegment
	}
	seg.nextOffSet = seg.nextOffSet + uint64(len(values))
	return nil
}

// Scan implements the Segment interface. It attempts to fetch numMessages starting from the given
// startOffset.
func (seg *BadgerSegment) Scan(startOffset uint64, numMessages uint64) (values [][]byte, errs []error) {
	seg.segLock.RLock()
	defer seg.segLock.RUnlock()

	if seg.closed || seg.metadata.Expired {
		glog.Errorf("Segment: %s is already closed", seg.logStr)
		for ii := 0; ii < int(startOffset+numMessages); ii++ {
			errs = append(errs, ErrSegmentClosed)
		}
		return
	}
	// Compute the keys that need to be fetched.
	no := seg.nextOffSet
	if startOffset+numMessages >= no {
		numMessages = no - startOffset
	}
	if no == 0 {
		return values, errs
	}
	keys := seg.generateKeys(startOffset, numMessages)
	// Fetch values from DB.
	// TODO: Test/benchmark using Txn.iterator(itr.Seek(start_key) to get to the key of interest).
	seg.ddb.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				glog.Errorf("Unable to get key: %v from segment: %s due to err: %s",
					key, seg.logStr, err.Error())
				errs = append(errs, ErrGenericSegment)
				values = append(values, nil)
				continue
			}
			tmpValue, err := item.ValueCopy(nil)
			if err != nil {
				glog.Errorf("Unable to parse value for key: %v on segment: %s due to err: %s",
					key, seg.logStr, err.Error())
				errs = append(errs, ErrGenericSegment)
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

// MarkImmutable marks the segment as immutable.
func (seg *BadgerSegment) MarkImmutable() {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.metadata.Immutable = true
	seg.metadata.ImmutableTimestamp = time.Now()
	if seg.nextOffSet != 0 {
		seg.metadata.EndOffset = seg.metadata.StartOffset + seg.nextOffSet - 1
	}
	seg.mdb.PutMetadata(seg.metadata)
}

// MarkExpired marks the segment as immutable.
func (seg *BadgerSegment) MarkExpired() {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.metadata.Expired = true
	seg.metadata.ExpiredTimestamp = time.Now()
	seg.mdb.PutMetadata(seg.metadata)
}

// GetMetadata returns a copy of the metadata associated with the segment.
func (seg *BadgerSegment) GetMetadata() SegmentMetadata {
	seg.segLock.RLock()
	metadata := *seg.metadata
	seg.segLock.RUnlock()

	if !metadata.Immutable {
		if seg.nextOffSet != 0 {
			metadata.EndOffset = metadata.StartOffset + seg.nextOffSet - 1
		} else {
			metadata.EndOffset = metadata.StartOffset
		}
	}
	return metadata
}

// SetMetadata sets the metadata for the segment.
func (seg *BadgerSegment) SetMetadata(sm SegmentMetadata) {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.mdb.PutMetadata(&sm)
	seg.metadata = &sm
	seg.updateLogStr()
}

func (seg *BadgerSegment) Stats() {

}

// updateLogStr updates the logStr of the segment.
func (seg *BadgerSegment) updateLogStr() {
	seg.logStr = fmt.Sprintf("(ID: %d, Root Directory: %s)", seg.metadata.ID, seg.rootDir)
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
	glog.V(1).Infof("Initializing next offset for segment: %s", seg.logStr)
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
