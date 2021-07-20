package segments

import (
	"context"
	"eeylops/server/base"
	sbase "eeylops/server/storage/base"
	"eeylops/server/storage/kv_store"
	"eeylops/util"
	"eeylops/util/logging"
	"fmt"
	badger "github.com/dgraph-io/badger/v3"
	"os"
	"path"
	"sync"
	"time"
)

const kLastRLogIdxKey = "last_rlog_idx"

// BadgerSegment implements Segment where the data is backed using badger db.
type BadgerSegment struct {
	ddb         *badger.DB         // Segment data db.
	dataDB      kv_store.KVStore   // Backing KV store to hold the data.
	metadataDB  *SegmentMetadataDB // Segment metadata ddb.
	nextOffSet  base.Offset        // Next start offset for new appends.
	segLock     sync.RWMutex       // A RW lock for the segment.
	closed      bool               // Flag that indicates whether the segment is closed.
	rootDir     string             // Root directory of this segment.
	metadata    *SegmentMetadata   // Cached segment metadata.
	appendLock  sync.Mutex         // A lock for appends allowing only one append at a time.
	lastRLogIdx int64              // Last replicated log index.
	openedOnce  bool               // A flag to indicate if the segment was opened once. A segment cannot be
	// closed and reopened.
	logger        *logging.PrefixLogger // Logger object.
	topicName     string                // Topic name.
	partitionID   uint                  // Partition ID.
	scanSizeBytes int                   // Maximum scan size in bytes.
}

type BadgerSegmentOpts struct {
	RootDir       string                // Root directory for the segment. This is a compulsory parameter.
	ParentLogger  *logging.PrefixLogger // Parent logger if any. Optional parameter.
	Topic         string                // Topic name. Optional parameter.
	PartitionID   uint                  // Partition ID. Optional parameter.
	ScanSizeBytes int                   // Max scan size(in bytes). Optional parameter.
}

// NewBadgerSegment initializes a new instance of badger segment.
func NewBadgerSegment(opts *BadgerSegmentOpts) (*BadgerSegment, error) {
	if err := os.MkdirAll(path.Join(opts.RootDir, dataDirName), 0774); err != nil {
		return nil, err
	}
	seg := new(BadgerSegment)
	seg.rootDir = opts.RootDir
	seg.logger = logging.NewPrefixLoggerWithParent(fmt.Sprintf("segment:%d", -1), opts.ParentLogger)
	seg.initialize()
	seg.logger = logging.NewPrefixLoggerWithParent(fmt.Sprintf("segment:%d", -1), opts.ParentLogger)
	seg.topicName = opts.Topic
	seg.partitionID = opts.PartitionID
	seg.scanSizeBytes = opts.ScanSizeBytes
	return seg, nil
}

// Initialize implements the Segment interface. It initializes the segment.
func (seg *BadgerSegment) initialize() {
	seg.logger.Infof("Initializing badger segment located at: %s", seg.rootDir)
	// Initialize metadata ddb.
	seg.metadataDB = NewSegmentMetadataDB(seg.rootDir)
	seg.metadata = seg.metadataDB.GetMetadata()
	if seg.metadata.ID == 0 {
		seg.logger.Infof("Did not find any metadata associated with this segment. This must be a new segment!")
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
		seg.logger.Fatalf("Unable to open segment due to err: %s", err.Error())
	}
	seg.dataDB = kv_store.NewBadgerKVStore(path.Join(seg.rootDir, dataDirName), opts)
}

func (seg *BadgerSegment) Open() {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	if seg.openedOnce {
		seg.logger.Fatalf("The segment cannot be reopened again")
	}
	seg.openedOnce = true
	seg.closed = false
	seg.initializeNextOffset()
}

func (seg *BadgerSegment) ID() int {
	return int(seg.metadata.ID)
}

// Close implements the Segment interface. It closes the connection to the underlying
// BadgerDB database as well as invoking the context's cancel function.
func (seg *BadgerSegment) Close() error {
	if seg.closed {
		return nil
	}
	seg.logger.Infof("Closing segment")
	err := seg.ddb.Close()
	seg.metadataDB.Close()
	seg.ddb = nil
	err = seg.dataDB.Close()
	seg.metadataDB = nil
	seg.closed = true
	return err
}

// IsEmpty implements the Segment interface. Returns true if the segment is empty. False otherwise.
func (seg *BadgerSegment) IsEmpty() bool {
	return seg.nextOffSet == 0
}

func (seg *BadgerSegment) Append(ctx context.Context, arg *sbase.AppendEntriesArg) *sbase.AppendEntriesRet {
	seg.segLock.RLock()
	defer seg.segLock.RUnlock()
	var ret sbase.AppendEntriesRet
	ret.Error = nil
	if seg.closed || seg.metadata.Expired || seg.metadata.Immutable {
		ret.Error = ErrSegmentClosed
		return &ret
	}
	keys := seg.generateKeys(seg.nextOffSet, base.Offset(len(arg.Entries)))
	values := makeMessageValues(arg.Entries, arg.Timestamp)
	if arg.RLogIdx >= 0 {
		keys = append(keys, []byte(kLastRLogIdxKey))
		values = append(values, util.UintToBytes(uint64(arg.RLogIdx)))
	}
	err := seg.dataDB.BatchPut(keys, values)
	if err != nil {
		seg.logger.Errorf("Unable to append entries in segment: %d due to err: %s", seg.ID(), err.Error())
		ret.Error = ErrSegmentBackend
	}
	return &ret
}

// Scan implements the Segment interface. It attempts to fetch numMessages starting from the given
// startOffset.
func (seg *BadgerSegment) Scan(ctx context.Context, arg *sbase.ScanEntriesArg) *sbase.ScanEntriesRet {
	seg.segLock.RLock()
	defer seg.segLock.RUnlock()
	var ret sbase.ScanEntriesRet
	if seg.closed || seg.metadata.Expired {
		seg.logger.Errorf("Segment is already closed")
		ret.Error = ErrSegmentClosed
		return &ret
	}
	if arg.StartOffset == -1 && arg.StartTimestamp == -1 {
		seg.logger.Fatalf("Both StartOffset and StartTimestamp cannot be undefined")
	}

	// Sanity checks to see if the segment contains our start offset.
	if arg.StartOffset < seg.metadata.StartOffset {
		// The start offset does not belong to this segment.
		ret.Error = ErrSegmentInvalid
		return &ret
	}

	if (seg.nextOffSet == seg.metadata.StartOffset+1) || (arg.StartOffset >= seg.nextOffSet) {
		// Either the segment is empty or it does not contain our desired offset yet.
		ret.Error = nil
		ret.Values = nil
		ret.NextOffset = -1
		return &ret
	}

	// TODO: If start timestamp and end timestamp are given, use the segment index to find the closest element to
	// TODO: our timestamp and then use that as the start offset.
	tmpNumMsgs := base.Offset(arg.NumMessages)
	// Compute the keys that need to be fetched.
	if arg.StartOffset+tmpNumMsgs >= seg.nextOffSet {
		tmpNumMsgs = seg.nextOffSet - arg.StartOffset
	}
	endOffset := arg.StartOffset + tmpNumMsgs - 1
	sk := seg.offsetToKey(arg.StartOffset)

	scanner := seg.dataDB.CreateScanner([]byte(""), sk)
	defer scanner.Close()
	bytesScannedSoFar := 0
	for ; scanner.Valid(); scanner.Next() {
		key, msg, err := scanner.GetItem()
		if err != nil {
			seg.logger.Errorf("Failed to scan offset due to scan backend err: %s", err.Error())
			ret.Error = ErrSegmentBackend
			ret.Values = nil
			ret.NextOffset = -1
			return &ret
		}
		offset := seg.keyToOffset(key)
		val, ts := fetchValueFromMessage(msg)
		if arg.StartTimestamp > 0 {
			if ts < arg.StartTimestamp {
				//
				continue
			}
		}
		if arg.EndTimestamp > 0 {
			if ts >= arg.EndTimestamp {
				// Stop scanning as entries beyond this timestamp are not required.
				break
			}
		}
		if seg.scanSizeBytes > 0 {
			bytesScannedSoFar += len(val)
			if bytesScannedSoFar > seg.scanSizeBytes {
				break
			}
		}
		ret.Values = append(ret.Values, &sbase.ScanValue{
			Offset:    offset,
			Value:     val,
			Timestamp: ts,
		})
		if offset == endOffset {
			break
		}
	}
	return &ret
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
	seg.metadataDB.PutMetadata(seg.metadata)
}

// MarkExpired marks the segment as immutable.
func (seg *BadgerSegment) MarkExpired() {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.metadata.Expired = true
	seg.metadata.ExpiredTimestamp = time.Now()
	seg.metadataDB.PutMetadata(seg.metadata)
}

// GetMetadata returns a copy of the metadata associated with the segment.
func (seg *BadgerSegment) GetMetadata() SegmentMetadata {
	seg.segLock.RLock()
	metadata := *seg.metadata
	seg.segLock.RUnlock()

	if !metadata.Immutable {
		if seg.nextOffSet != 0 {
			metadata.EndOffset = seg.nextOffSet - 1
		} else {
			metadata.EndOffset = metadata.StartOffset
		}
	}
	return metadata
}

// GetRange returns the range of the segment.
func (seg *BadgerSegment) GetRange() (sOff base.Offset, eOff base.Offset) {
	seg.segLock.RLock()
	defer seg.segLock.RUnlock()

	sOff = seg.metadata.StartOffset
	eOff = seg.metadata.EndOffset
	if !seg.metadata.Immutable {
		if seg.nextOffSet == sOff {
			eOff = -1
		} else {
			eOff = seg.nextOffSet - 1
		}
	}
	return
}

// SetMetadata sets the metadata for the segment.
func (seg *BadgerSegment) SetMetadata(sm SegmentMetadata) {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.metadataDB.PutMetadata(&sm)
	seg.metadata = &sm
}

func (seg *BadgerSegment) Stats() {

}

// generateKeys generates keys based on the given startOffset and numMessages.
func (seg *BadgerSegment) generateKeys(startOffset base.Offset, numMessages base.Offset) [][]byte {
	lastOffset := startOffset + numMessages
	var keys [][]byte
	for ii := startOffset; ii < lastOffset; ii++ {
		keys = append(keys, seg.offsetToKey(ii))
	}
	return keys
}

func (seg *BadgerSegment) offsetToKey(offset base.Offset) []byte {
	return util.UintToBytes(uint64(offset))
}

func (seg *BadgerSegment) keyToOffset(val []byte) base.Offset {
	return base.Offset(util.BytesToUint(val))
}

// initializeOffsets initializes the start and last offset by scanning the underlying DB.
func (seg *BadgerSegment) initializeNextOffset() {
	seg.logger.VInfof(0, "Initializing next offset for segment")
	txn := seg.ddb.NewTransaction(true)
	opt := badger.DefaultIteratorOptions
	opt.Reverse = true
	itr := txn.NewIterator(opt)
	var hasVal bool
	for itr.Rewind(); itr.Valid(); itr.Next() {
		item := itr.Item()
		val := item.KeyCopy(nil)
		seg.nextOffSet = seg.GetMetadata().StartOffset + base.Offset(util.BytesToUint(val)) + 1
		hasVal = true
		break
	}
	if !hasVal {
		seg.nextOffSet = seg.GetMetadata().StartOffset
	}
	itr.Close()
	txn.Discard()
}
