package segments

import (
	"bytes"
	"context"
	"eeylops/server/base"
	sbase "eeylops/server/storage/base"
	"eeylops/server/storage/kv_store"
	"eeylops/util"
	"eeylops/util/logging"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"os"
	"path"
	"sync"
	"time"
)

var (
	kLastRLogIdxKeyBytes = []byte(kLastRLogIdxKey)
)

// BadgerSegment implements Segment where the data is backed using badger db.
type BadgerSegment struct {
	dataDB        kv_store.KVStore      // Backing KV store to hold the data.
	metadataDB    *SegmentMetadataDB    // Segment metadata ddb.
	nextOffSet    base.Offset           // Next start offset for new appends.
	segLock       sync.RWMutex          // A RW lock for the segment.
	closed        bool                  // Flag that indicates whether the segment is closed.
	rootDir       string                // Root directory of this segment.
	metadata      *SegmentMetadata      // Cached segment metadata.
	appendLock    sync.Mutex            // A lock for appends allowing only one append at a time.
	lastRLogIdx   int64                 // Last replicated log index.
	firstMsgTs    int64                 // First message timestamp.
	lastMsgTs     int64                 // Last message timestamp.
	openedOnce    bool                  // A flag to indicate if the segment was opened once. A segment cannot be closed and reopened.
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
	// Set segment ID as root directory for now since we still haven't initialized the segment metadata.
	seg.logger = logging.NewPrefixLoggerWithParent(fmt.Sprintf("segment:%s", opts.RootDir), opts.ParentLogger)
	seg.topicName = opts.Topic
	seg.partitionID = opts.PartitionID
	seg.scanSizeBytes = opts.ScanSizeBytes
	seg.initialize()
	// Reinitialize logger with correct segment id.
	seg.logger = logging.NewPrefixLoggerWithParent(fmt.Sprintf("segment:%d", seg.ID()), opts.ParentLogger)
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
	seg.open()
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
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.logger.Infof("Closing segment")
	seg.metadataDB.Close()
	err := seg.dataDB.Close()
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
		seg.logger.Errorf("Segment is either closed, expired or immutable. Cannot append entries")
		ret.Error = ErrSegmentClosed
		return &ret
	}
	if arg.Timestamp < seg.lastMsgTs {
		ret.Error = ErrSegmentInvalidTimestamp
		return &ret
	}
	oldNextOffset := seg.nextOffSet
	keys := seg.generateKeys(seg.nextOffSet, base.Offset(len(arg.Entries)))
	values := makeMessageValues(arg.Entries, arg.Timestamp)
	if arg.RLogIdx <= seg.lastRLogIdx {
		seg.logger.Errorf("Invalid replicated log index: %d. Expected value greater than: %d",
			arg.RLogIdx, seg.lastRLogIdx)
		ret.Error = ErrSegmentInvalidRLogIdx
		return &ret
	}
	keys = append(keys, kLastRLogIdxKeyBytes)
	values = append(values, util.UintToBytes(uint64(arg.RLogIdx)))

	err := seg.dataDB.BatchPut(keys, values)
	if err != nil {
		seg.logger.Errorf("Unable to append entries in segment: %d due to err: %s", seg.ID(), err.Error())
		ret.Error = ErrSegmentBackend
		return &ret
	}
	if oldNextOffset == seg.metadata.StartOffset {
		// These were the first messages appended to the segment.
		seg.firstMsgTs = arg.Timestamp
	}
	seg.lastRLogIdx = arg.RLogIdx
	seg.lastMsgTs = arg.Timestamp
	return &ret
}

// Scan implements the Segment interface. It attempts to fetch numMessages starting from the given StartOffset or
// StartTimestamp.
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
	if (arg.StartOffset >= 0) && (arg.StartOffset < seg.metadata.StartOffset) {
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
	var tmpNumMsgs base.Offset
	var endOffset base.Offset
	var sk []byte
	tmpNumMsgs = base.Offset(arg.NumMessages)

	// Compute the keys that need to be fetched. Start offset has been provided.
	if arg.StartOffset >= 0 {
		if arg.StartOffset+tmpNumMsgs >= seg.nextOffSet {
			tmpNumMsgs = seg.nextOffSet - arg.StartOffset
		}
		endOffset = arg.StartOffset + tmpNumMsgs - 1
		sk = seg.offsetToKey(arg.StartOffset)
	} else if arg.StartTimestamp > 0 {
		// TODO: Use index db and scan the store to find the sk. Set the endOffset accordingly.
	}

	scanner := seg.dataDB.CreateScanner(nil, sk, false)
	defer scanner.Close()
	bytesScannedSoFar := 0
	for ; scanner.Valid(); scanner.Next() {
		key, msg, err := scanner.GetItem()
		if err != nil {
			seg.logger.Errorf("Failed to scan offset due to scan backend err: %s", err.Error())
			ret.Error = ErrSegmentBackend
			ret.Values = nil
			ret.NextOffset = -1
			break
		}
		offset := seg.keyToOffset(key)
		val, ts := fetchValueFromMessage(msg)
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
		seg.metadata.FirstMsgTimestamp = time.Unix(0, seg.firstMsgTs)
		seg.metadata.LastMsgTimestamp = time.Unix(0, seg.lastMsgTs)
	}
	seg.metadataDB.PutMetadata(seg.metadata)
}

// MarkExpired marks the segment as expired.
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
		if seg.nextOffSet == metadata.StartOffset {
			metadata.EndOffset = -1
		} else {
			metadata.EndOffset = seg.nextOffSet - 1
		}
		metadata.FirstMsgTimestamp = time.Unix(0, seg.firstMsgTs)
		metadata.LastMsgTimestamp = time.Unix(0, seg.lastMsgTs)
	}
	return metadata
}

// GetRange returns the range(start and end offset) of the segment.
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

// GetMsgTimestampRange returns the first and last message unix timestamps(in nanoseconds).
func (seg *BadgerSegment) GetMsgTimestampRange() (fMsgTs int64, lMsgTs int64) {
	seg.segLock.RLock()
	defer seg.segLock.RUnlock()
	if seg.nextOffSet == seg.metadata.StartOffset {
		// Segment is empty.
		return
	}
	fMsgTs = seg.firstMsgTs
	lMsgTs = seg.lastMsgTs
	return
}

// SetMetadata sets the metadata for the segment.
func (seg *BadgerSegment) SetMetadata(sm SegmentMetadata) {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.metadataDB.PutMetadata(&sm)
	seg.metadata = &sm
}

// Stats returns the stats of the segment.
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

// Converts the given offset to a key representation for dataDB.
func (seg *BadgerSegment) offsetToKey(offset base.Offset) []byte {
	return util.UintToBytes(uint64(offset))
}

// Converts the key representation of the offset(from dataDB) back to base.Offset type.
func (seg *BadgerSegment) keyToOffset(key []byte) base.Offset {
	return base.Offset(util.BytesToUint(key))
}

// Opens the segment.
func (seg *BadgerSegment) open() {
	// Gather the last replicated log index in the segment.
	val, err := seg.dataDB.Get(kLastRLogIdxKeyBytes)
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			// Segment is empty.
			seg.nextOffSet = seg.GetMetadata().StartOffset
			seg.firstMsgTs = -1
			seg.lastMsgTs = -1
			seg.lastRLogIdx = -1
			return
		}
	} else {
		seg.lastRLogIdx = int64(util.BytesToUint(val))
	}

	// Gather first and last message timestamps by scanning in both forward and reverse directions. Also gather the
	// last offset appended and hence the nextOffset in the segment.
	dirs := []bool{false, true}
	for _, dir := range dirs {
		itr := seg.dataDB.CreateScanner(nil, nil, true)
		for ; itr.Valid(); itr.Next() {
			key, item, err := itr.GetItem()
			if err != nil {
				seg.logger.Fatalf("Unable to initialize next offset, first and last message timestamps due to "+
					"scan err: %s", err.Error())
			}
			if bytes.Compare(key, kLastRLogIdxKeyBytes) == 0 {
				// Skip this key.
				continue
			}
			// Set next offset and last appended timestamp for segment.
			ts := fetchTimestampFromMessage(item)
			if dir {
				// Scanning in reverse direction. Set last append ts and next offset.
				seg.nextOffSet = seg.keyToOffset(key) + 1
				seg.lastMsgTs = ts
			} else {
				// Scanning in forward direction. Set the first append timestamp.
				seg.firstMsgTs = ts
			}
			break
		}
		itr.Close()
	}

}
