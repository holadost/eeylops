package segments

import (
	"bytes"
	"context"
	"eeylops/server/base"
	sbase "eeylops/server/storage/base"
	"eeylops/server/storage/kv_store"
	"eeylops/util"
	"eeylops/util/logging"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

var (
	kLastRLogIdxKeyBytes          = sbase.KLastRLogIdxKeyBytes
	kTimestampIndexPrefixKeyBytes = []byte(kTimestampIndexKeyPrefix)
	KOffSetPrefixBytes            = []byte(kOffsetKeyPrefix)
)

// BadgerSegment implements Segment where the data is backed using badger db.
type BadgerSegment struct {
	segLock     sync.RWMutex       // A RW lock for the segment.
	rootDir     string             // Root directory of this segment.
	topicName   string             // Topic name.
	partitionID uint               // Partition ID.
	ttlSeconds  int                // TTL seconds for a message.
	dataDB      kv_store.KVStore   // Backing KV store to hold the data.
	metadataDB  *SegmentMetadataDB // Segment metadata DB.
	startOffset int64              // Segment start offset

	// Flags that indicate whether segment has been opened and closed.
	closed            bool          // Flag that indicates whether the segment is closed.
	openedOnce        bool          // A flag to indicate if the segment was opened once. Segment cannot be reopened.
	segmentClosedChan chan struct{} // A channel to ask background goroutines to exit.

	// Segment metadata.
	liveStateLock sync.RWMutex     // Protects access to nextOffset, firstMsgTs, lastMsgTs and lastRLogIdx. TODO: Come up with a better name for this.
	metadata      *SegmentMetadata // Cached segment metadata.
	nextOffset    int64            // Next start offset for new appends.
	lastRLogIdx   int64            // Last replicated log index.
	firstMsgTs    int64            // First message timestamp.
	lastMsgTs     int64            // Last message timestamp.

	// Segment logger.
	logger *logging.PrefixLogger // Logger object.

	// States used for timestamp based indexes on messages written to the segment.
	currentIndexBatchSizeBytes int64                      // Current index size in bytes.
	timestampBRI               []TimestampIndexEntry      // Timestamp block range index.
	timestampIndexLock         sync.RWMutex               // This protects timestampBRI
	timestampIndexChan         chan []TimestampIndexEntry // The channel where the timestamp indexes are forwarded.
	rebuildIndexOnce           sync.Once                  // Mutex to protect us from building indexes only once.

}

type BadgerSegmentOpts struct {
	RootDir     string                // Root directory for the segment. This is a compulsory parameter.
	Logger      *logging.PrefixLogger // Parent logger if any. Optional parameter.
	Topic       string                // Topic name. Optional parameter.
	PartitionID uint                  // Partition ID. Optional parameter.
	TTLSeconds  int                   // TTL seconds for messages.
}

// NewBadgerSegment initializes a new instance of badger segment.
func NewBadgerSegment(opts *BadgerSegmentOpts) (*BadgerSegment, error) {
	if err := os.MkdirAll(path.Join(opts.RootDir, dataDirName), 0774); err != nil {
		return nil, err
	}
	seg := new(BadgerSegment)
	seg.rootDir = opts.RootDir
	// Set segment ID as root directory for now since we still haven't initialized the segment metadata.
	if opts.Logger == nil {
		seg.logger = logging.NewPrefixLogger(fmt.Sprintf("segment:%s", opts.RootDir))
	} else {
		seg.logger = opts.Logger
	}
	seg.topicName = opts.Topic
	seg.partitionID = opts.PartitionID
	seg.ttlSeconds = opts.TTLSeconds
	seg.initialize()
	// Reinitialize logger with correct segment id.
	if opts.Logger == nil {
		seg.logger = logging.NewPrefixLogger(fmt.Sprintf("segment:%d", seg.ID()))
	}
	return seg, nil
}

func NewBadgerSegmentWithMetadata(opts *BadgerSegmentOpts, sm SegmentMetadata) (*BadgerSegment, error) {
	if err := os.MkdirAll(path.Join(opts.RootDir, dataDirName), 0774); err != nil {
		return nil, err
	}
	seg := new(BadgerSegment)
	seg.rootDir = opts.RootDir
	seg.topicName = opts.Topic
	seg.partitionID = opts.PartitionID
	seg.ttlSeconds = opts.TTLSeconds
	// Set segment ID as root directory for now since we still haven't initialized the segment metadata.
	if opts.Logger == nil {
		seg.logger = logging.NewPrefixLogger(fmt.Sprintf("segment:%s", opts.RootDir))
	} else {
		seg.logger = opts.Logger
	}
	seg.metadataDB = NewSegmentMetadataDB(seg.rootDir)
	seg.SetMetadata(sm)
	seg.initialize()
	// Reinitialize logger with correct segment id.
	if opts.Logger == nil {
		seg.logger = logging.NewPrefixLogger(fmt.Sprintf("segment:%d", seg.ID()))
	}
	return seg, nil
}

// Initialize implements the Segment interface. It initializes the segment.
func (seg *BadgerSegment) initialize() {
	seg.logger.Infof("Initializing badger segment located at: %s", seg.rootDir)
	// Initialize metadata ddb.
	if seg.metadataDB == nil {
		seg.metadataDB = NewSegmentMetadataDB(seg.rootDir)
	}
	seg.metadata = seg.metadataDB.GetMetadata()
	if seg.metadata.ID == 0 {
		seg.logger.Infof("Did not find any metadata associated with this segment. This must be a new segment!")
		seg.startOffset = -1
	} else {
		seg.startOffset = int64(seg.metadata.StartOffset)
	}
	seg.segmentClosedChan = make(chan struct{}, 1)
	seg.timestampIndexChan = make(chan []TimestampIndexEntry, 128)
}

// ID returns the segment id.
func (seg *BadgerSegment) ID() int {
	return int(seg.metadata.ID)
}

// Open opens the segment.
func (seg *BadgerSegment) Open() {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.logger.Infof("Opening segment")
	if seg.openedOnce {
		seg.logger.Fatalf("The segment cannot be reopened again")
	}
	seg.openedOnce = true
	seg.closed = false
	seg.open()
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
	close(seg.segmentClosedChan)
	seg.metadataDB.Close()
	var err error
	if seg.dataDB != nil {
		err = seg.dataDB.Close()
	}
	seg.metadataDB = nil
	seg.closed = true
	return err
}

// IsEmpty implements the Segment interface. Returns true if the segment is empty. False otherwise.
func (seg *BadgerSegment) IsEmpty() bool {
	nextOff := seg.getNextOffset()
	return nextOff == seg.metadata.StartOffset
}

// Append appends the given entries to the segment.
func (seg *BadgerSegment) Append(ctx context.Context, arg *AppendEntriesArg) *AppendEntriesRet {
	seg.segLock.RLock()
	defer seg.segLock.RUnlock()
	var ret AppendEntriesRet
	ret.Error = nil

	// Sanity checks.
	if seg.closed || seg.metadata.Expired || seg.metadata.Immutable || !seg.openedOnce {
		seg.logger.Errorf("Segment is either closed, expired or immutable. Cannot append entries")
		ret.Error = ErrSegmentClosed
		return &ret
	}
	lastMsgTs := seg.getLastMsgTs()
	if arg.Timestamp < lastMsgTs {
		seg.logger.Errorf("Invalid timestamp: %d. Expected timestamp >= %d", arg.Timestamp, lastMsgTs)
		ret.Error = ErrSegmentInvalidTimestamp
		return &ret
	}
	if arg.RLogIdx <= seg.lastRLogIdx {
		seg.logger.Errorf("Invalid replicated log index: %d. Expected value greater than: %d",
			arg.RLogIdx, seg.lastRLogIdx)
		ret.Error = ErrSegmentInvalidRLogIdx
		return &ret
	}
	nextOff := seg.getNextOffset()
	oldNextOffset := nextOff
	// Convert entries into (key, value) pairs
	keys := seg.generateKeys(nextOff, base.Offset(len(arg.Entries)))
	values, tsEntries, nextIndexBatchSizeBytes := prepareMessageValues(arg.Entries, arg.Timestamp,
		seg.currentIndexBatchSizeBytes, nextOff)

	// Add an index entry if required.
	currIndexSize := len(seg.timestampBRI)
	for ii, tse := range tsEntries {
		keys = append(keys, append(kTimestampIndexPrefixKeyBytes, util.UintToBytes(uint64(currIndexSize+ii))...))
		tse.SetTimestamp(arg.Timestamp)
		tse.SetOffset(nextOff)
		values = append(values, tse.Serialize())
	}

	// Update replicated log index as well.
	keys = append(keys, kLastRLogIdxKeyBytes)
	values = append(values, util.UintToBytes(uint64(arg.RLogIdx)))

	// Persist entries.
	err := seg.dataDB.BatchPut(keys, values)
	if err != nil {
		seg.logger.Errorf("Unable to append entries in segment: %d due to err: %s", seg.ID(), err.Error())
		ret.Error = ErrSegmentBackend
		return &ret
	}

	// Update segment internal states.
	seg.liveStateLock.Lock()
	seg.currentIndexBatchSizeBytes = nextIndexBatchSizeBytes
	if oldNextOffset == seg.metadata.StartOffset {
		// These were the first messages appended to the segment. Save the first message timestamp.
		seg.setFirstMsgTs(arg.Timestamp)
	}
	seg.lastRLogIdx = arg.RLogIdx
	seg.setLastMsgTs(arg.Timestamp)
	nextOff += base.Offset(len(arg.Entries))
	seg.setNextOffset(nextOff)
	seg.liveStateLock.Unlock()

	if len(tsEntries) > 0 {
		// Notify indexer with the new index entry that was created.
		seg.notifyIndexer(tsEntries)
	}
	return &ret
}

// Scan attempts to fetch the requested number of messages sequentially starting from the entry with the given
// StartOffset or StartTimestamp. Only one of StartOffset or StartTimestamp must be provided.
func (seg *BadgerSegment) Scan(ctx context.Context, arg *ScanEntriesArg) *ScanEntriesRet {
	seg.segLock.RLock()
	defer seg.segLock.RUnlock()
	var ret ScanEntriesRet
	// Sanitize arguments.
	if err := seg.sanitizeScanArg(arg, &ret); err != nil {
		return &ret
	}

	// Compute start and end offsets for the scan.
	startOffset, err := seg.computeStartOffsetForScan(arg, &ret)
	if err != nil {
		return &ret
	}

	// Scan the segment for all messages between start and end offset.
	seg.scanMessages(arg, &ret, startOffset)
	return &ret
}

// MarkImmutable marks the segment as immutable.
func (seg *BadgerSegment) MarkImmutable() {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.logger.Infof("Marking segment as immutable")
	seg.metadata.Immutable = true
	seg.metadata.ImmutableTimestamp = time.Now()
	if !seg.IsEmpty() {
		// The liveStateLock is not required here since we can be sure there are no appends happening at this
		//time(as we have a write lock on the segment). Lets acquire the lock however for paranoia reasons.
		seg.liveStateLock.RLock()
		defer seg.liveStateLock.RUnlock()
		nextOff := seg.getNextOffset()
		seg.metadata.EndOffset = nextOff - 1
		seg.metadata.FirstMsgTimestamp = time.Unix(0, seg.getFirstMsgTs())
		seg.metadata.LastMsgTimestamp = time.Unix(0, seg.getLastMsgTs())
	}
	seg.metadataDB.PutMetadata(seg.metadata)
}

// MarkExpired marks the segment as expired.
func (seg *BadgerSegment) MarkExpired() {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.logger.Infof("Marking segment as expired")
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
		seg.liveStateLock.RLock()
		defer seg.liveStateLock.RUnlock()
		nextOff := seg.getNextOffset()
		if nextOff == metadata.StartOffset {
			metadata.EndOffset = -1
		} else {
			metadata.EndOffset = nextOff - 1
		}
		metadata.FirstMsgTimestamp = time.Unix(0, seg.getFirstMsgTs())
		metadata.LastMsgTimestamp = time.Unix(0, seg.getLastMsgTs())
	}
	return metadata
}

// GetRange returns the range(start and end offset) of the segment.
func (seg *BadgerSegment) GetRange() (sOff base.Offset, eOff base.Offset) {
	startOff := seg.getStartOffset()
	nextOff := seg.getNextOffset()
	if nextOff == startOff {
		return startOff, -1
	}
	return startOff, nextOff - 1
}

// GetMsgTimestampRange returns the first and last message unix timestamps(in nanoseconds).
func (seg *BadgerSegment) GetMsgTimestampRange() (int64, int64) {
	if seg.IsEmpty() {
		return -1, -1
	}
	// Fast path. Just load and see if the values look sane and if they do, just return.
	firstMsgTs := seg.getFirstMsgTs()
	lastMsgTs := seg.getLastMsgTs()
	if (firstMsgTs == lastMsgTs) || ((firstMsgTs > 0) && (firstMsgTs < lastMsgTs)) {
		return firstMsgTs, lastMsgTs
	}
	// The data did not make sense i.e. the firstMsgTs was greater than the lastMsgTs or we had a lastMsgTs but not the
	// firstMsgTs. This could have only happened if we were racing with the very first append to this segment.
	// Take the liveStateLock and then read the values.
	seg.liveStateLock.RLock()
	defer seg.liveStateLock.RUnlock()
	return seg.getFirstMsgTs(), seg.getLastMsgTs()
}

// SetMetadata sets the metadata for the segment.
func (seg *BadgerSegment) SetMetadata(sm SegmentMetadata) {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.metadataDB.PutMetadata(&sm)
	seg.metadata = &sm
	// We do this because when the segment is first initialized, the parent will set the metadata and then
	// open the segment i.e. the segment might not be reinitialized. Set it here again for paranoia reasons.
	// Doing this should not affect correctness.
	seg.setStartOffset(seg.metadata.StartOffset)
}

// Stats returns the stats of the segment.
func (seg *BadgerSegment) Stats() {

}

// Size returns the stats of the segment.
func (seg *BadgerSegment) Size() int64 {
	seg.segLock.RLock()
	defer seg.segLock.RUnlock()
	return seg.dataDB.Size()
}

func (seg *BadgerSegment) getStartOffset() base.Offset {
	return base.Offset(atomic.LoadInt64(&seg.startOffset))
}

func (seg *BadgerSegment) setStartOffset(off base.Offset) {
	atomic.StoreInt64(&seg.startOffset, int64(off))
}

// Returns the current nextOffset of the segment.
func (seg *BadgerSegment) getNextOffset() base.Offset {
	return base.Offset(atomic.LoadInt64(&seg.nextOffset))
}

// Returns the current nextOffset of the segment.
func (seg *BadgerSegment) setNextOffset(val base.Offset) {
	atomic.StoreInt64(&seg.nextOffset, int64(val))
}

func (seg *BadgerSegment) getFirstMsgTs() int64 {
	return atomic.LoadInt64(&seg.firstMsgTs)
}

func (seg *BadgerSegment) setFirstMsgTs(ts int64) {
	atomic.StoreInt64(&seg.firstMsgTs, ts)
}

func (seg *BadgerSegment) getLastMsgTs() int64 {
	return atomic.LoadInt64(&seg.lastMsgTs)
}

func (seg *BadgerSegment) setLastMsgTs(ts int64) {
	atomic.StoreInt64(&seg.lastMsgTs, ts)
}

// sanitizeScanArg is a helper method to Scan that checks if the arguments provided are fine. If not, it populates
// ret and returns an error. This method assumes that the segLock has been acquired.
func (seg *BadgerSegment) sanitizeScanArg(arg *ScanEntriesArg, ret *ScanEntriesRet) error {
	ret.Error = nil
	if arg.StartOffset == -1 && arg.StartTimestamp == -1 {
		seg.logger.Fatalf("Both StartOffset and StartTimestamp cannot be undefined")
	}
	if seg.closed || seg.metadata.Expired || !seg.openedOnce {
		seg.logger.Errorf("Segment is already closed")
		ret.Error = ErrSegmentClosed
	}
	nextOff := seg.getNextOffset()
	// Sanity checks to see if the segment contains our start offset.
	if (arg.StartOffset >= 0) && (arg.StartOffset < seg.metadata.StartOffset) {
		// The start offset does not belong to this segment.
		seg.logger.Errorf("Start offset does not belong to this segment. Given start offset: %d, "+
			"Seg start offset: %d, Segment next offset: %d", arg.StartOffset, seg.metadata.StartOffset, nextOff)
		ret.Error = ErrSegmentInvalid
	}
	if seg.IsEmpty() || (arg.StartOffset >= nextOff) {
		// Either the segment is empty or it does not contain our desired offset yet.
		seg.logger.VInfof(1, "Requested offset: %d is not present in this segment. "+
			"Seg Start offset: %d, Segment Next Offset: %d", arg.StartOffset, seg.metadata.StartOffset, nextOff)
		ret.Error = nil
		ret.Values = nil
		ret.NextOffset = -1
		return errors.New("empty segment")
	}
	return ret.Error
}

// computeStartOffsetForScan is a helper method for Scan that computes the first offset from where the scan should
// start. If no such offset if found, or if an offset is found and that was the only offset requested(in which case we
// populate ret and return early and avoid re-reading this message again), this method  will return an error.
// Otherwise, it will return the start offset from where the scan should start. This method assumes that the segLock
// has been acquired.
func (seg *BadgerSegment) computeStartOffsetForScan(arg *ScanEntriesArg, ret *ScanEntriesRet) (base.Offset, error) {
	now := time.Now()
	firstUnexpiredMsgTs := now.UnixNano() - int64(seg.ttlSeconds)*(1e9)
	firstMsgTs := seg.getFirstMsgTs()
	var startOffset base.Offset
	if arg.StartOffset >= 0 {
		startOffset = arg.StartOffset
		if firstMsgTs < firstUnexpiredMsgTs {
			// The message may have expired. Check if it has and if it has, move to the first unexpired offset.
			msg, err := seg.getMsgAtOffset(arg.StartOffset)
			if err != nil {
				ret.Error = err
				return -1, ret.Error
			}
			// Check if message has expired.
			if seg.hasValueExpired(msg.GetTimestamp(), now.UnixNano()) {
				// The message has expired. Scan the index to find the first offset that hasn't expired.
				startOffset, err = seg.findFirstOffsetWithTimestampGE(firstUnexpiredMsgTs)
				if err != nil {
					ret.Error = err
					return -1, ret.Error
				}
				if startOffset == -1 {
					seg.logger.Errorf("Unable to find any live records in the segment")
					ret.Error = nil
					ret.NextOffset = -1
					return -1, ErrSegmentNoRecordsWithTimestamp
				}
			} else {
				// The value hasn't expired. Check if only one value was requested and if so, we can return early here
				// since we already have the message with us.
				if arg.NumMessages == 1 {
					ret.Values = append(ret.Values, &sbase.ScanValue{
						Offset:    arg.StartOffset,
						Value:     msg.GetBody(),
						Timestamp: msg.GetTimestamp(),
					})
					ret.Error = nil
					ret.NextOffset = arg.StartOffset + 1
					return -1, errSegmentScanDoneEarly
				}
			}
		}
	} else {
		startTs := arg.StartTimestamp
		mayBeStartTs := util.MaxInt(firstUnexpiredMsgTs, firstMsgTs)
		if mayBeStartTs >= startTs {
			startTs = mayBeStartTs
		}
		var err error
		startOffset, err = seg.findFirstOffsetWithTimestampGE(startTs)
		if err != nil {
			seg.logger.Errorf("Failed to find start offset due to err: %s", err.Error())
			ret.Error = ErrSegmentBackend
			return -1, ret.Error
		}
		if startOffset == -1 {
			seg.logger.VInfof(1, "Unable to find any messages with timestamp >= %d", arg.StartTimestamp)
			ret.Error = nil
			ret.NextOffset = -1
			return -1, ErrSegmentNoRecordsWithTimestamp
		}
	}
	// Sanity check to make sure that we have a start offset.
	if startOffset == -1 {
		seg.logger.Fatalf("Unable to determine any good start offset for start offset: %d, start timestamp: %d",
			arg.StartOffset, arg.StartTimestamp)
	}
	return startOffset, nil
}

// scanMessages is a helper method for Scan which scans messages from the segment from the given startOffset. It ends
// the scan when arg.NumMessages is reached or arg.ScanSizeBytes is exceeded or if the requested timestamp is lesser
// than arg.EndTimestamp.
func (seg *BadgerSegment) scanMessages(arg *ScanEntriesArg, ret *ScanEntriesRet, startOffset base.Offset) {
	var tmpNumMsgs base.Offset
	var endOffset base.Offset
	var sk []byte
	tmpNumMsgs = base.Offset(arg.NumMessages)
	nextOff := seg.getNextOffset()
	if startOffset+tmpNumMsgs >= nextOff {
		tmpNumMsgs = nextOff - startOffset
	}
	endOffset = startOffset + tmpNumMsgs - 1
	sk = seg.offsetToKey(startOffset)
	scanner := seg.dataDB.CreateScanner(KOffSetPrefixBytes, sk, false)
	defer scanner.Close()
	bytesScannedSoFar := int64(0)
	// now := time.Now().UnixNano()
	for ; scanner.Valid(); scanner.Next() {
		key, val, err := scanner.GetItem()
		if err != nil {
			seg.logger.Errorf("Failed to scan offset due to scan backend err: %s", err.Error())
			ret.Error = ErrSegmentBackend
			ret.Values = nil
			ret.NextOffset = -1
			break
		}
		offset := seg.keyToOffset(key)
		var msg Message
		msg.InitializeFromRaw(val)
		if arg.ScanSizeBytes > 0 {
			bytesScannedSoFar += int64(msg.GetBodySize())
			if bytesScannedSoFar > arg.ScanSizeBytes {
				break
			}
		}
		if arg.EndTimestamp > 0 {
			if msg.GetTimestamp() >= arg.EndTimestamp {
				break
			}
		}
		ret.Values = append(ret.Values, &sbase.ScanValue{
			Offset:    offset,
			Value:     msg.GetBody(),
			Timestamp: msg.GetTimestamp(),
		})
		ret.NextOffset = offset + 1
		if offset == endOffset {
			break
		}
	}
}

// getMsgAtOffset fetches a single offset from the backend.
func (seg *BadgerSegment) getMsgAtOffset(offset base.Offset) (*Message, error) {
	val, err := seg.dataDB.Get(seg.offsetToKey(offset))
	if err != nil {
		seg.logger.Errorf("Unable to get offset: %d due to err: %s", offset, err.Error())
		return nil, ErrSegmentBackend
	}
	var msg Message
	msg.InitializeFromRaw(val)
	return &msg, nil
}

// findFirstOffsetWithTimestampGE finds the first offset in the segment whose timestamp >= given ts. This method
// assumes that segLock has been acquired.
func (seg *BadgerSegment) findFirstOffsetWithTimestampGE(ts int64) (base.Offset, error) {
	startOffsetHint := seg.getNearestSmallerOffsetFromIndex(ts)
	if startOffsetHint == -1 {
		return -1, ErrSegmentInvalidTimestamp
	}
	scanner := seg.dataDB.CreateScanner(KOffSetPrefixBytes, seg.offsetToKey(startOffsetHint), false)
	for ; scanner.Valid(); scanner.Next() {
		key, val, err := scanner.GetItem()
		if err != nil {
			seg.logger.Errorf("Failure while scanning the segment. Error: %s", err.Error())
			return -1, ErrSegmentBackend
		}
		offset := seg.keyToOffset(key)
		var msg Message
		msg.InitializeFromRaw(val)
		if ts > msg.GetTimestamp() {
			continue
		}
		return offset, nil
	}
	// We didn't find any message with timestamp >= to the given ts.
	return -1, nil
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

// offsetToKey converts the given offset to a key representation for dataDB.
func (seg *BadgerSegment) offsetToKey(offset base.Offset) []byte {
	return append(KOffSetPrefixBytes, util.UintToBytes(uint64(offset))...)
}

// keyToOffset converts the key representation of the offset(from dataDB) to base.Offset type.
func (seg *BadgerSegment) keyToOffset(key []byte) base.Offset {
	if len(key) != len(KOffSetPrefixBytes)+8 {
		seg.logger.Fatalf("Given key: %s is not an offset key", string(key))
	}
	return base.Offset(util.BytesToUint(key[len(KOffSetPrefixBytes):]))
}

// numToIndexKey converts the given number to a key representation for dataDB.
func (seg *BadgerSegment) numToIndexKey(num int) []byte {
	return append(kTimestampIndexPrefixKeyBytes, util.UintToBytes(uint64(num))...)
}

// indexKeyToNum converts the given index key to its numeric representation.
func (seg *BadgerSegment) indexKeyToNum(key []byte) int {
	if len(key) != len(kTimestampIndexPrefixKeyBytes)+8 {
		seg.logger.Fatalf("Expected timestamp index key. Got: %s", string(key))
	}
	return int(util.BytesToUint(key[len(kTimestampIndexPrefixKeyBytes):]))
}

// A helper method that lets us know if a key has started with the given prefix.
func (seg *BadgerSegment) doesKeyStartWithPrefix(key []byte, prefix []byte) bool {
	if len(key) < len(prefix) {
		return false
	}
	if bytes.Compare(key[0:len(prefix)], prefix) != 0 {
		return false
	}
	return true
}

// hasValueExpired returns true if the value has expired. False otherwise.
func (seg *BadgerSegment) hasValueExpired(tsNano int64, nowNano int64) bool {
	if (nowNano-tsNano)/(1e9) >= int64(seg.ttlSeconds) {
		return true
	}
	return false
}

// indexer is a background goroutine that is started when the segment is first opened. It scans dataDB and
// rebuilds an in memory timestamp index. It then waits for new indexes that were created and updates the
// in memory timestamp index.
func (seg *BadgerSegment) indexer() {
	seg.rebuildIndexes()
	for {
		select {
		case <-seg.segmentClosedChan:
			seg.logger.VInfof(1, "Indexer exiting")
			return
		case entries := <-seg.timestampIndexChan:
			seg.updateTimestampIndex(entries)
		}
	}
}

// notifyIndexer is called by Append when a new index entry needs to be added to the timestamp index.
func (seg *BadgerSegment) notifyIndexer(entries []TimestampIndexEntry) {
	seg.timestampIndexChan <- entries
}

// updateTimestampIndex is called by the indexer when it receives an index entry on timestampIndexChan.
func (seg *BadgerSegment) updateTimestampIndex(entries []TimestampIndexEntry) {
	// Acquire write lock on the timestamp index as we are about to modify it.
	seg.timestampIndexLock.Lock()
	defer seg.timestampIndexLock.Unlock()

	if seg.closed || !seg.openedOnce {
		seg.logger.Warningf("Either segment is closed or never opened. Skipping timestamp index updates")
		return
	}
	seg.timestampBRI = append(seg.timestampBRI, entries...)
}

// getNearestSmallerOffsetFromIndex returns the first offset that is smaller than the given timestamp. This method
// assumes that segLock has been acquired.
func (seg *BadgerSegment) getNearestSmallerOffsetFromIndex(ts int64) base.Offset {
	seg.timestampIndexLock.RLock()
	defer seg.timestampIndexLock.RUnlock()
	if len(seg.timestampBRI) == 0 {
		// There are no indexes. Return the smallest offset.
		return seg.metadata.StartOffset
	}
	if ts > seg.timestampBRI[len(seg.timestampBRI)-1].GetTimestamp() {
		return seg.timestampBRI[len(seg.timestampBRI)-1].GetOffset()
	}

	// Binary search index to find nearest offset lesser than the given timestamp.
	left := 0
	right := len(seg.timestampBRI) - 1
	nearestIdx := -1
	for {
		if left > right {
			break
		}
		mid := left + (right-left)/2
		if seg.timestampBRI[mid].GetTimestamp() >= ts {
			right = mid - 1
		} else {
			// mid now could be the potential nearest index as the given timestamp is definitely not present to the
			// leftside of mid(as all elements would be smaller than mid and hence couldn't be nearer than we are now).
			// It is still possible that we might find a candidate to the right of mid in subsequent iterations.
			nearestIdx = mid
			left = mid + 1
		}
	}
	if nearestIdx == -1 {
		if ts >= seg.getFirstMsgTs() {
			return seg.metadata.StartOffset
		}
		return base.Offset(-1)
	}
	return seg.timestampBRI[nearestIdx].GetOffset()
}

// rebuildIndexes is called by the indexer when the segment is opened for the first time.
func (seg *BadgerSegment) rebuildIndexes() {
	seg.rebuildIndexOnce.Do(func() {
		seg.logger.VInfof(1, "Rebuilding segment indexes")
		startIdxKey := seg.numToIndexKey(0)
		_, err := seg.dataDB.Get(startIdxKey)
		if err != nil {
			if err == kv_store.ErrKVStoreKeyNotFound {
				seg.logger.VInfof(1, "No indexes found. Nothing to rebuild")
				return
			}
			seg.logger.Fatalf("Error while attempting to fetch timestamp index. Error: %s", err.Error())
		}
		seg.timestampIndexLock.Lock()
		defer seg.timestampIndexLock.Unlock()
		// Clear the index.
		seg.timestampBRI = nil

		// Scan the DB for keys with the timestamp index prefix and rebuild timestampBRI.
		itr := seg.dataDB.CreateScanner(kTimestampIndexPrefixKeyBytes, startIdxKey, false)
		defer itr.Close()
		itr.Seek(startIdxKey)
		count := 0
		for ; itr.Valid(); itr.Next() {
			key, val, err := itr.GetItem()
			if err != nil {
				seg.logger.Fatalf("Unable to initialize index due to scan err: %s", err.Error())
			}
			// Sanity check to make sure that the key is of the expected type.
			if !seg.doesKeyStartWithPrefix(key, kTimestampIndexPrefixKeyBytes) {
				seg.logger.Fatalf("Did not expect other keys in index prefix scan. Got key: %s", string(key))
			}
			idxNum := seg.indexKeyToNum(key)
			// Sanity check to make sure that the entry is monotonically increasing.
			if idxNum != count {
				seg.logger.Fatalf("Expected to find index id: %d, got: %d", count, idxNum)
			}
			var tse TimestampIndexEntry
			tse.InitializeFromRaw(val)
			seg.timestampBRI = append(seg.timestampBRI, tse)
			count++
		}
		seg.logger.VInfof(1, "Found %d index entries persisted in segment", len(seg.timestampBRI))
	})
}

// Opens the segment. This method assumes that segLock has been acquired.
func (seg *BadgerSegment) open() {
	opts := badger.DefaultOptions(path.Join(seg.rootDir, dataDirName))
	opts.SyncWrites = true
	opts.NumMemtables = 3
	opts.VerifyValueChecksum = true
	opts.BlockCacheSize = 0 // Disable block cache.
	opts.NumCompactors = 2  // Use 2 compactors.
	opts.IndexCacheSize = 0
	opts.Compression = options.None
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO
	opts.CompactL0OnClose = false
	opts.Logger = seg.logger
	if seg.metadata.Immutable {
		opts.ReadOnly = true
		opts.NumMemtables = 0
	}
	opts.Logger = seg.logger
	seg.dataDB = kv_store.NewBadgerKVStore(path.Join(seg.rootDir, dataDirName), opts)

	go seg.indexer()
	// Gather the last replicated log index in the segment.
	val, err := seg.dataDB.Get(kLastRLogIdxKeyBytes)
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			// Segment is empty.
			seg.nextOffset = int64(seg.metadata.StartOffset)
			seg.firstMsgTs = -1
			seg.lastMsgTs = -1
			seg.lastRLogIdx = -1
			return
		} else {
			seg.logger.Fatalf("Error while getting last log index. Error: %s", err.Error())
		}
	} else {
		seg.lastRLogIdx = int64(util.BytesToUint(val))
	}

	// Gather first and last message timestamps by scanning in both forward and reverse directions. Also gather the
	// last offset appended and hence the nextOffset in the segment.
	dirs := []bool{false, true}
	for _, dir := range dirs {
		var sk []byte
		// TODO: Ugly hack here currently. When scanning in forward direction, the timestamp index keys come first.
		// TODO: Prefix scans as a result return immediately since the very first key does not contain our prefix.
		// TODO: So for forward scans, we set the start key forcefully. For reverse scans, we luck out because
		// TODO: the offset keys are the last keys in the DB and hence no start key is required. If we did, this would
		// TODO: fail since we don't know the last key(this method in fact initializes the last offset in the segment).
		// TODO: The clean fix would be to have the KV store provide a CF notion that would protect us from these
		// TODO: sort of scenarios.
		if !dir {
			sk = seg.offsetToKey(seg.metadata.StartOffset)
		}
		itr := seg.dataDB.CreateScanner(KOffSetPrefixBytes, sk, dir)
		for ; itr.Valid(); itr.Next() {
			key, item, err := itr.GetItem()
			if err != nil {
				seg.logger.Fatalf("Unable to initialize next offset, first and last message timestamps due to "+
					"scan err: %s", err.Error())
			}
			if bytes.Compare(key[0:len(KOffSetPrefixBytes)], KOffSetPrefixBytes) != 0 {
				seg.logger.Fatalf("Expected key with offset prefix. Got: %s", string(key))
			}

			// Set next offset and last appended timestamp for segment.
			var msg Message
			msg.InitializeFromRaw(item)
			if dir {
				// We are scanning in reverse direction. Set last append ts and next offset.
				seg.nextOffset = int64(seg.keyToOffset(key) + 1)
				seg.lastMsgTs = msg.GetTimestamp()
			} else {
				// Scanning in forward direction. Set the first append timestamp.
				seg.firstMsgTs = msg.GetTimestamp()
			}
			break
		}
		itr.Close()
	}
	seg.logger.VInfof(1, "First Message Timestamp: %d, Last Message Timestamp: %d, Next Offset: %d, "+
		"Last RLog Index: %d", seg.firstMsgTs, seg.lastMsgTs, seg.nextOffset, seg.lastRLogIdx)
}
