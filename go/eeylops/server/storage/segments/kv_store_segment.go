package segments

import (
	"bytes"
	"context"
	"eeylops/server/base"
	storagebase "eeylops/server/storage/base"
	"eeylops/server/storage/kv_store"
	bkv "eeylops/server/storage/kv_store/badger_kv_store"
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
	kLastRLogIdxKeyBytes = storagebase.KLastRLogIdxKeyBytes
	kNextIndexKeyBytes   = []byte("next_index_key")
)

// KVStoreSegment implements Segment where the data is backed using a KV store. Currently, KV store uses badger.
type KVStoreSegment struct {
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
	nextTimestampIndexKey      int                        // Next timestamp index key.
}

type KVStoreSegmentOpts struct {
	RootDir     string                // Root directory for the segment. This is a compulsory parameter.
	Logger      *logging.PrefixLogger // Parent logger if any. Optional parameter.
	Topic       string                // Topic name. Optional parameter.
	PartitionID uint                  // Partition ID. Optional parameter.
	TTLSeconds  int                   // TTL seconds for messages.
}

// NewKVStoreSegment initializes a new instance of KV store based segment.
func NewKVStoreSegment(opts *KVStoreSegmentOpts) (*KVStoreSegment, error) {
	if err := os.MkdirAll(path.Join(opts.RootDir, dataDirName), 0774); err != nil {
		return nil, err
	}
	seg := new(KVStoreSegment)
	seg.rootDir = opts.RootDir
	// Set segment ID as root directory for now since we still haven't initialized the segment metadata.
	if opts.Logger == nil {
		seg.logger = logging.NewPrefixLogger(fmt.Sprintf("segment-%s", opts.RootDir))
	} else {
		seg.logger = opts.Logger
	}
	seg.topicName = opts.Topic
	seg.partitionID = opts.PartitionID
	seg.ttlSeconds = opts.TTLSeconds
	seg.initialize()
	// Reinitialize logger with correct segment id.
	if opts.Logger == nil {
		seg.logger = logging.NewPrefixLogger(fmt.Sprintf("segment: %d", seg.ID()))
	}
	return seg, nil
}

func NewKVStoreSegmentWithMetadata(opts *KVStoreSegmentOpts, sm SegmentMetadata) (*KVStoreSegment, error) {
	if err := os.MkdirAll(path.Join(opts.RootDir, dataDirName), 0774); err != nil {
		return nil, err
	}
	seg := new(KVStoreSegment)
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
func (seg *KVStoreSegment) initialize() {
	seg.logger.Infof("Initializing segment located at: %s", seg.rootDir)
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
func (seg *KVStoreSegment) ID() int {
	return int(seg.metadata.ID)
}

// Open opens the segment.
func (seg *KVStoreSegment) Open() {
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
func (seg *KVStoreSegment) Close() error {
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
		seg.dataDB.Close()
	}
	seg.metadataDB = nil
	seg.closed = true
	return err
}

// IsEmpty implements the Segment interface. Returns true if the segment is empty. False otherwise.
func (seg *KVStoreSegment) IsEmpty() bool {
	nextOff := seg.getNextOffset()
	return nextOff == seg.metadata.StartOffset
}

// Append appends the given entries to the segment.
func (seg *KVStoreSegment) Append(ctx context.Context, arg *AppendEntriesArg) *AppendEntriesRet {
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
	var entries []*kv_store.KVStoreEntry
	for ii, key := range keys {
		entry := &kv_store.KVStoreEntry{
			Key:          key,
			Value:        values[ii],
			ColumnFamily: kOffsetColumnFamily,
		}
		entries = append(entries, entry)
	}

	// Add index entries if required.
	nextIndexKey := seg.getNextIndexKey()
	for _, tse := range tsEntries {
		entries = append(entries, &kv_store.KVStoreEntry{
			Key:          util.UintToBytes(uint64(nextIndexKey)),
			Value:        tse.Serialize(),
			ColumnFamily: kTimestampIndexColumnFamily,
		})
		nextIndexKey++
	}

	// Update replicated log index and next index key as well.
	entries = append(entries,
		&kv_store.KVStoreEntry{
			Key:          kLastRLogIdxKeyBytes,
			Value:        util.UintToBytes(uint64(arg.RLogIdx)),
			ColumnFamily: kMiscColumnFamily},
		&kv_store.KVStoreEntry{
			Key:          kNextIndexKeyBytes,
			Value:        util.UintToBytes(uint64(nextIndexKey)),
			ColumnFamily: kMiscColumnFamily,
		})

	// Persist entries.
	err := seg.dataDB.BatchPut(entries)
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
	if len(tsEntries) > 0 {
		seg.setNextIndexKey(nextIndexKey)
	}
	seg.liveStateLock.Unlock()

	if len(tsEntries) > 0 {
		// Notify indexer with the new index entry that was created.
		seg.notifyIndexer(tsEntries)
	}
	return &ret
}

// Scan attempts to fetch the requested number of messages sequentially starting from the entry with the given
// StartOffset or StartTimestamp. Only one of StartOffset or StartTimestamp must be provided.
func (seg *KVStoreSegment) Scan(ctx context.Context, arg *ScanEntriesArg) *ScanEntriesRet {
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
	if arg.NumMessages > 100 {
		seg.scanMessages(arg, &ret, startOffset)
	} else {
		seg.batchGetMessages(arg, &ret, startOffset)
	}
	return &ret
}

// MarkImmutable marks the segment as immutable.
func (seg *KVStoreSegment) MarkImmutable() {
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
func (seg *KVStoreSegment) MarkExpired() {
	seg.segLock.Lock()
	defer seg.segLock.Unlock()
	seg.logger.Infof("Marking segment as expired")
	seg.metadata.Expired = true
	seg.metadata.ExpiredTimestamp = time.Now()
	seg.metadataDB.PutMetadata(seg.metadata)
}

// GetMetadata returns a copy of the metadata associated with the segment.
func (seg *KVStoreSegment) GetMetadata() SegmentMetadata {
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
func (seg *KVStoreSegment) GetRange() (sOff base.Offset, eOff base.Offset) {
	startOff := seg.getStartOffset()
	nextOff := seg.getNextOffset()
	if nextOff == startOff {
		return startOff, -1
	}
	return startOff, nextOff - 1
}

// GetMsgTimestampRange returns the first and last message unix timestamps(in nanoseconds).
func (seg *KVStoreSegment) GetMsgTimestampRange() (int64, int64) {
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
func (seg *KVStoreSegment) SetMetadata(sm SegmentMetadata) {
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
func (seg *KVStoreSegment) Stats() {

}

// Size returns the stats of the segment.
func (seg *KVStoreSegment) Size() int64 {
	seg.segLock.RLock()
	defer seg.segLock.RUnlock()
	return seg.dataDB.Size()
}

func (seg *KVStoreSegment) getStartOffset() base.Offset {
	return base.Offset(atomic.LoadInt64(&seg.startOffset))
}

func (seg *KVStoreSegment) setStartOffset(off base.Offset) {
	atomic.StoreInt64(&seg.startOffset, int64(off))
}

// Returns the current nextOffset of the segment.
func (seg *KVStoreSegment) getNextOffset() base.Offset {
	return base.Offset(atomic.LoadInt64(&seg.nextOffset))
}

// Returns the current nextOffset of the segment.
func (seg *KVStoreSegment) setNextOffset(val base.Offset) {
	atomic.StoreInt64(&seg.nextOffset, int64(val))
}

func (seg *KVStoreSegment) getFirstMsgTs() int64 {
	return atomic.LoadInt64(&seg.firstMsgTs)
}

func (seg *KVStoreSegment) setFirstMsgTs(ts int64) {
	atomic.StoreInt64(&seg.firstMsgTs, ts)
}

func (seg *KVStoreSegment) getLastMsgTs() int64 {
	return atomic.LoadInt64(&seg.lastMsgTs)
}

func (seg *KVStoreSegment) setLastMsgTs(ts int64) {
	atomic.StoreInt64(&seg.lastMsgTs, ts)
}

func (seg *KVStoreSegment) getNextIndexKey() int {
	return seg.nextTimestampIndexKey
}

func (seg *KVStoreSegment) setNextIndexKey(key int) {
	if key <= seg.nextTimestampIndexKey {
		seg.logger.Fatalf("Key: %d must be greater than the next index key: %d", key, seg.nextTimestampIndexKey)
	}
	seg.nextTimestampIndexKey = key
}

// sanitizeScanArg is a helper method to Scan that checks if the arguments provided are fine. If not, it populates
// ret and returns an error. This method assumes that the segLock has been acquired.
func (seg *KVStoreSegment) sanitizeScanArg(arg *ScanEntriesArg, ret *ScanEntriesRet) error {
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
func (seg *KVStoreSegment) computeStartOffsetForScan(arg *ScanEntriesArg, ret *ScanEntriesRet) (base.Offset, error) {
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
					ret.Values = append(ret.Values, &storagebase.ScanValue{
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

// batchGetMessages is a helper method for Scan which scans messages from the segment from the given startOffset.
// It ends the scan when arg.NumMessages is reached or arg.ScanSizeBytes is exceeded or if the requested timestamp is
// lesser than arg.EndTimestamp.
func (seg *KVStoreSegment) batchGetMessages(arg *ScanEntriesArg, ret *ScanEntriesRet, startOffset base.Offset) {
	var tmpNumMsgs base.Offset
	var endOffset base.Offset
	tmpNumMsgs = base.Offset(arg.NumMessages)
	nextOff := seg.getNextOffset()
	if startOffset+tmpNumMsgs >= nextOff {
		tmpNumMsgs = nextOff - startOffset
	}
	endOffset = startOffset + tmpNumMsgs - 1
	txn := seg.dataDB.NewTransaction()
	defer txn.Discard()
	bytesScannedSoFar := int64(0)
	for ii := startOffset; ii <= endOffset; ii++ {
		val, err := txn.Get(&kv_store.KVStoreKey{
			Key:          seg.offsetToKey(ii),
			ColumnFamily: kOffsetColumnFamily,
		})
		if err != nil {
			seg.logger.Errorf("Failed to scan offset due to scan backend err: %s", err.Error())
			ret.Error = ErrSegmentBackend
			ret.Values = nil
			ret.NextOffset = -1
			return
		}
		var msg Message
		msg.InitializeFromRaw(val.Value)
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
		ret.Values = append(ret.Values, &storagebase.ScanValue{
			Offset:    ii,
			Value:     msg.GetBody(),
			Timestamp: msg.GetTimestamp(),
		})
		ret.NextOffset = ii + 1
	}
}

// scanMessages is a helper method for Scan which scans messages from the segment from the given startOffset. It ends
// the scan when arg.NumMessages is reached or arg.ScanSizeBytes is exceeded or if the requested timestamp is lesser
// than arg.EndTimestamp.
func (seg *KVStoreSegment) scanMessages(arg *ScanEntriesArg, ret *ScanEntriesRet, startOffset base.Offset) {
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
	scanner, err := seg.dataDB.NewScanner(kOffsetColumnFamily, sk, false)
	if err != nil {
		seg.logger.Errorf("Failed to create scanner while reading segment due to err: %v", err)
		ret.Error = ErrSegmentBackend
		ret.Values = nil
		ret.NextOffset = -1
		return
	}
	defer scanner.Close()
	bytesScannedSoFar := int64(0)
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
		ret.Values = append(ret.Values, &storagebase.ScanValue{
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
func (seg *KVStoreSegment) getMsgAtOffset(offset base.Offset) (*Message, error) {
	entry, err := seg.dataDB.Get(&kv_store.KVStoreKey{
		Key:          seg.offsetToKey(offset),
		ColumnFamily: kOffsetColumnFamily,
	})
	if err != nil {
		seg.logger.Errorf("Unable to get offset: %d due to err: %s", offset, err.Error())
		return nil, ErrSegmentBackend
	}
	var msg Message
	msg.InitializeFromRaw(entry.Value)
	return &msg, nil
}

// findFirstOffsetWithTimestampGE finds the first offset in the segment whose timestamp >= given ts. This method
// assumes that segLock has been acquired.
func (seg *KVStoreSegment) findFirstOffsetWithTimestampGE(ts int64) (base.Offset, error) {
	startOffsetHint := seg.getNearestSmallerOffsetFromIndex(ts)
	if startOffsetHint == -1 {
		return -1, ErrSegmentInvalidTimestamp
	}
	scanner, err := seg.dataDB.NewScanner(kOffsetColumnFamily, seg.offsetToKey(startOffsetHint), false)
	if err != nil {
		seg.logger.Errorf("Unable to initialize scanner due to err: %v", err)
		return -1, ErrSegmentBackend
	}
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
func (seg *KVStoreSegment) generateKeys(startOffset base.Offset, numMessages base.Offset) [][]byte {
	lastOffset := startOffset + numMessages
	var keys [][]byte
	for ii := startOffset; ii < lastOffset; ii++ {
		keys = append(keys, seg.offsetToKey(ii))
	}
	return keys
}

// offsetToKey converts the given offset to a key representation for dataDB.
func (seg *KVStoreSegment) offsetToKey(offset base.Offset) []byte {
	return util.UintToBytes(uint64(offset))
}

// keyToOffset converts the key representation of the offset(from dataDB) to base.Offset type.
func (seg *KVStoreSegment) keyToOffset(key []byte) base.Offset {
	if len(key) != 8 {
		seg.logger.Fatalf("Given key: %s is not an offset key", string(key))
	}
	return base.Offset(util.BytesToUint(key))
}

// numToIndexKey converts the given number to a key representation for dataDB.
func (seg *KVStoreSegment) numToIndexKey(num int) []byte {
	return util.UintToBytes(uint64(num))
}

// indexKeyToNum converts the given index key to its numeric representation.
func (seg *KVStoreSegment) indexKeyToNum(key []byte) int {
	if len(key) != 8 {
		seg.logger.Fatalf("Expected timestamp index key. Got: %s", string(key))
	}
	return int(util.BytesToUint(key))
}

// A helper method that lets us know if a key has started with the given prefix.
func (seg *KVStoreSegment) doesKeyStartWithPrefix(key []byte, prefix []byte) bool {
	if len(key) < len(prefix) {
		return false
	}
	if bytes.Compare(key[0:len(prefix)], prefix) != 0 {
		return false
	}
	return true
}

// hasValueExpired returns true if the value has expired. False otherwise.
func (seg *KVStoreSegment) hasValueExpired(tsNano int64, nowNano int64) bool {
	if (nowNano-tsNano)/(1e9) >= int64(seg.ttlSeconds) {
		return true
	}
	return false
}

// indexer is a background goroutine that is started when the segment is first opened. It scans dataDB and
// rebuilds an in memory timestamp index. It then waits for new indexes that were created and updates the
// in memory timestamp index.
func (seg *KVStoreSegment) indexer() {
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
func (seg *KVStoreSegment) notifyIndexer(entries []TimestampIndexEntry) {
	seg.timestampIndexChan <- entries
}

// updateTimestampIndex is called by the indexer when it receives an index entry on timestampIndexChan.
func (seg *KVStoreSegment) updateTimestampIndex(entries []TimestampIndexEntry) {
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
func (seg *KVStoreSegment) getNearestSmallerOffsetFromIndex(ts int64) base.Offset {
	seg.timestampIndexLock.RLock()
	defer seg.timestampIndexLock.RUnlock()
	if len(seg.timestampBRI) == 0 {
		// There are no indexes. Return the smallest offset.
		return seg.metadata.StartOffset
	}
	if ts > seg.timestampBRI[len(seg.timestampBRI)-1].GetTimestamp() {
		return seg.timestampBRI[len(seg.timestampBRI)-1].GetOffset()
	}

	// Binary search index to find the nearest offset lesser than the given timestamp.
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
func (seg *KVStoreSegment) rebuildIndexes() {
	seg.rebuildIndexOnce.Do(func() {
		seg.logger.VInfof(1, "Rebuilding segment indexes")
		startIdxKey := seg.numToIndexKey(0)
		_, err := seg.dataDB.Get(&kv_store.KVStoreKey{
			Key:          startIdxKey,
			ColumnFamily: kTimestampIndexColumnFamily,
		})
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
		itr, err := seg.dataDB.NewScanner(kTimestampIndexColumnFamily, nil, false)
		if err != nil {
			seg.logger.Fatalf("Unable to initialize scanner to rebuild indexes due to err: %v", err)
		}
		defer itr.Close()
		itr.Seek(startIdxKey)
		count := 0
		for ; itr.Valid(); itr.Next() {
			key, val, err := itr.GetItem()
			if err != nil {
				seg.logger.Fatalf("Unable to initialize index due to scan err: %s", err.Error())
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
func (seg *KVStoreSegment) open() {
	segEmpty := false
	// Initialize KV store.
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
	opts.LoadBloomsOnOpen = true
	if seg.metadata.Immutable {
		opts.ReadOnly = true
		opts.NumMemtables = 0
	}
	seg.dataDB = bkv.NewBadgerKVStore(path.Join(seg.rootDir, dataDirName), opts, seg.logger)

	// Create column families.
	cfs := []string{kOffsetColumnFamily, kTimestampIndexColumnFamily, kMiscColumnFamily}
	for _, cf := range cfs {
		err := seg.dataDB.AddColumnFamily(cf)
		if err == nil {
			continue
		}
		if err == kv_store.ErrKVStoreColumnFamilyExists {
			continue
		}
		seg.logger.Fatalf("Unable to add column family: %s due to err: %v", cf, err)
	}

	// Gather the last replicated log index in the segment.
	entry, err := seg.dataDB.Get(&kv_store.KVStoreKey{
		Key:          kLastRLogIdxKeyBytes,
		ColumnFamily: kMiscColumnFamily,
	})
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			// Segment is empty.
			seg.logger.Infof("Did not find any records in segment. Assuming it is empty!")
			segEmpty = true
			seg.nextOffset = int64(seg.metadata.StartOffset)
			seg.firstMsgTs = -1
			seg.lastMsgTs = -1
			seg.lastRLogIdx = -1
			seg.nextTimestampIndexKey = 0
		} else {
			seg.logger.Fatalf("Error while getting last log index. Error: %s", err.Error())
		}
	} else {
		seg.lastRLogIdx = int64(util.BytesToUint(entry.Value))
	}

	// Gather the next index key.
	entry, err = seg.dataDB.Get(&kv_store.KVStoreKey{
		Key:          kNextIndexKeyBytes,
		ColumnFamily: kMiscColumnFamily,
	})
	if err != nil {
		if err == kv_store.ErrKVStoreKeyNotFound {
			// Segment is empty.
			seg.nextTimestampIndexKey = 0
		} else {
			seg.logger.Fatalf("Error while getting last log index. Error: %s", err.Error())
		}
	} else {
		seg.nextTimestampIndexKey = int(util.BytesToUint(entry.Value))
	}

	// Start indexer.
	go seg.indexer()

	if segEmpty {
		// Segment is empty. Return early.
		return
	}

	// Gather first and last message timestamps by scanning in both forward and reverse directions. Also gather the
	// last offset appended and hence the nextOffset in the segment.
	dirs := []bool{false, true}
	for _, dir := range dirs {
		itr, err := seg.dataDB.NewScanner(kOffsetColumnFamily, nil, dir)
		if err != nil {
			seg.logger.Fatalf("Unable to create scanner due to err: %v", err)
		}
		for ; itr.Valid(); itr.Next() {
			key, item, err := itr.GetItem()
			if err != nil {
				seg.logger.Fatalf("Unable to initialize next offset, first and last message timestamps due to "+
					"scan err: %s", err.Error())
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
