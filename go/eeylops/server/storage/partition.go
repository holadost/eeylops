package storage

import (
	"context"
	"eeylops/server/base"
	storagebase "eeylops/server/storage/base"
	"eeylops/server/storage/segments"
	"eeylops/util"
	"eeylops/util/logging"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const KNumSegmentRecordsThreshold = 9.5e6 // 9.5 million
const KSegmentsDirectoryName = "segments"
const KMaxScanSizeBytes = 15 * 1024 * 1024 // 15MB
const KExpiredSegmentDirSuffix = "-expired"
const KSegmentSizeThreshold = 40 * (1024 * 1024 * 1024) // 40GB

const KSegmentCreationCountStrategy = 1
const KSegmentCreationSizeStrategy = 2
const KSegmentCreationCountAndSizeStrategy = 3

var (
	FlagExpiredSegmentMonitorIntervalSecs = flag.Int("partition_exp_segment_monitor_interval_seconds", 600,
		"Expired segment monitor interval seconds")
	FlagLiveSegmentMonitorIntervalSecs = flag.Int("partition_live_seg_monitor_interval_seconds", 5,
		"Live segment monitor interval seconds")
	FlagNumRecordsInSegment = flag.Int("partition_num_records_per_segment_threshold", KNumSegmentRecordsThreshold,
		"Number of records in a segment threshold")
	FlagMaxScanSizeBytes = flag.Int("partition_max_scan_size_bytes", KMaxScanSizeBytes,
		"Max scan size in bytes. Defaults to 15MB")
	FlagSegmentSizeThreshold = flag.Int64("segment_size_threshold_bytes", KSegmentSizeThreshold,
		"Segment size threshold. Defaults to 40GB")
	FlagSegmentCreationStrategy = flag.Int64("segment_creation_strategy", KSegmentCreationSizeStrategy,
		fmt.Sprintf("The strategy to use for segment creation. The supported options are: Count(%d), "+
			"Size(%d), Count and Size(%d)", KSegmentCreationCountStrategy, KSegmentCreationSizeStrategy,
			KSegmentCreationCountAndSizeStrategy))
)

type Partition struct {
	// Lock on the partition configuration. All operations on the partition need to acquire this lock.
	partitionCfgLock sync.RWMutex
	// Name of the topic
	topicName string
	// Partition ID.
	partitionID int
	// Root directory of the partition.
	rootDir string
	// TTL seconds for every record.
	ttlSeconds int
	// Current Start offset of the partition.
	currStartOffset base.Offset
	// Expired segment poll interval seconds.
	expiredSegmentPollIntervalSecs time.Duration
	// Live segment poll interval seconds.
	liveSegmentPollIntervalSecs time.Duration
	// Number of records per segment.
	numRecordsPerSegment int
	// Max scan size bytes.
	maxScanSizeBytes int
	// Segment size(in bytes) threshold.
	segmentSizeThresholdBytes int64
	// List of segments in the partition.
	segments []segments.Segment
	// Notification to ask background goroutines to exit.
	backgroundJobDone        chan bool
	partitionManagerDoneChan chan bool
	// Snapshot channel.
	snapshotChan chan bool
	// Disposer.
	disposer *StorageDisposer
	// Flag to indicate whether the partition is open/closed.
	closed bool
	// Partition logger
	logger *logging.PrefixLogger
}

type PartitionOpts struct {
	// Name of the topic. This is a compulsory parameter.
	TopicName string

	// The partition ID. This is a compulsory parameter.
	PartitionID int

	// Data directory where the partition data is stored. This is a compulsory parameter.
	RootDirectory string

	// The interval(in seconds) when segments are checked to see if they have expired and are disposed. This is an
	// optional parameter.
	ExpiredSegmentPollIntervalSecs int

	// The interval(in seconds) where the live segment is monitored to check if it has crossed the
	//NumRecordsPerSegmentThreshold. This is an optional parameter.
	LiveSegmentPollIntervalSecs int

	// The number of records per segment. This is an optional parameter.
	NumRecordsPerSegmentThreshold int

	// Segment size threshold
	SegmentSizeThreshold int64

	// Size threshold bytes.
	SizeThresholdBytes int64

	// Maximum scan size in bytes. This is an optional parameter.
	MaxScanSizeBytes int

	// TTL seconds for every record. This is an optional parameter. Defaults to -1. If -1, the data is never
	// reclaimed.
	TTLSeconds int
}

func NewPartition(opts PartitionOpts) *Partition {
	p := new(Partition)
	p.topicName = opts.TopicName
	if p.topicName == "" {
		glog.Fatalf("A topic name must be provided when initializing a partition")
	}
	p.partitionID = opts.PartitionID
	if p.partitionID <= 0 {
		glog.Fatalf("Partition ID must be defined. Partition ID must be > 0")
	}
	p.logger = logging.NewPrefixLoggerWithParent(fmt.Sprintf("Partition: %d", p.partitionID),
		logging.NewPrefixLogger(fmt.Sprintf("Topic: %s", p.topicName)))
	p.rootDir = opts.RootDirectory
	if p.rootDir == "" {
		p.logger.Fatalf("A data directory must be specified for topic: %s, partition: %d",
			p.topicName, p.partitionID)
	}
	if opts.TTLSeconds <= 0 {
		p.logger.Infof("TTL seconds <= 0. Setting TTL to -1 instead for topic: %s, partition: %d",
			p.topicName, p.partitionID)
		p.ttlSeconds = -1
	} else {
		p.ttlSeconds = opts.TTLSeconds
	}

	if opts.ExpiredSegmentPollIntervalSecs <= 0 {
		if *FlagExpiredSegmentMonitorIntervalSecs <= 0 {
			p.logger.Fatalf("Expired segment monitor interval must be > 0")
		}
		p.expiredSegmentPollIntervalSecs = time.Duration(*FlagExpiredSegmentMonitorIntervalSecs) * time.Second
	} else {
		p.expiredSegmentPollIntervalSecs = time.Duration(opts.ExpiredSegmentPollIntervalSecs) * time.Second
	}

	if opts.LiveSegmentPollIntervalSecs == 0 {
		if *FlagLiveSegmentMonitorIntervalSecs <= 0 {
			p.logger.Fatalf("Live segment monitor interval must be > 0")
		}
		p.liveSegmentPollIntervalSecs = time.Duration(*FlagLiveSegmentMonitorIntervalSecs) * time.Second
	} else {
		p.liveSegmentPollIntervalSecs = time.Duration(opts.LiveSegmentPollIntervalSecs) * time.Second
	}

	if opts.NumRecordsPerSegmentThreshold <= 0 {
		if *FlagNumRecordsInSegment <= 0 {
			p.logger.Fatalf("Number of records in the segment must be > 0")
		}
		p.numRecordsPerSegment = *FlagNumRecordsInSegment
	} else {
		p.numRecordsPerSegment = opts.NumRecordsPerSegmentThreshold
	}

	if opts.SegmentSizeThreshold <= 0 {
		if *FlagSegmentSizeThreshold <= 0 {
			p.logger.Fatalf("Segment size threshold must be > 0 bytes")
		}
		p.segmentSizeThresholdBytes = *FlagSegmentSizeThreshold
	} else {
		p.segmentSizeThresholdBytes = opts.SegmentSizeThreshold
	}

	if opts.MaxScanSizeBytes <= 0 {
		if *FlagMaxScanSizeBytes <= 0 {
			p.logger.Fatalf("Maximum scan size bytes must be > 0")
		}
		p.maxScanSizeBytes = *FlagMaxScanSizeBytes
	} else {
		p.maxScanSizeBytes = opts.MaxScanSizeBytes
	}

	if opts.SizeThresholdBytes <= 0 {
		if *FlagSegmentSizeThreshold <= 0 {
			p.logger.Fatalf("Segment size threshold not defined")
		}
		p.segmentSizeThresholdBytes = *FlagSegmentSizeThreshold
	} else {
		p.segmentSizeThresholdBytes = opts.SizeThresholdBytes
	}
	p.initialize()
	p.logger.Infof("Partition initialized. Partition Config:\n------------------------------------------------"+
		"\nTopic Name: %s\nPartition ID: %d\nData Directory: %s\nTTL Seconds: %d"+
		"\nNum Records Per Segment Threshold: %d\nMax Scan Size(bytes): %d"+
		"\nExpired Segment Monitor Interval: %v\nLive Segment Monitor Interval: %v"+
		"\n------------------------------------------------",
		p.topicName, p.partitionID, p.rootDir, p.ttlSeconds, p.numRecordsPerSegment, p.maxScanSizeBytes,
		p.expiredSegmentPollIntervalSecs, p.liveSegmentPollIntervalSecs)
	return p
}

// initialize the partition.
func (p *Partition) initialize() {
	p.backgroundJobDone = make(chan bool)
	p.snapshotChan = make(chan bool)
	p.partitionManagerDoneChan = make(chan bool, 1)
	p.disposer = DefaultDisposer()
	p.logger.Infof("Initializing partition: %d", p.partitionID)
	err := os.MkdirAll(p.getSegmentRootDirectory(), 0774)
	if err != nil {
		p.logger.Fatalf("Unable to create segment root directory due to err: %s", err.Error())
		return
	}

	// Initialize segments.
	segmentIds := p.getFileSystemSegments()
	for ii, segmentID := range segmentIds {
		opts := segments.KVStoreSegmentOpts{
			RootDir:     p.getSegmentDirectory(segmentID),
			Logger:      logging.NewPrefixLoggerWithParent(fmt.Sprintf("segment-%d", segmentID), p.logger),
			Topic:       p.topicName,
			PartitionID: uint(p.partitionID),
			TTLSeconds:  p.ttlSeconds,
		}
		segment, err := segments.NewKVStoreSegment(&opts)
		if err != nil {
			p.logger.Fatalf("Unable to initialize segment due to err: %s", err.Error())
			return
		}
		meta := segment.GetMetadata()
		if meta.ID != uint64(segmentID) {
			if ii != len(segmentIds)-1 {
				p.logger.Fatalf("Found segment with no metadata even though we have more "+
					"segments to initialize. Segment ID: %d", segmentID)
			}
			p.logger.Infof("Last segment: %d wasn't initialized properly in the previous incarnation. "+
				"Reinitializing now!", segmentID)
			metadata := segments.SegmentMetadata{
				ID:               uint64(segmentID),
				Immutable:        false,
				Expired:          false,
				CreatedTimestamp: time.Now(),
				StartOffset:      0, // Setting it to 0 in case the very first initialization failed.
			}
			if len(p.segments) != 0 {
				prevSeg := p.segments[len(p.segments)-1]
				metadata.StartOffset = prevSeg.GetMetadata().EndOffset + 1
			}
			segment.SetMetadata(metadata)
		}
		if meta.Expired {
			if len(p.segments) != 0 {
				p.logger.Fatalf("Found an expired segment in the middle of segments. Segment ID: %d",
					meta.ID)
			}
			continue
		}
		if ii < len(segmentIds)-1 {
			if !meta.Immutable {
				p.logger.Fatalf("Found segment at index: %d(%d), %s to be live", ii, len(segmentIds),
					meta.ToString())
			}
		}
		segment.Open()
		p.segments = append(p.segments, segment)
	}

	// There are no segments in the backing file system. This must be the first time that the partition is being
	// created.
	if len(p.segments) == 0 {
		p.logger.Infof("Did not find any segment in the backing store. Creating segment for first time")
		p.createNewSegmentSafe()
	}

	// The last segment can be live or immutable. If immutable, create a new segment.
	lastSeg := p.segments[len(p.segments)-1]
	lastMeta := lastSeg.GetMetadata()
	if lastMeta.Immutable {
		p.createNewSegmentSafe()
	}
	p.closed = false

	// Delete all expired segments from the file system.
	expiredSegDirs := p.getExpiredFileSystemSegments()
	createCB := func(segDir string) func(error) {
		return func(err error) {
			p.logger.Fatalf("Unable to delete seg dir: %s due to err: %s", segDir, err.Error())
		}
	}
	for _, segDir := range expiredSegDirs {
		p.disposer.Dispose(segDir, createCB(segDir))
	}

	// Start partition manager.
	go p.partitionManager()
}

// Append records to the partition.
func (p *Partition) Append(ctx context.Context, arg *storagebase.AppendEntriesArg) *storagebase.AppendEntriesRet {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	var ret storagebase.AppendEntriesRet
	if p.closed {
		ret.Error = ErrPartitionClosed
		return &ret
	}
	seg := p.getLiveSegment()
	var sarg segments.AppendEntriesArg
	sarg.Entries = arg.Entries
	sarg.RLogIdx = arg.RLogIdx
	sarg.Timestamp = arg.Timestamp
	segRet := seg.Append(ctx, &sarg)
	if segRet.Error != nil {
		p.logger.Errorf("Unable to append entries to partition due to err: %s", ret.Error.Error())
		if segRet.Error == segments.ErrSegmentInvalidRLogIdx {
			ret.Error = ErrInvalidRLogIdx
		} else {
			ret.Error = ErrPartitionAppend
		}
		return &ret
	}
	return &ret
}

// Scan numMessages records from the partition from the given startOffset.
func (p *Partition) Scan(ctx context.Context, arg *storagebase.ScanEntriesArg) *storagebase.ScanEntriesRet {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	var ret storagebase.ScanEntriesRet
	if p.closed {
		ret.Error = ErrPartitionClosed
		return &ret
	}

	if arg.StartOffset >= 0 {
		// Do offset based scans.
		p.offsetScan(ctx, arg, &ret)
		return &ret
	} else {
		// Do timestamp based scans.
		p.timestampScan(ctx, arg, &ret)
		return &ret
	}
}

// Snapshot the partition.
func (p *Partition) Snapshot() error {
	if p.closed {
		return ErrPartitionClosed
	}
	return nil
}

// Close the partition.
func (p *Partition) Close() {
	p.partitionCfgLock.Lock()
	defer p.partitionCfgLock.Unlock()
	if p.closed {
		return
	}
	p.logger.Infof("Closing partition")
	p.closed = true
	close(p.backgroundJobDone)
	for _, seg := range p.segments {
		meta := seg.GetMetadata()
		err := seg.Close()
		if err != nil {
			p.logger.Fatalf("Failed to close segment: %d due to err: %s", meta.ID, err.Error())
		}
	}
}

// offsetScan is a helper method of scan that does offset based scans.
func (p *Partition) offsetScan(ctx context.Context, arg *storagebase.ScanEntriesArg, ret *storagebase.ScanEntriesRet) {
	var startOffset base.Offset
	var endOffset base.Offset
	numMsgsOffset := base.Offset(arg.NumMessages)
	// Offset scans.
	endOffset = arg.StartOffset + numMsgsOffset - 1
	startOffset = arg.StartOffset
	fOffset, _ := p.segments[0].GetRange()
	if arg.StartOffset < fOffset {
		ret.NextOffset = fOffset
		ret.Error = nil
		return
	}
	startIdx, endIdx := p.getSegmentsByOffset(startOffset, endOffset)
	if startIdx == -1 {
		// We didn't find any segments. The scan is complete.
		ret.NextOffset = -1
		ret.Error = nil
		return
	}
	p.scanMessages(ctx, arg, ret, true, arg.EndTimestamp, startIdx, endIdx)
}

// timestampScan is a helper method of scan that does timestamp based scans.
func (p *Partition) timestampScan(ctx context.Context, arg *storagebase.ScanEntriesArg, ret *storagebase.ScanEntriesRet) {
	if arg.StartTimestamp <= 0 {
		p.logger.Errorf("Invalid inputs. Either timestamp/offset must be provided")
		ret.Error = ErrPartitionScan
		ret.NextOffset = -1
		return
	}
	if arg.EndTimestamp > 0 {
		if arg.StartTimestamp >= arg.EndTimestamp {
			p.logger.Errorf("Invalid timestamps. Chosen Start TS: %d, Chosen End TS: %d",
				arg.StartTimestamp, arg.EndTimestamp)
			ret.Error = ErrPartitionScan
			ret.NextOffset = -1
			return
		}
	}
	now := time.Now().UnixNano()
	startTs := arg.StartTimestamp
	firstSegTs, _ := p.segments[0].GetMsgTimestampRange()
	startTsHint := util.MaxInt(now-(int64(p.ttlSeconds)*(1e9)), firstSegTs)
	if startTs < startTsHint {
		startTs = startTsHint
	}
	endTs := now
	if arg.EndTimestamp > 0 {
		endTs = arg.EndTimestamp
	}
	if startTs >= endTs {
		// This could have happened if the user explicitly asked for messages with an end timestamp, but we found
		// that the first unexpired message in the partition has a timestamp >= the end timestamp.
		ret.Error = nil
		ret.NextOffset = -1
		return
	}
	startIdx, endIdx := p.getSegmentsByTimestamp(startTs, endTs)
	if startIdx == -1 {
		// We didn't find any segments. The scan is complete.
		ret.NextOffset = -1
		ret.Error = nil
		return
	}
	p.scanMessages(ctx, arg, ret, false, endTs, startIdx, endIdx)
}

// scanMessages scans messages based on the given arg and populates the results in the given ret.
func (p *Partition) scanMessages(ctx context.Context, arg *storagebase.ScanEntriesArg, ret *storagebase.ScanEntriesRet,
	offsetType bool, endTs int64, startIdx, endIdx int) {
	var scanSizeBytes int
	scanSizeBytes = 0
	numPendingMsgs := arg.NumMessages
	count := -1
	for ii := startIdx; ii <= endIdx; ii++ {
		count++
		seg := p.segments[ii]
		if numPendingMsgs == 0 {
			break
		}
		var sarg segments.ScanEntriesArg
		if offsetType {
			sarg.StartTimestamp = -1 // Unset timestamp type.
			if count == 0 {
				sarg.StartOffset = arg.StartOffset
			} else {
				sOff, _ := seg.GetRange()
				sarg.StartOffset = sOff
			}
		} else {
			sarg.StartOffset = -1 // Unset offset type.
			if count == 0 {
				sarg.StartTimestamp = arg.StartTimestamp
			} else {
				sts, _ := seg.GetMsgTimestampRange()
				sarg.StartTimestamp = sts
			}
		}
		sarg.EndTimestamp = endTs
		sarg.NumMessages = numPendingMsgs
		sarg.ScanSizeBytes = int64(p.maxScanSizeBytes - scanSizeBytes)
		sret := seg.Scan(ctx, &sarg)
		if sret.Error != nil {
			ret.Error = ErrPartitionScan
			ret.NextOffset = -1
			return
		}
		numPendingMsgs -= uint64(len(sret.Values))
		for _, val := range sret.Values {
			// Ensure that the batch size remains smaller than the max scan size.
			if (len(val.Value) + scanSizeBytes) > p.maxScanSizeBytes {
				ret.Error = nil
				ret.NextOffset = val.Offset // As this message is not being included.
				return
			}
			scanSizeBytes += len(val.Value)
			// Merge all partial values into a single list.
			ret.Values = append(ret.Values, val)
		}
	}
	ret.Error = nil
	ret.NextOffset = ret.Values[len(ret.Values)-1].Offset + 1
	return
}

// getSegmentsByOffset returns a list of segments that contains all the elements between the given Start and end offsets.
// This function assumes that a partitionCfgLock has been acquired.
func (p *Partition) getSegmentsByOffset(startOffset base.Offset, endOffset base.Offset) (int, int) {
	// Find Start offset segment.
	startSegIdx := p.findSegmentIdxWithOffset(startOffset)
	if startSegIdx == -1 {
		p.logger.Infof("Did not find any segment with offset: %d", startOffset)
		// We did not find any segments that contains our offsets.
		return -1, -1
	}

	// Find the end offset segment. Finding the end offset is split into two paths: fast and slow.
	// Fast Path: For the most part, the endIdx is going to be in the Start or the next couple of
	// segments right after Start. Check these segments first and if not present, fall back to
	// scanning all the segments.
	endSegIdx := -1
	for ii := startSegIdx; ii < startSegIdx+3; ii++ {
		if ii >= len(p.segments) {
			break
		}
		if p.offsetInSegment(endOffset, p.segments[ii]) {
			endSegIdx = ii
			break
		}
	}
	// Slow path.
	if endSegIdx == -1 {
		endSegIdx = p.findSegmentIdxWithOffset(endOffset)
	}
	if endSegIdx == -1 {
		endSegIdx = len(p.segments) - 1
	}
	return startSegIdx, endSegIdx
}

// getSegmentsByTimestamp returns a list of segments that contains all the elements between the given Start and end
// offsets. This function assumes that a partitionCfgLock has been acquired.
func (p *Partition) getSegmentsByTimestamp(startTs int64, endTs int64) (int, int) {
	// Find Start offset segment.
	startSegIdx := p.findSegmentIdxWithTimestamp(startTs)
	if startSegIdx == -1 {
		p.logger.Infof("Did not find any segment with ts: %d", startTs)
		return -1, -1
	}

	// Fast path. Look at getSegmentsByOffset for more info.
	endSegIdx := -1
	for ii := startSegIdx; ii < startSegIdx+3; ii++ {
		if ii >= len(p.segments) {
			break
		}
		if p.timestampInSegment(endTs, p.segments[ii]) {
			endSegIdx = ii
			break
		}
	}
	// Slow path.
	if endSegIdx == -1 {
		endSegIdx = p.findSegmentIdxWithTimestamp(endTs)
	}
	if endSegIdx == -1 {
		endSegIdx = len(p.segments) - 1
	}
	// Populate segments.
	return startSegIdx, endSegIdx
}

// getLiveSegment returns the current live segment. This function assumes that the partitionCfgLock has been acquired.
func (p *Partition) getLiveSegment() segments.Segment {
	return p.segments[len(p.segments)-1]
}

// findSegmentIdxWithOffset finds the segment index that contains the given offset. This function assumes the
// partitionCfgLock has been acquired.
func (p *Partition) findSegmentIdxWithOffset(offset base.Offset) int {
	offsetCmp := func(idx int) int {
		sOff, _ := p.segments[idx].GetRange()
		if p.offsetInSegment(offset, p.segments[idx]) {
			return 0
		} else if offset < sOff {
			return -1
		} else {
			return 1
		}
	}
	return p.binarySearchSegments(0, len(p.segments)-1, offsetCmp)
}

// findSegmentIdxWithTimestamp finds the segment index that contains the given offset. This method assumes the
// partitionCfgLock has been acquired.
func (p *Partition) findSegmentIdxWithTimestamp(timestamp int64) int {
	timestampCmp := func(idx int) int {
		sts, _ := p.segments[idx].GetMsgTimestampRange()
		if p.timestampInSegment(timestamp, p.segments[idx]) {
			return 0
		} else if timestamp < sts {
			return -1
		} else {
			return 1
		}
	}
	return p.binarySearchSegments(0, len(p.segments)-1, timestampCmp)
}

// binarySearchSegments searches segments using the cmp function. The cmp function should return 0, if a match
// was found with the given segments idx, -1 if it is lesser and 1 if it is greater.
func (p *Partition) binarySearchSegments(startIdx, endIdx int, cmp func(int) int) int {
	if startIdx > endIdx {
		return -1
	}
	midIdx := startIdx + (endIdx-startIdx)/2
	res := cmp(midIdx)
	if res == 0 {
		return midIdx
	} else if res == -1 {
		return p.binarySearchSegments(startIdx, midIdx-1, cmp)
	} else {
		return p.binarySearchSegments(midIdx+1, endIdx, cmp)
	}
}

// findSegmentIdxByID returns the index in segments whose segment ID is segID.
func (p *Partition) findSegmentIdxByID(segID int) int {
	fsegID := p.segments[0].ID()
	if segID < fsegID {
		return -1
	}
	idx := segID - fsegID
	if idx >= len(p.segments) {
		return -1
	}
	return idx
}

// offsetInSegment checks whether the given offset is in the segment or not.
func (p *Partition) offsetInSegment(offset base.Offset, seg segments.Segment) bool {
	if seg.IsEmpty() {
		return false
	}
	sOff, eOff := seg.GetRange()
	if offset >= sOff && offset <= eOff {
		return true
	}
	return false
}

// timestampInSegment checks whether the given timestamp is in the segment or not.
func (p *Partition) timestampInSegment(timestamp int64, seg segments.Segment) bool {
	if seg.IsEmpty() {
		return false
	}
	sts, lts := seg.GetMsgTimestampRange()
	if timestamp >= sts && timestamp <= lts {
		return true
	}
	return false
}

/******************************************* PARTITION MANAGER ************************************************/
// partitionManager is a long running background routine that performs the following operations:
//     1. Checks the current live segment and if it has hit a threshold, it creates a new segment.
//     2. Checks the segments that have expired and marks them for GC. GC is handled by another background goroutine.
//     3. Checks the snapshotChan and saves the partition configuration when a snapshot is requested.
func (p *Partition) partitionManager() {
	p.logger.Infof("Partition manager has started")
	liveSegTicker := time.NewTicker(p.liveSegmentPollIntervalSecs)
	expTicker := time.NewTicker(p.expiredSegmentPollIntervalSecs)
	for {
		select {
		case <-p.backgroundJobDone:
			p.logger.Infof("Partition manager exiting")
			return
		case <-liveSegTicker.C:
			p.maybeCreateNewSegment()
		case <-expTicker.C:
			p.maybeReclaimExpiredSegments()
		case <-p.snapshotChan:
			p.snapshot()
		}
	}
}

// maybeCreateNewSegment checks if a new segment needs to be created and if so, creates one.
func (p *Partition) maybeCreateNewSegment() {
	p.logger.VInfof(1, "Checking if new segment needs to be created")
	if p.shouldCreateNewSegment() {
		p.createNewSegmentSafe()
	}
}

// shouldCreateNewSegment returns true if a new segment is required. false otherwise.
func (p *Partition) shouldCreateNewSegment() bool {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	if p.closed {
		return false
	}
	countStrategy := func(seg segments.Segment) bool {
		metadata := seg.GetMetadata()
		numRecords := metadata.EndOffset - metadata.StartOffset + 1
		if numRecords >= base.Offset(p.numRecordsPerSegment) {
			p.logger.Infof("Current live segment records(%d) has reached/exceeded threshold(%d). "+
				"New segment is required", numRecords, p.numRecordsPerSegment)
			return true
		}
		return false
	}
	sizeStrategy := func(seg segments.Segment) bool {
		size := seg.Size()
		if seg.Size() >= p.segmentSizeThresholdBytes {
			p.logger.Infof("Current live segment size(%d) has reached/exceeded threshold(%d). "+
				"New segment is required", size, p.segmentSizeThresholdBytes)
			return true
		}
		return false
	}
	seg := p.segments[len(p.segments)-1]
	switch *FlagSegmentCreationStrategy {
	case KSegmentCreationCountStrategy:
		return countStrategy(seg)
	case KSegmentCreationSizeStrategy:
		return sizeStrategy(seg)
	case KSegmentCreationCountAndSizeStrategy:
		if countStrategy(seg) {
			return true
		}
		if sizeStrategy(seg) {
			return true
		}
		return false
	default:
		p.logger.Fatalf("Invalid segment creation strategy: %d", *FlagSegmentCreationStrategy)
		return false
	}
}

// createNewSegmentSafe creates a new segment after acquiring the partitionCfgLock.
func (p *Partition) createNewSegmentSafe() {
	p.partitionCfgLock.Lock()
	defer p.partitionCfgLock.Unlock()
	if p.closed {
		return
	}
	p.createNewSegmentUnsafe()
}

// createNewSegmentUnsafe marks the current live segment immutable and creates a new live segment. This method assumes
// that partitionCfgLock has been acquired in write mode as the segments slice will be modified.
func (p *Partition) createNewSegmentUnsafe() {
	p.logger.Infof("Creating new segment")
	var startOffset base.Offset
	var segID uint64
	// A flag to indicate whether the current last segment is being marked as immutable.
	isImmutizing := false
	wg := sync.WaitGroup{}
	// This helper function marks the current last segment as immutable.
	immutize := func() {
		seg := p.segments[len(p.segments)-1]
		segmentID := seg.ID()
		p.logger.VInfof(1, "Marking segment: %d as immutable", segmentID)
		seg.MarkImmutable()
		err := seg.Close()
		if err != nil {
			p.logger.Fatalf("Failure while closing last segment: %d in partition %d due to err: %s",
				segmentID, p.partitionID, err.Error())
			return
		}
		opts := segments.KVStoreSegmentOpts{
			RootDir:     p.getSegmentDirectory(segmentID),
			Logger:      logging.NewPrefixLoggerWithParent(fmt.Sprintf("Segment: %d", segmentID), p.logger),
			Topic:       p.topicName,
			PartitionID: uint(p.partitionID),
			TTLSeconds:  p.ttlSeconds,
		}
		seg, err = segments.NewKVStoreSegment(&opts)
		if err != nil {
			p.logger.Fatalf("Failure while closing and reopening last segment due to err: %s",
				err.Error())
		}
		seg.Open()
		p.segments[len(p.segments)-1] = seg
		wg.Done()
	}
	if len(p.segments) == 0 {
		// First ever segment in the partition.
		p.logger.Infof("No segments found. Creating brand new segment starting at offset: 0")
		startOffset = 0
		segID = 1
	} else {
		seg := p.segments[len(p.segments)-1]
		prevMetadata := seg.GetMetadata()
		isImmutizing = true
		wg.Add(1)
		go immutize()
		segID = prevMetadata.ID + 1
		startOffset = prevMetadata.EndOffset + 1
	}

	opts := segments.KVStoreSegmentOpts{
		RootDir:     p.getSegmentDirectory(int(segID)),
		Logger:      logging.NewPrefixLoggerWithParent(fmt.Sprintf("Segment: %d", segID), p.logger),
		Topic:       p.topicName,
		PartitionID: uint(p.partitionID),
		TTLSeconds:  p.ttlSeconds,
	}
	metadata := segments.SegmentMetadata{
		ID:               segID,
		Immutable:        false,
		Expired:          false,
		CreatedTimestamp: time.Now(),
		StartOffset:      startOffset,
	}
	newSeg, err := segments.NewKVStoreSegmentWithMetadata(&opts, metadata)
	if err != nil {
		p.logger.Fatalf("Unable to create new segment due to err: %s", err.Error())
		return
	}
	newSeg.Open()
	if isImmutizing {
		wg.Wait()
	}
	p.segments = append(p.segments, newSeg)
}

// maybeReclaimExpiredSegments checks all the segments and marks the out of date segments as expired.
// It also marks these segments for deletion after ensuring that the segment is not required by any
// snapshots.
func (p *Partition) maybeReclaimExpiredSegments() {
	p.logger.VInfof(1, "Checking if segments need to be expired")
	if p.shouldExpireSegment() {
		p.expireSegments()
	}
}

// shouldExpireSegment returns true if segments need to be expired. false otherwise
func (p *Partition) shouldExpireSegment() bool {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	if p.closed {
		return false
	}
	seg := p.segments[0]
	metadata := seg.GetMetadata()
	if !p.isSegExpirable(&metadata) {
		return false
	}
	return true
}

// expireSegments expires all the required segments.
func (p *Partition) expireSegments() {
	expiredSegs := p.clearExpiredSegsAndMaybeCreateNewSeg()

	// The expired segs have been removed from segments. We can now safely acquire the RLock and delete the
	// segments
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	for _, seg := range expiredSegs {
		segId := seg.ID()
		seg.MarkExpired()
		err := seg.Close()
		if err != nil {
			p.logger.Fatalf("Unable to close segment due to err: %s", err.Error())
		}
		segDir := p.getSegmentDirectory(segId)
		expiredSegDirName := filepath.Base(segDir) + KExpiredSegmentDirSuffix
		expiredSegDir := path.Join(filepath.Dir(segDir), expiredSegDirName)
		p.logger.Infof("Renaming segment directory for segment: %d to %s", segId, expiredSegDir)
		err = os.Rename(segDir, expiredSegDir)
		if err != nil {
			p.logger.Fatalf("Unable to rename segment directory as expired due to err: %s",
				err.Error())
		}
		p.disposer.Dispose(expiredSegDir, func(err error) {
			if err != nil {
				p.logger.Fatalf("Unable to delete segment: %d in partition: [%s:%d] due to err: %s",
					segId, p.topicName, p.partitionID, err.Error())
			}
		})
	}
}

// clearExpiredSegsAndMaybeCreateNewSeg removes the expired segment(s) from segments.
func (p *Partition) clearExpiredSegsAndMaybeCreateNewSeg() []segments.Segment {
	// Acquire write lock on partition as we are going to be changing segments.
	p.partitionCfgLock.Lock()
	defer p.partitionCfgLock.Unlock()
	if p.closed {
		return nil
	}

	lastIdxExpired := -1
	for ii, seg := range p.segments {
		metadata := seg.GetMetadata()
		if !p.isSegExpirable(&metadata) {
			break
		}
		lastIdxExpired = ii
	}
	var expiredSegs []segments.Segment
	if lastIdxExpired == len(p.segments)-1 {
		// All segments have expired. Create a new segment before removing all expired segments.
		// We use the unsafe new segment creation method since we have already acquired the lock.
		p.createNewSegmentUnsafe()
	}
	expiredSegs, p.segments = p.segments[0:lastIdxExpired+1], p.segments[lastIdxExpired+1:]
	return expiredSegs
}

// isSegExpirable returns true if the segment can be expired. False otherwise.
func (p *Partition) isSegExpirable(metadata *segments.SegmentMetadata) bool {
	now := time.Now().Unix()
	its := metadata.ImmutableTimestamp.Unix()
	lt := now - its
	if !metadata.Immutable {
		return false
	}
	if lt <= int64(p.ttlSeconds) {
		p.logger.VInfof(1, "Segment: %d has not expired. Current life time: %d, TTL: %d seconds",
			metadata.ID, lt, p.ttlSeconds)
		return false
	}
	p.logger.VInfof(1, "Segment: %d can be expired. Current life time: %d seconds, TTL: %d seconds",
		metadata.ID, lt, p.ttlSeconds)
	return true
}

// snapshot saves the current partition configuration.
func (p *Partition) snapshot() {

}

/************************************* Helper methods ************************************************/
// getPartitionDirectory returns the root partition directory.
func (p *Partition) getPartitionDirectory() string {
	return path.Join(p.rootDir, strconv.Itoa(p.partitionID))
}

// getSegmentRootDirectory returns the segments root directory.
func (p *Partition) getSegmentRootDirectory() string {
	return path.Join(p.getPartitionDirectory(), KSegmentsDirectoryName)
}

// getSegmentDirectory returns the segment directory for a given segmentID.
func (p *Partition) getSegmentDirectory(segmentID int) string {
	return path.Join(p.getSegmentRootDirectory(), strconv.Itoa(segmentID))
}

// getFileSystemSegments returns all the segments that are currently present in the backing file system.
func (p *Partition) getFileSystemSegments() []int {
	fileInfo, err := ioutil.ReadDir(p.getSegmentRootDirectory())
	if err != nil {
		p.logger.Fatalf("Unable to read partition directory and find segments due to err: %v", err)
		return nil
	}
	var segmentIDs []int
	for _, file := range fileInfo {
		if file.IsDir() {
			if strings.HasSuffix(file.Name(), KExpiredSegmentDirSuffix) {
				continue
			}
			segmentID, err := strconv.Atoi(file.Name())
			if err != nil {
				p.logger.Fatalf("Unable to convert segment id to int due to err: %v", err)
				return nil
			}
			segmentIDs = append(segmentIDs, segmentID)
		}
	}
	sort.Ints(segmentIDs)
	if segmentIDs == nil || len(segmentIDs) == 0 || len(segmentIDs) == 1 {
		return segmentIDs
	}
	// Sanity check the segments to make sure that there are no holes.
	for ii := 1; ii < len(segmentIDs); ii++ {
		id := segmentIDs[ii]
		prevID := segmentIDs[ii-1]
		if prevID+1 != id {
			p.logger.Fatalf("Found hole in segments. Prev Seg ID: %d, Curr Seg ID: %d", prevID, id)
		}
	}
	return segmentIDs
}

// getExpiredFileSystemSegments returns all the segments that have the expired suffix in the segment directory name.
// It is possible to have such directories if the directories were renamed but were not deleted.
func (p *Partition) getExpiredFileSystemSegments() []string {
	segRootDir := p.getSegmentRootDirectory()
	fileInfo, err := ioutil.ReadDir(segRootDir)
	if err != nil {
		p.logger.Fatalf("Unable to read partition directory and find segments due to err: %v", err)
		return nil
	}
	var expiredSegs []string
	for _, file := range fileInfo {
		if file.IsDir() {
			if strings.HasSuffix(file.Name(), KExpiredSegmentDirSuffix) {
				expiredSegs = append(expiredSegs, path.Join(segRootDir, file.Name()))
			}
		}
	}
	return expiredSegs
}
