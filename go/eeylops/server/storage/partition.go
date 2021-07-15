package storage

import (
	"eeylops/server/base"
	segments2 "eeylops/server/storage/segments"
	"flag"
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
const KMaxScanSizeBytes = 15 * 1000000 // 15MB
const KExpiredSegmentDirSuffix = "-expired"

var (
	FlagExpiredSegmentMonitorIntervalSecs = flag.Int("partition_exp_segment_monitor_interval_seconds", 600,
		"Expired segment monitor interval seconds")
	FlagLiveSegmentMonitorIntervalSecs = flag.Int("partition_live_seg_monitor_interval_seconds", 5,
		"Live segment monitor interval seconds")
	FlagNumRecordsInSegment = flag.Int("partition_num_records_per_segment_threshold", KNumSegmentRecordsThreshold,
		"Number of records in a segment threshold")
	FlagMaxScanSizeBytes = flag.Int("partition_max_scan_size_bytes", KMaxScanSizeBytes,
		"Max scan size in bytes. Defaults to 15MB")
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
	// Current start offset of the partition.
	currStartOffset base.Offset
	// Expired segment poll interval seconds.
	expiredSegmentPollIntervalSecs time.Duration
	// Live segment poll interval seconds.
	liveSegmentPollIntervalSecs time.Duration
	// Number of records per segment.
	numRecordsPerSegment int
	// Max scan size bytes.
	maxScanSizeBytes int
	// List of segments in the partition.
	segments []segments2.Segment
	// Notification to ask background goroutines to exit.
	backgroundJobDone        chan bool
	partitionManagerDoneChan chan bool
	// Snapshot channel.
	snapshotChan chan bool
	// Disposer.
	disposer *StorageDisposer
	// Flag to indicate whether the partition is open/closed.
	closed bool
	// Log ID string.
	logIDStr string
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
	p.rootDir = opts.RootDirectory
	if p.rootDir == "" {
		glog.Fatalf("A data directory must be specified for topic: %s, partition: %d",
			p.topicName, p.partitionID)
	}
	if opts.TTLSeconds <= 0 {
		glog.Infof("TTL seconds <= 0. Setting TTL to -1 instead for topic: %s, partition: %d",
			p.topicName, p.partitionID)
		p.ttlSeconds = -1
	} else {
		p.ttlSeconds = opts.TTLSeconds
	}

	if opts.ExpiredSegmentPollIntervalSecs <= 0 {
		if *FlagExpiredSegmentMonitorIntervalSecs <= 0 {
			glog.Fatalf("Expired segment monitor interval must be > 0")
		}
		p.expiredSegmentPollIntervalSecs = time.Duration(*FlagExpiredSegmentMonitorIntervalSecs) * time.Second
	} else {
		p.expiredSegmentPollIntervalSecs = time.Duration(opts.ExpiredSegmentPollIntervalSecs) * time.Second
	}

	if opts.LiveSegmentPollIntervalSecs == 0 {
		if *FlagLiveSegmentMonitorIntervalSecs <= 0 {
			glog.Fatalf("Live segment monitor interval must be > 0")
		}
		p.liveSegmentPollIntervalSecs = time.Duration(*FlagLiveSegmentMonitorIntervalSecs) * time.Second
	} else {
		p.liveSegmentPollIntervalSecs = time.Duration(opts.LiveSegmentPollIntervalSecs) * time.Second
	}

	if opts.NumRecordsPerSegmentThreshold <= 0 {
		if *FlagNumRecordsInSegment <= 0 {
			glog.Fatalf("Number of records in the segment must be > 0")
		}
		p.numRecordsPerSegment = *FlagNumRecordsInSegment
	} else {
		p.numRecordsPerSegment = opts.NumRecordsPerSegmentThreshold
	}

	if opts.MaxScanSizeBytes <= 0 {
		if *FlagMaxScanSizeBytes <= 0 {
			glog.Fatalf("Maximum scan size bytes must be > 0")
		}
		p.maxScanSizeBytes = *FlagMaxScanSizeBytes
	} else {
		p.maxScanSizeBytes = opts.MaxScanSizeBytes
	}
	p.logIDStr = "[" + p.topicName + ":" + strconv.Itoa(p.partitionID) + "]"
	p.initialize()
	glog.Infof("Partition initialized. Partition Config:\n------------------------------------------------"+
		"\nTopic Name: %s\nPartition ID: %d\nData Directory: %s\nTTL Seconds: %d"+
		"\nNum Records Per Segment Threshold: %d\nMax Scan Size(bytes): %d"+
		"\nExpired Segment Monitor Interval: %v\nLive Segment Monitor Interval: %v"+
		"\n------------------------------------------------",
		p.topicName, p.partitionID, p.rootDir, p.ttlSeconds, p.numRecordsPerSegment, p.maxScanSizeBytes,
		p.expiredSegmentPollIntervalSecs, p.liveSegmentPollIntervalSecs)
	return p
}

func (p *Partition) initialize() {
	p.backgroundJobDone = make(chan bool)
	p.snapshotChan = make(chan bool)
	p.partitionManagerDoneChan = make(chan bool, 1)
	p.disposer = DefaultDisposer()
	glog.Infof("Initializing partition: %d", p.partitionID)
	err := os.MkdirAll(p.getSegmentRootDirectory(), 0774)
	if err != nil {
		glog.Fatalf("Unable to create segment root directory due to err: %s", err.Error())
		return
	}

	// Initialize segments.
	segmentIds := p.getFileSystemSegments()
	for ii, segmentID := range segmentIds {
		// TODO: In the future, create a factory func here that will return segment based on type.
		segment, err := segments2.NewBadgerSegment(p.getSegmentDirectory(segmentID))
		if err != nil {
			glog.Fatalf("%s Unable to initialize segment due to err: %s", p.logIDStr, err.Error())
			return
		}
		meta := segment.GetMetadata()
		if meta.Expired {
			if len(p.segments) != 0 {
				glog.Fatalf("%s Found an expired segment in the middle of segments. Segment ID: %d",
					p.logIDStr, meta.ID)
			}
			continue
		}
		if ii < len(segmentIds)-1 {
			if !meta.Immutable {
				glog.Fatalf("%s Found segment at index: %d(%d), %s to be live", p.logIDStr, ii, len(segmentIds),
					meta.ToString())
			}
		}
		p.segments = append(p.segments, segment)
	}

	// There are no segments in the backing file system. This must be the first time that the partition is being
	// created.
	if len(p.segments) == 0 {
		glog.Infof("%s Did not find any segment in the backing store. Creating segment for first time",
			p.logIDStr)
		p.createNewSegmentWithLock()
	}

	// The last segment can be live or immutable. If immutable, create a new segment.
	lastSeg := p.segments[len(p.segments)-1]
	lastMeta := lastSeg.GetMetadata()
	if lastMeta.Immutable {
		p.createNewSegmentWithLock()
	}
	p.closed = false

	// Delete all expired segments from the file system.
	expiredSegDirs := p.getExpiredFileSystemSegments()
	createCB := func(segDir string) func(error) {
		return func(err error) {
			glog.Fatalf("%s Unable to delete seg dir: %s due to err: %s", p.logIDStr, segDir, err.Error())
		}
	}
	for _, segDir := range expiredSegDirs {
		p.disposer.Dispose(segDir, createCB(segDir))
	}

	// Start partition manager.
	go p.partitionManager()
}

// Append records to the partition.
func (p *Partition) Append(values [][]byte) error {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	if p.closed {
		return ErrPartitionClosed
	}
	seg := p.getLiveSegment()
	err := seg.Append(values, -1)
	if err != nil {
		glog.Errorf("%s Unable to append entries to partition: %d due to err: %s", p.logIDStr, p.partitionID,
			err.Error())
		return ErrPartitionAppend
	}
	return nil
}

func (p *Partition) AppendV2(values [][]byte, tsNano int64, lastRLogIdx int64) error {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	if p.closed {
		return ErrPartitionClosed
	}
	seg := p.getLiveSegment()
	err := seg.Append(segments2.makeMessageValues(values, tsNano), lastRLogIdx)
	if err != nil {
		glog.Errorf("%s Unable to append entries to partition: %d due to err: %s",
			p.logIDStr, p.partitionID, err.Error())
		return ErrPartitionAppend
	}
	return nil
}

// Scan numMessages records from the partition from the given startOffset.
func (p *Partition) Scan(startOffset base.Offset, numMessages uint64) (values [][]byte, errs []error) {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	if p.closed {
		return nil, []error{ErrPartitionClosed}
	}
	numMsgsOffset := base.Offset(numMessages)
	endOffset := startOffset + numMsgsOffset - 1

	// Get all the segments that contain our desired offsets.
	segs := p.getSegments(startOffset, endOffset)
	if segs == nil || len(segs) == 0 {
		return
	}
	var scanSizeBytes int
	scanSizeBytes = 0

	// All offsets are present in the same segment.
	if len(segs) == 1 {
		segStartOffset := startOffset - segs[0].GetMetadata().StartOffset
		tmpVals, tmpErrs := segs[0].Scan(segStartOffset, numMessages)
		for ii, val := range tmpVals {
			// Ensure that the batch size remains smaller than the max scan size.
			if (len(val) + scanSizeBytes) > p.maxScanSizeBytes {
				break
			}
			scanSizeBytes += len(val)
			values = append(values, val)
			if tmpErrs[ii] != nil {
				errs = append(errs, ErrPartitionScan)
			} else {
				errs = append(errs, nil)
			}
		}
		return
	}

	// The values are present across multiple segments. Merge them before returning.
	glog.V(1).Infof("%s Gathering values from multiple(%d) segments", p.logIDStr, len(segs))
	numPendingMsgs := numMessages
	var nextStartOffset base.Offset
	for ii, seg := range segs {
		if numPendingMsgs == 0 {
			return
		}
		if ii == 0 {
			// First segment. Start scanning from the correct offset into the segment.
			nextStartOffset = startOffset - seg.GetMetadata().StartOffset
		} else {
			// Start scanning from the very first offset in the segment.
			nextStartOffset = 0
		}
		partialValues, partialErrs := seg.Scan(nextStartOffset, numPendingMsgs)
		numPendingMsgs -= uint64(len(partialValues))
		for jj, val := range partialValues {
			// Ensure that the batch size remains smaller than the max scan size.
			if (len(val) + scanSizeBytes) > p.maxScanSizeBytes {
				return
			}
			scanSizeBytes += len(val)

			// Merge all partial values into a single list.
			values = append(values, val)
			if partialErrs[jj] != nil {
				errs = append(errs, ErrPartitionScan)
			} else {
				errs = append(errs, nil)
			}
		}
	}
	return
}

// ScanV2 numMessages records from the partition from the given startOffset.
func (p *Partition) ScanV2(startOffset base.Offset, numMessages uint64) ([][]byte, error, base.Offset) {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	var values [][]byte
	var nextOffset base.Offset
	if p.closed {
		return nil, ErrPartitionClosed, -1
	}

	// The start offset is no longer present. Ask client to start scanning from the current start offset of the
	// partition.
	if startOffset < p.currStartOffset {
		return values, nil, p.currStartOffset
	}
	endOffset := startOffset + base.Offset(numMessages) - 1

	// Get all the segments that contain our desired offsets.
	segs := p.getSegments(startOffset, endOffset)
	if segs == nil || len(segs) == 0 {
		// The start offset is greater than the largest offset in the partition. There is nothing to scan.
		return values, nil, -1
	}
	var scanSizeBytes int
	scanSizeBytes = 0

	// All offsets are present in the same segment.
	currOffset := startOffset
	if len(segs) == 1 {
		segStartOffset := startOffset - segs[0].GetMetadata().StartOffset
		tmpVals, tmpErrs := segs[0].Scan(segStartOffset, numMessages)
		for ii, val := range tmpVals {
			if ii != 0 {
				currOffset++
			}
			if tmpErrs[ii] != nil {
				return nil, ErrPartitionScan, startOffset
			}
			// Ensure that the batch size remains smaller than the max scan size.
			if (len(val) + scanSizeBytes) > p.maxScanSizeBytes {
				// We are skipping this message so set the nextOffset to the currOffset since it will need to
				// be scanned in the next scan call.
				nextOffset = currOffset
				break
			}
			nextOffset = currOffset + 1
			scanSizeBytes += len(val)
			body, _ := segments2.fetchValueFromMessage(val)
			values = append(values, body)
		}
		return values, nil, nextOffset
	}

	// The values are present across multiple segments. Merge them before returning.
	glog.V(1).Infof("%s Gathering values from multiple(%d) segments", p.logIDStr, len(segs))
	numPendingMsgs := numMessages
	var segStartOffset base.Offset
	for ii, seg := range segs {
		if numPendingMsgs == 0 {
			return values, nil, nextOffset
		}
		if ii == 0 {
			// First segment. Start scanning from the correct offset into the segment.
			segStartOffset = startOffset - seg.GetMetadata().StartOffset
		} else {
			// Start scanning from the very first offset in the segment.
			segStartOffset = 0
		}
		partialValues, partialErrs := seg.Scan(segStartOffset, numPendingMsgs)
		numPendingMsgs -= uint64(len(partialValues))
		for jj, val := range partialValues {
			if (ii != 0) && (jj != 0) {
				currOffset++
			}
			if partialErrs[jj] != nil {
				return nil, ErrPartitionScan, startOffset
			}
			// Ensure that the batch size remains smaller than the max scan size.
			if (len(val) + scanSizeBytes) > p.maxScanSizeBytes {
				nextOffset = currOffset
				return values, nil, nextOffset
			}
			scanSizeBytes += len(val)
			nextOffset = currOffset + 1

			// Merge all partial values into a single list.
			body, _ := segments2.fetchValueFromMessage(val)
			values = append(values, body)
		}
	}
	return values, nil, nextOffset
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
	p.closed = true
	close(p.backgroundJobDone)
	for _, seg := range p.segments {
		meta := seg.GetMetadata()
		err := seg.Close()
		if err != nil {
			glog.Fatalf("%s Failed to close segment: %d due to err: %s", p.logIDStr, meta.ID, err.Error())
		}
	}
}

// getSegments returns a list of segments that contains all the elements between the given start and end offsets.
// This function assumes that a partitionCfgLock has been acquired.
func (p *Partition) getSegments(startOffset base.Offset, endOffset base.Offset) []segments2.Segment {
	var segs []segments2.Segment

	// Find start offset segment.
	startSegIdx := p.findOffset(0, len(p.segments)-1, startOffset)
	if startSegIdx == -1 {
		// We did not find any segments that contains our offsets.
		return segs
	}

	// Find the end offset segment. Finding the end offset is split into two paths: fast and slow.
	// Fast Path: For the most part, the endIdx is going to be in the start or the next couple of
	// segments right after start. Check these segments first and if not present, fall back to
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
		endSegIdx = p.findOffset(startSegIdx, len(p.segments)-1, endOffset)
	}
	// Populate segments.
	segs = append(segs, p.segments[startSegIdx])
	if startSegIdx == endSegIdx {
		return segs
	}
	if endSegIdx == -1 {
		endSegIdx = len(p.segments) - 1
	}
	for ii := startSegIdx + 1; ii <= endSegIdx; ii++ {
		segs = append(segs, p.segments[ii])
	}
	return segs
}

// getLiveSegment returns the current live segment. This function assumes that the partitionCfgLock has been acquired.
func (p *Partition) getLiveSegment() segments2.Segment {
	return p.segments[len(p.segments)-1]
}

// findOffset finds the segment index that contains the given offset. This function assumes the partitionCfgLock has
// been acquired.
func (p *Partition) findOffset(startIdx int, endIdx int, offset base.Offset) int {
	// Base cases.
	if startIdx > endIdx {
		return -1
	}
	if startIdx == endIdx {
		if p.offsetInSegment(offset, p.segments[startIdx]) {
			return startIdx
		}
		return -1
	}
	midIdx := startIdx + (endIdx-startIdx)/2
	sOff, _ := p.segments[midIdx].GetRange()
	if p.offsetInSegment(offset, p.segments[midIdx]) {
		return midIdx
	} else if offset < sOff {
		return p.findOffset(0, midIdx-1, offset)
	} else {
		return p.findOffset(midIdx+1, endIdx, offset)
	}
}

// offsetInSegment checks whether the given offset is in the segment or not.
func (p *Partition) offsetInSegment(offset base.Offset, seg segments2.Segment) bool {
	if seg.IsEmpty() {
		return false
	}
	sOff, eOff := seg.GetRange()
	if offset >= sOff && offset <= eOff {
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
	glog.Infof("%s Partition manager has started", p.logIDStr)
	liveSegTicker := time.NewTicker(p.liveSegmentPollIntervalSecs)
	expTicker := time.NewTicker(p.expiredSegmentPollIntervalSecs)
	for {
		select {
		case <-p.backgroundJobDone:
			glog.Infof("%s Partition manager exiting", p.logIDStr)
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
	glog.Infof("%s Checking if new segment needs to be created", p.logIDStr)
	if p.shouldCreateNewSegment() {
		p.createNewSegmentWithLock()
	}
}

// shouldCreateNewSegment returns true if a new segment is required. false otherwise.
func (p *Partition) shouldCreateNewSegment() bool {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	if p.closed {
		return false
	}
	seg := p.segments[len(p.segments)-1]
	metadata := seg.GetMetadata()
	numRecords := metadata.EndOffset - metadata.StartOffset + 1
	if numRecords >= base.Offset(p.numRecordsPerSegment) {
		glog.Infof("%s Current live segment records(%d) has reached/exceeded threshold(%d). "+
			"New segment is required", p.logIDStr, numRecords, p.numRecordsPerSegment)
		return true
	}
	return false
}

func (p *Partition) createNewSegmentWithLock() {
	p.partitionCfgLock.Lock()
	defer p.partitionCfgLock.Unlock()
	if p.closed {
		return
	}
	p.createNewSegment()
}

// createNewSegment marks the current live segment immutable and creates a new live segment.
func (p *Partition) createNewSegment() {
	glog.Infof("%s Creating new segment", p.logIDStr)
	var startOffset base.Offset
	var segID uint64

	if len(p.segments) == 0 {
		// First ever segment in the partition.
		startOffset = 0
		segID = 1
	} else {
		seg := p.segments[len(p.segments)-1]
		seg.MarkImmutable()
		prevMetadata := seg.GetMetadata()
		err := seg.Close()
		if err != nil {
			glog.Fatalf("%s Failure while closing last segment: %d in partition %d due to err: %s",
				p.logIDStr, prevMetadata.ID, p.partitionID, err.Error())
			return
		}
		seg, err = segments2.NewBadgerSegment(p.getSegmentDirectory(int(prevMetadata.ID)))
		if err != nil {
			glog.Fatalf("%s Failure while closing and reopening last segment due to err: %s",
				p.logIDStr, err.Error())
			return
		}
		p.segments[len(p.segments)-1] = seg

		// Save the segment ID and start offset.
		segID = prevMetadata.ID + 1
		startOffset = prevMetadata.EndOffset + 1
	}

	newSeg, err := segments2.NewBadgerSegment(p.getSegmentDirectory(int(segID)))
	if err != nil {
		glog.Fatalf("%s Unable to create new segment due to err: %s", p.logIDStr, err.Error())
		return
	}
	metadata := segments2.SegmentMetadata{
		ID:               segID,
		Immutable:        false,
		Expired:          false,
		CreatedTimestamp: time.Now(),
		StartOffset:      startOffset,
	}
	newSeg.SetMetadata(metadata)
	p.segments = append(p.segments, newSeg)
}

// maybeReclaimExpiredSegments checks all the segments and marks the out of date segments as expired.
// It also marks these segments for deletion after ensuring that the segment is not required by any
// snapshots.
func (p *Partition) maybeReclaimExpiredSegments() {
	glog.Infof("%s Checking if segments need to be expired", p.logIDStr)
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
	expiredSegs := p.removeExpiredSegments()

	// The expired segs have been removed from segments. We can now safely acquire the RLock and delete the
	// segments
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	for _, seg := range expiredSegs {
		segId := seg.ID()
		seg.MarkExpired()
		err := seg.Close()
		if err != nil {
			glog.Fatalf("%s Unable to close segment due to err: %s", p.logIDStr, err.Error())
		}
		segDir := p.getSegmentDirectory(segId)
		expiredSegDirName := filepath.Base(segDir) + KExpiredSegmentDirSuffix
		expiredSegDir := path.Join(filepath.Dir(segDir), expiredSegDirName)
		glog.Infof("%s Renaming segment directory for segment: %d to %s", p.logIDStr, segId, expiredSegDir)
		err = os.Rename(segDir, expiredSegDir)
		if err != nil {
			glog.Fatalf("%s Unable to rename segment directory as expired due to err: %s",
				p.logIDStr, err.Error())
		}
		p.disposer.Dispose(expiredSegDir, func(err error) {
			if err != nil {
				glog.Fatalf("%s Unable to delete segment: %d in partition: [%s:%d] due to err: %s",
					p.logIDStr, segId, p.topicName, p.partitionID, err.Error())
			}
		})
	}
}

// removeExpiredSegments removes the expired segment(s) from segments.
func (p *Partition) removeExpiredSegments() []segments2.Segment {
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
	if lastIdxExpired == len(p.segments)-1 {
		p.segments = nil
		p.createNewSegment()
		return nil
	}
	var expiredSegs []segments2.Segment
	expiredSegs, p.segments = p.segments[0:lastIdxExpired+1], p.segments[lastIdxExpired+1:]
	return expiredSegs
}

func (p *Partition) isSegExpirable(metadata *segments2.SegmentMetadata) bool {
	now := time.Now().Unix()
	its := metadata.ImmutableTimestamp.Unix()
	lt := now - its
	if !metadata.Immutable {
		glog.V(0).Infof("%s Segment: %d is not immutable. Segment has not expired", p.logIDStr, metadata.ID)
		return false
	}
	if lt <= int64(p.ttlSeconds) {
		glog.V(0).Infof("%s Segment: %d has not expired. Current life time: %d, TTL: %d seconds",
			p.logIDStr, metadata.ID, lt, p.ttlSeconds)
		return false
	}
	glog.Infof("%s Segment: %d can be expired. Current life time: %d seconds, TTL: %d seconds",
		p.logIDStr, metadata.ID, lt, p.ttlSeconds)
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
		glog.Fatalf("%s Unable to read partition directory and find segments due to err: %v",
			p.logIDStr, err)
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
				glog.Fatalf("%s Unable to convert segment id to int due to err: %v",
					p.logIDStr, err)
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
			glog.Fatalf("%s Found hole in segments. Prev Seg ID: %d, Curr Seg ID: %d",
				p.logIDStr, prevID, id)
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
		glog.Fatalf("%s Unable to read partition directory and find segments due to err: %v",
			p.logIDStr, err)
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
