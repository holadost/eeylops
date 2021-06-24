package storage

import (
	"eeylops/server/base"
	"flag"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"
)

const KNumSegmentRecordsThreshold = 9.5e6 // 9.5 million
const KSegmentsDirectoryName = "segments"
const KMaxScanSizeBytes = 15 * 1000000 // 15MB

var (
	FlagExpiredSegmentMonitorIntervalSecs = flag.Int("partition_exp_segment_monitor_interval_seconds", 600,
		"Expired segment monitor interval seconds")
	FlagLiveSegmentMonitorIntervalSecs = flag.Int("partition_live_seg_monitor_interval_seconds", 5,
		"Live segment monitor interval seconds")
	FlagNumRecordsInSegment = flag.Int("partition_num_records_per_segment_threshold", KNumSegmentRecordsThreshold,
		"Number of records in a segment threshold")
	FlagMaxScanSizeBytes = flag.Int("partition_max_scan_size_bytes", KMaxScanSizeBytes,
		"Max scan size in bytes. Defaults to 16MB")
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
	currStartOffset uint64
	// Expired segment poll interval seconds.
	expiredSegmentPollIntervalSecs time.Duration
	// Live segment poll interval seconds.
	liveSegmentPollIntervalSecs time.Duration
	// Number of records per segment.
	numRecordsPerSegment int
	// Max scan size bytes.
	maxScanSizeBytes int
	// List of segments in the partition.
	segments []Segment
	// Notification to ask background goroutines to exit.
	backgroundJobDone chan bool
	// Snapshot channel.
	snapshotChan chan bool
	// Disposer.
	disposer *StorageDisposer
	// Callback channel after segments have been disposed.
	disposedChan chan int
	// Flag to indicate whether the partition is open/closed.
	closed bool
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
	p.disposedChan = make(chan int, 128)
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
		segment, err := NewBadgerSegment(p.getSegmentDirectory(segmentID))
		if err != nil {
			glog.Fatalf("Unable to initialize segment due to err: %s", err.Error())
			return
		}
		if ii < len(segmentIds)-1 {
			meta := segment.GetMetadata()
			if !meta.Immutable {
				glog.Fatalf("Found segment at index: %d(%d), %s to be live", ii, len(segmentIds),
					meta.ToString())
			}
		}
		p.segments = append(p.segments, segment)
	}

	// There are no segments in the backing file system. This must be the first time that the partition is being
	// created.
	if len(p.segments) == 0 {
		glog.Infof("Did not find any segment in the backing store. Creating segment for first time")
		p.createNewSegment()
	}

	// The last segment can be live or immutable. If immutable, create a new segment.
	lastSeg := p.segments[len(p.segments)-1]
	lastMeta := lastSeg.GetMetadata()
	if lastMeta.Immutable {
		p.createNewSegment()
	}
	p.closed = false

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
	err := seg.Append(values)
	if err != nil {
		glog.Errorf("Unable to append entries to partition: %d due to err: %s", p.partitionID, err.Error())
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
			if (len(val) + scanSizeBytes) >= p.maxScanSizeBytes {
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
	glog.V(1).Infof("Gathering values from multiple(%d) segments", len(segs))
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
			if (len(val) + scanSizeBytes) >= p.maxScanSizeBytes {
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
	close(p.backgroundJobDone)
	for _, seg := range p.segments {
		meta := seg.GetMetadata()
		err := seg.Close()
		if err != nil {
			glog.Fatalf("Failed to close segment: %d due to err: %s", meta.ID, err.Error())
		}
	}
	p.closed = true
}

// getSegments returns a list of segments that contains all the elements between the given start and end offsets.
// This function assumes that a partitionCfgLock has been acquired.
func (p *Partition) getSegments(startOffset base.Offset, endOffset base.Offset) []Segment {
	var segs []Segment

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
func (p *Partition) getLiveSegment() Segment {
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
func (p *Partition) offsetInSegment(offset base.Offset, seg Segment) bool {
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
	glog.Infof("Partition manager for partition ID: %d is now running", p.partitionID)
	liveSegTicker := time.NewTicker(p.liveSegmentPollIntervalSecs)
	expTicker := time.NewTicker(p.expiredSegmentPollIntervalSecs)
	for {
		select {
		case <-p.backgroundJobDone:
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
	if p.shouldCreateNewSegment() {
		p.createNewSegment()
	}
}

// shouldCreateNewSegment returns true if a new segment is required. false otherwise.
func (p *Partition) shouldCreateNewSegment() bool {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	seg := p.segments[len(p.segments)-1]
	metadata := seg.GetMetadata()
	if (metadata.EndOffset - metadata.StartOffset) > base.Offset(p.numRecordsPerSegment) {
		return true
	}
	return false
}

// createNewSegment marks the current live segment immutable and creates a new live segment.
func (p *Partition) createNewSegment() {
	// Acquire the segments lock since we are creating a new segment.
	p.partitionCfgLock.Lock()
	defer p.partitionCfgLock.Unlock()

	glog.Infof("Creating new segment for partition: %d", p.partitionID)
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
			glog.Fatalf("Failure while closing last segment: %d in partition %d due to err: %s",
				prevMetadata.ID, p.partitionID, err.Error())
			return
		}
		seg, err = NewBadgerSegment(p.getSegmentDirectory(int(prevMetadata.ID)))
		if err != nil {
			glog.Fatalf("Failure while closing and reopening last segment due to err: %s", err.Error())
			return
		}
		p.segments[len(p.segments)-1] = seg

		// Save the segment ID and start offset.
		segID = prevMetadata.ID + 1
		startOffset = prevMetadata.EndOffset + 1
	}

	newSeg, err := NewBadgerSegment(p.getSegmentDirectory(int(segID)))
	if err != nil {
		glog.Fatalf("Unable to create new segment due to err: %s", err.Error())
		return
	}
	metadata := SegmentMetadata{
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
	glog.Warningf("maybeReclaimExpiredSegments: Still not implemented!")
}

// snapshot saves the current partition configuration.
func (p *Partition) snapshot() {

}

/************************************* Helper methods ************************************************/
// getPartitionDirectory returns the root partition directory.
func (p *Partition) getPartitionDirectory() string {
	return path.Join(p.rootDir, strconv.Itoa(p.partitionID))
}

// getSegmentRootDirectory returns the segment directory for a given segmentID.
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
		glog.Fatalf("Unable to read partition directory and find segments due to err: %v", err)
		return nil
	}
	var segmentIDs []int
	for _, file := range fileInfo {
		if file.IsDir() {
			segmentID, err := strconv.Atoi(file.Name())
			if err != nil {
				glog.Fatalf("Unable to convert segment id to int due to err: %v", err)
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
			glog.Fatalf("Found hole in segments. Prev Seg ID: %d, Curr Seg ID: %d", prevID, id)
		}
	}
	return segmentIDs
}
