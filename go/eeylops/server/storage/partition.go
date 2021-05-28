package storage

import (
	"context"
	"github.com/golang/glog"
	"path"
	"strconv"
	"sync"
	"time"
)

const KNumSegmentRecordsThreshold = 9.5e6 // 9.5 million
const KSegmentsDirectoryName = "segments"

type Partition struct {
	partitionID      int                // Partition ID.
	segments         []Segment          // List of segments in the partition.
	rootDir          string             // Root directory of the partition.
	gcPeriod         time.Duration      // GC period in seconds.
	ctx              context.Context    // Context for the partition.
	cancelFunc       context.CancelFunc // Cancellation function that is invoked when the partition is closed.
	partitionCfgLock sync.RWMutex       // Lock on the partition configuration.
}

func NewPartition(id int, rootDir string) (*Partition, error) {
	p := new(Partition)
	p.partitionID = id
	p.rootDir = rootDir
	return nil, nil
}

func (p *Partition) initialize() {
}

// Append records to the partition.
func (p *Partition) Append(values [][]byte) {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
}

// Scan messages from the partition from the given startOffset up to the numMessages.
func (p *Partition) Scan(startOffset uint64, numMessages uint64) (values [][]byte, errs []error) {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	return nil, nil
}

// Snapshot the partition.
func (p *Partition) Snapshot() error {
	return nil
}

// getSegments returns a list of segments that contains all the elements between the given start and end offsets.
// This function assumes that a partitionCfgLock has been acquired.
func (p *Partition) getSegments(startOffset uint64, endOffset uint64) []Segment {
	var segs []Segment

	// Find start offset segment.
	startSegIdx := p.findOffset(0, len(p.segments)-1, startOffset)
	if startSegIdx == -1 {
		// We did not find any segments that contains our offsets.
		return segs
	}

	// Find the end offset segment. Finding the end offset is split into two paths: fast and slow.
	// Fast Path: For the most part, the endIdx is going to be in the start or the next couple of
	// segments right after start. So we quickly check that and if it isn't there, we fall back to scanning
	// all segments.
	endSegIdx := -1
	for ii := startSegIdx; ii < startSegIdx+3; ii++ {
		if ii >= len(p.segments) {
			break
		}
		if p.offsetInSegment(endOffset, p.segments[ii].GetMetadata()) {
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
	if endSegIdx == -1 || startSegIdx == endSegIdx {
		return segs
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

// findOffset finds the segment index that contain the given offset. This function assumes the partitionCfgLock has
// been acquired.
func (p *Partition) findOffset(startIdx int, endIdx int, offset uint64) int {
	// Base cases.
	if startIdx > endIdx {
		return -1
	}
	if startIdx == endIdx {
		metadata := p.segments[startIdx].GetMetadata()
		if p.offsetInSegment(offset, metadata) {
			return startIdx
		}
		return -1
	}
	midIdx := startIdx + (endIdx-startIdx)/2
	metadata := p.segments[midIdx].GetMetadata()
	if p.offsetInSegment(offset, metadata) {
		return midIdx
	} else if offset < metadata.StartOffset {
		return p.findOffset(0, midIdx-1, offset)
	} else {
		return p.findOffset(midIdx+1, endIdx, offset)
	}
}

// offsetInSegment checks whether the given offset is in the segment or not.
func (p *Partition) offsetInSegment(offset uint64, metadata SegmentMetadata) bool {
	if offset >= metadata.StartOffset && offset <= metadata.EndOffset {
		return true
	}
	return false
}

// monitorExpiredSegments periodically monitors all the segments and marks the out of date segments as expired.
// It also deletes segments after ensuring that the segment is not required by any snapshots.
func (p *Partition) monitorExpiredSegments() {

}

// monitorLiveSegment monitors the current live segment periodically and if the segment has more records than
// the threshold, it then marks the segment as immutable and opens a new live segment.
func (p *Partition) monitorLiveSegment() {
	for {
		time.Sleep(time.Second * 5)
		if p.shouldCreateNewSegment() {
			p.createNewSegment()
		}
	}
}

func (p *Partition) shouldCreateNewSegment() bool {
	p.partitionCfgLock.RLock()
	defer p.partitionCfgLock.RUnlock()
	seg := p.segments[len(p.segments)-1]
	metadata := seg.GetMetadata()
	if (metadata.EndOffset - metadata.StartOffset) > (KNumSegmentRecordsThreshold) {
		return true
	}
	return false
}

func (p *Partition) createNewSegment() {
	glog.Infof("Creating new segment for partition: %d", p.partitionID)
	// Acquire the segments lock since we are creating a new segment.
	p.partitionCfgLock.Lock()
	defer p.partitionCfgLock.Unlock()

	// Acquire the lock on the segment entry since we are going to mark the segment immutable.
	segEntry := p.segments[len(p.segments)-1]
	segEntry.MarkImmutable()
	prevMetadata := segEntry.GetMetadata()

	newSeg, err := NewBadgerSegment(path.Join(p.getPartitionDirectory(), KSegmentsDirectoryName,
		strconv.Itoa(int(prevMetadata.ID+1))))
	if err != nil {
		glog.Fatalf("Unable to create new segment due to err: %s", err.Error())
		return
	}
	metadata := SegmentMetadata{
		ID:               prevMetadata.ID + 1,
		Immutable:        false,
		Expired:          false,
		CreatedTimestamp: time.Now(),
		StartOffset:      prevMetadata.EndOffset + 1,
	}
	newSeg.SetMetadata(metadata)
	p.segments = append(p.segments, newSeg)
}

func (p *Partition) getPartitionDirectory() string {
	return path.Join(p.rootDir, strconv.Itoa(p.partitionID))
}
