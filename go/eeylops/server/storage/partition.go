package storage

import (
	"sync"
)

type Partition struct {
	segments     []*segmentEntry // List of segments in the partition.
	segmentsLock sync.RWMutex    // This mutex guards segments and segmentIntervals slice.
	cache        *PartitionCache // Partition cache.
}

type segmentEntry struct {
	segment Segment
	segLock sync.RWMutex // This guards read/write access to the segment.
}

func NewPartition() (*Partition, error) {
	return nil, nil
}

func (p *Partition) initialize() {
}

// GetSegments returns a list of segments that contains all the elements between the given start and end offsets.
func (p *Partition) GetSegments(startOffset uint64, endOffset uint64) []Segment {
	p.segmentsLock.RLock()
	defer p.segmentsLock.RUnlock()
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
		if p.offsetInSegment(endOffset, p.segments[ii].segment.GetMetadata()) {
			endSegIdx = ii
			break
		}
	}
	// Slow path.
	if endSegIdx == -1 {
		endSegIdx = p.findOffset(startSegIdx, len(p.segments)-1, endOffset)
	}

	// Populate segments.
	segs = append(segs, p.segments[startSegIdx].segment)
	if endSegIdx == -1 || startSegIdx == endSegIdx {
		return segs
	}
	for ii := startSegIdx + 1; ii <= endSegIdx; ii++ {
		segs = append(segs, p.segments[ii].segment)
	}
	return segs
}

// GetLiveSegment returns the current live segment.
func (p *Partition) GetLiveSegment() Segment {
	p.segmentsLock.RLock()
	defer p.segmentsLock.RUnlock()
	return p.segments[len(p.segments)-1].segment
}

// findOffset finds the segment index that contain the given offset. This function assumes that a
// read lock has been acquired on segments.
func (p *Partition) findOffset(startIdx int, endIdx int, offset uint64) int {
	// Base cases.
	if startIdx > endIdx {
		return -1
	}
	if startIdx == endIdx {
		metadata := p.segments[startIdx].segment.GetMetadata()
		if p.offsetInSegment(offset, metadata) {
			return startIdx
		}
		return -1
	}
	midIdx := startIdx + (endIdx-startIdx)/2
	metadata := p.segments[midIdx].segment.GetMetadata()
	if p.offsetInSegment(offset, metadata) {
		return midIdx
	} else if offset < metadata.StartOffset {
		return p.findOffset(0, midIdx-1, offset)
	} else {
		return p.findOffset(midIdx+1, endIdx, offset)
	}
}

func (p *Partition) offsetInSegment(offset uint64, metadata SegmentMetadata) bool {
	if offset >= metadata.StartOffset && offset <= metadata.EndOffset {
		return true
	}
	return false
}
