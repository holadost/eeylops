package storage

import (
	"sync"
)

type Partition struct {
	segments    []Segment
	liveSegment Segment
	segmentLock sync.Mutex
}

func NewPartition() (*Partition, error) {
	return nil, nil
}

func (p *Partition) Initialize() {

}

func (p *Partition) GetSegments() []Segment {
	return nil
}

func (p *Partition) GetLiveSegment() Segment {
	return nil
}
