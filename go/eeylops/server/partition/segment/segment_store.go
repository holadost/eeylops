package segment

type SegmentStore interface {
	Append()
	Scan()
	Metrics()
	GetMetadata()
	Close()
	Initialize()
}

type SegmentMetadata struct {
}
