package segment

type Segment interface {
	// Initialize the segment.
	Initialize()
	// Close the segment.
	Close()
	// Append values to the segment.
	Append([][]byte) error
	// Scan numMessages values from the segment store from the given start offset.
	Scan(startOffset uint64, numMessages uint64) ([][]byte, []error)
	// Stats fetches the stats for this instance of segment.
	Stats()
	// Metadata fetches the metadata of the segment.
	Metadata() SegmentMetadata
	// SetImmutable marks the segment store as immutable.
	SetImmutable()
}

// SegmentMetadata holds the metadata of a segment.
type SegmentMetadata struct {
	NextOffset uint64
	DataDir    string
	Immutable  bool
}
