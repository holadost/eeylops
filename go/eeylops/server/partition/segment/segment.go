package segment

type SegmentStore interface {
	// Initialize the segment store.
	Initialize()
	// Close the segment store.
	Close()
	// Append values to the segment store.
	Append([][]byte) error
	// Scan numMessages values from the segment store from the given start offset.
	Scan(startOffset uint64, numMessages uint64) ([][]byte, []error)
	// Stats fetches the stats for this instance of segment store.
	Stats()
	// Metadata fetches the metadata of the segment store.
	Metadata() SegmentMetadata
	// SetImmutable marks the segment store as immutable.
	SetImmutable()
}

type SegmentMetadata struct {
}
