package segments

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	_ "github.com/lib/pq"
)

type Segment interface {
	ID() int
	// Close the segment.
	Close() error
	// Append values to the segment. The second int64 defines the last replicated log index.
	Append([][]byte, int64) error
	AppendV2(*storage.AppendEntriesArg) *storage.AppendEntriesRet
	// Scan numMessages values from the segment store from the given start offset.
	Scan(startOffset base.Offset, numMessages uint64) ([][]byte, []error)
	ScanV2(*storage.ScanEntriesArg) *storage.ScanEntriesRet
	// IsEmpty returns true if the segment is empty. False otherwise.
	IsEmpty() bool
	// Stats fetches the stats for this instance of segment.
	Stats()
	// GetMetadata fetches the metadata of the segment.
	GetMetadata() SegmentMetadata
	// GetRange returns the start and end offset of the segment.
	GetRange() (base.Offset, base.Offset)
	// SetMetadata sets the metadata. This is updated internally and by the partition when a segment is created.
	SetMetadata(SegmentMetadata)
	// MarkImmutable marks the segment as immutable.
	MarkImmutable()
	// MarkExpired marks the segment as expired.
	MarkExpired()
}
