package segments

import (
	"context"
	"eeylops/server/base"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	_ "github.com/lib/pq"
)

type Segment interface {
	// ID of the segment. This call must be allowed even when the segment is closed or never been opened.
	ID() int
	// Open the segment. Should only happen once. Subsequent opens must panic.
	Open()
	// Close the segment.
	Close() error
	// Append values to the segment. This is allowed only after the segment has been opened.
	Append(ctx context.Context, arg *AppendEntriesArg) *AppendEntriesRet
	// Scan values from the segment. This is allowed only after the segment has been opened.
	Scan(ctx context.Context, arg *ScanEntriesArg) *ScanEntriesRet
	// IsEmpty returns true if the segment is empty. False otherwise. This is allowed only after the segment has been
	// opened.
	IsEmpty() bool
	// Stats fetches the stats for this instance of segment. This is allowed only after the segment has been opened.
	Stats()
	// Size returns the size of the segment(in bytes). This is allowed only after the segment has been opened.
	Size() int64
	// GetMetadata fetches the metadata of the segment. This should be allowed even if the segment is not open.
	GetMetadata() SegmentMetadata
	// GetRange returns the start and end offset of the segment. This is allowed only after the segment is open.
	GetRange() (base.Offset, base.Offset)
	// GetMsgTimestampRange returns the first and last message timestamps(in nano seconds) in the segment. This is
	//allowed only after the segment is open.
	GetMsgTimestampRange() (int64, int64)
	// SetMetadata sets the metadata. This is updated internally and by the partition when a segment is created. This
	// is allowed only after the segment is open.
	SetMetadata(SegmentMetadata)
	// MarkImmutable marks the segment as immutable. This is allowed only after the segment is open.
	MarkImmutable()
	// MarkExpired marks the segment as expired. This is allowed only after the segment is open.
	MarkExpired()
}
