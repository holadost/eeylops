package segments

import (
	"context"
	"eeylops/server/base"
	sbase "eeylops/server/storage/base"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	_ "github.com/lib/pq"
)

type Segment interface {
	// ID of the segment.
	ID() int
	// Open the segment.
	Open()
	// Close the segment.
	Close() error
	// Append values to the segment.
	Append(ctx context.Context, arg *sbase.AppendEntriesArg) *sbase.AppendEntriesRet
	// Scan values from the segment.
	Scan(ctx context.Context, arg *sbase.ScanEntriesArg) *sbase.ScanEntriesRet
	// IsEmpty returns true if the segment is empty. False otherwise.
	IsEmpty() bool
	// Stats fetches the stats for this instance of segment.
	Stats()
	// GetMetadata fetches the metadata of the segment.
	GetMetadata() SegmentMetadata
	// GetRange returns the start and end offset of the segment.
	GetRange() (base.Offset, base.Offset)
	// GetMsgTimestampRange returns the first and last message timestamps(in nano seconds) in the segment.
	GetMsgTimestampRange() (int64, int64)
	// SetMetadata sets the metadata. This is updated internally and by the partition when a segment is created.
	SetMetadata(SegmentMetadata)
	// MarkImmutable marks the segment as immutable.
	MarkImmutable()
	// MarkExpired marks the segment as expired.
	MarkExpired()
}
