package segments

import (
	"context"
	"eeylops/server/base"
	base2 "eeylops/server/storage/base"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	_ "github.com/lib/pq"
)

type Segment interface {
	ID() int
	// Close the segment.
	Close() error
	// Append values to the segment.
	Append(ctx context.Context, arg *base2.AppendEntriesArg) *base2.AppendEntriesRet
	Scan(ctx context.Context, arg *base2.ScanEntriesArg) *base2.ScanEntriesRet
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
