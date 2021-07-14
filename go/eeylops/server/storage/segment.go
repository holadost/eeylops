package storage

import (
	"eeylops/server/base"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	_ "github.com/lib/pq"
	"time"
)

type Segment interface {
	ID() int
	// Close the segment.
	Close() error
	// Append values to the segment.
	Append([][]byte) error
	// Scan numMessages values from the segment store from the given start offset.
	Scan(startOffset base.Offset, numMessages uint64) ([][]byte, []error)
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

// SegmentMetadata holds the metadata of a segment.
// Note: Make sure to add any new fields to the ToString() method as well.
type SegmentMetadata struct {
	ID                 uint64      `json:"id"`                  // Segment ID.
	Immutable          bool        `json:"immutable"`           // Flag to indicate whether segment is immutable.
	Expired            bool        `json:"expired"`             // Flag indicating whether the segment has expired.
	StartOffset        base.Offset `json:"start_offset"`        // StartOffset of the segment.
	EndOffset          base.Offset `json:"end_offset"`          // EndOffset of the segment. Not valid if segment is live.
	CreatedTimestamp   time.Time   `json:"created_timestamp"`   // Segment created time.
	ImmutableTimestamp time.Time   `json:"immutable_timestamp"` // Time when segment was marked as immutable.
	ImmutableReason    int         `json:"immutable_reason"`    // The reason why the segment was marked immutable.
	ExpiredTimestamp   time.Time   `json:"expired_timestamp"`   // Time when segment was expired.
}

func newSegmentMetadata(data []byte) *SegmentMetadata {
	var sm SegmentMetadata
	err := json.Unmarshal(data, &sm)
	if err != nil {
		glog.Fatalf("Failed to deserialize segment metadata due to err: %s", err.Error())
	}
	return &sm
}

func (sm *SegmentMetadata) ToString() string {
	return fmt.Sprintf("ID: %d, Immutable %v, Expired %v, Start offset: %d, End offset: %d, Created At: %v, "+
		"Immutable At: %v, Expired At: %v, Immutable Reason: %d", sm.ID, sm.Immutable, sm.Expired, sm.StartOffset,
		sm.EndOffset, sm.CreatedTimestamp, sm.ImmutableTimestamp, sm.ExpiredTimestamp, sm.ImmutableReason)
}

func (sm *SegmentMetadata) Serialize() []byte {
	data, err := json.Marshal(sm)
	if err != nil {
		glog.Fatalf("Unable to serialize metadata for segment")
	}
	return data
}

// Constants.
const dataDirName = "data"
const metadataDirName = "metadata"
const metadataDbName = "metadata.db"
const metadataKeyName = "metadata"
