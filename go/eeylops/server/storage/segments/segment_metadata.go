package segments

import (
	"eeylops/server/base"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"time"
)

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
	FirstMsgTimestamp  time.Time   `json:"first_msg_timestamp"` // The first message timestamp.
	LastMsgTimestamp   time.Time   `json:"last_msg_timestamp"`  // The last message timestamp.
}

func NewSegmentMetadata(data []byte) *SegmentMetadata {
	var sm SegmentMetadata
	err := json.Unmarshal(data, &sm)
	if err != nil {
		glog.Fatalf("Failed to deserialize segment metadata due to err: %s", err.Error())
	}
	return &sm
}

func (sm *SegmentMetadata) ToString() string {
	return fmt.Sprintf("ID: %d, Immutable %v, Expired %v, Start offset: %d, End offset: %d, Created At: %v, "+
		"Immutable At: %v, Expired At: %v, Immutable Reason: %d, First Message Timestamp: %v, "+
		"Last Message Timestamp: %v", sm.ID, sm.Immutable, sm.Expired, sm.StartOffset, sm.EndOffset,
		sm.CreatedTimestamp, sm.ImmutableTimestamp, sm.ExpiredTimestamp, sm.ImmutableReason, sm.FirstMsgTimestamp,
		sm.LastMsgTimestamp)
}

func (sm *SegmentMetadata) Serialize() []byte {
	data, err := json.Marshal(sm)
	if err != nil {
		glog.Fatalf("Unable to serialize metadata for segment")
	}
	return data
}
