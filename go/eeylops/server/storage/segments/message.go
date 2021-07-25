package segments

import (
	SegmentsFB "eeylops/generated/flatbuf/server/storage/segments"
	"eeylops/server/base"
	"eeylops/util"
	"github.com/golang/glog"
	flatbuf "github.com/google/flatbuffers/go"
)

type Message struct {
	body      []byte
	timestamp int64
	raw       []byte
}

func NewMessage() *Message {
	msg := new(Message)
	msg.Clear()
	return msg
}

func (msg *Message) Clear() {
	msg.timestamp = -1
	msg.body = nil
	msg.raw = nil
}

func (msg *Message) SetBody(body []byte) {
	msg.body = body
}

func (msg *Message) SetTimestamp(ts int64) {
	msg.timestamp = ts
}

func (msg *Message) GetBody() []byte {
	if msg.body != nil {
		return msg.body
	}
	if msg.raw != nil {
		return msg.raw[8:]
	}
	return nil
}

func (msg *Message) GetBodySize() int {
	if msg.body != nil {
		return len(msg.body)
	} else if msg.raw != nil {
		return len(msg.raw) - 8
	} else {
		return 0
	}
}

func (msg *Message) GetTimestamp() int64 {
	return msg.timestamp
}

func (msg *Message) InitializeFromRaw(raw []byte) {
	msg.Clear()
	msg.raw = raw
	if len(msg.raw) <= 8 {
		glog.Fatalf("Expected raw input to be > 8 bytes. Got: %s, size: %d", string(raw), len(msg.raw))
	}
	// The body can be large. Avoid creating a copy till it is required. Just save the timestamp.
	msg.timestamp = int64(util.BytesToUint(msg.raw[0:8]))
}

func (msg *Message) Serialize() []byte {
	if msg.timestamp == -1 {
		glog.Fatalf("Timestamp field is not set")
	}
	if msg.body == nil {
		glog.Fatalf("Message body is not set")
	}
	return append(util.UintToBytes(uint64(msg.timestamp)), msg.body...)
}

func PrepareMessageValues(values [][]byte, ts int64) (retValues [][]byte, totalSize int64) {
	totalSize = 0
	retValues = make([][]byte, len(values))
	for ii, value := range values {
		var msg Message
		msg.SetTimestamp(ts)
		msg.SetBody(value)
		retValues[ii] = msg.Serialize()
		totalSize += int64(len(retValues[ii]))
	}
	return
}

func makeIndexEntry(ts int64, offset base.Offset) []byte {
	builder := flatbuf.NewBuilder(8)
	SegmentsFB.IndexEntryStart(builder)
	SegmentsFB.IndexEntryAddTimestamp(builder, ts)
	SegmentsFB.IndexEntryAddOffset(builder, int64(offset))
	msg := SegmentsFB.IndexEntryEnd(builder)
	builder.Finish(msg)
	return builder.FinishedBytes()
}
