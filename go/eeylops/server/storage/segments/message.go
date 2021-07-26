package segments

import (
	"bytes"
	"eeylops/server/base"
	"eeylops/util"
	"encoding/gob"
	"github.com/golang/glog"
)

type Message struct {
	body      []byte
	timestamp int64
	raw       []byte
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
		totalSize += int64(len(value))
		var msg Message
		msg.SetTimestamp(ts)
		msg.SetBody(value)
		retValues[ii] = msg.Serialize()
	}
	return
}

func prepareMessageValues(values [][]byte, ts int64, currIndexBatchSizeBytes int64, nextOffset base.Offset) ([][]byte, []TimestampIndexEntry, int64) {
	var tse []TimestampIndexEntry
	retValues := make([][]byte, len(values))
	pendingBeforeNextIndex := kIndexEveryNBytes - currIndexBatchSizeBytes
	for ii, value := range values {
		var msg Message
		msg.SetTimestamp(ts)
		msg.SetBody(value)
		retValues[ii] = msg.Serialize()
		valSize := int64(len(retValues[ii]))
		pendingBeforeNextIndex -= valSize
		if pendingBeforeNextIndex <= 0 {
			pendingBeforeNextIndex = 0
			var entry TimestampIndexEntry
			entry.SetOffset(nextOffset + base.Offset(ii))
			entry.SetTimestamp(ts)
			tse = append(tse, entry)
		}
	}
	return retValues, tse, kIndexEveryNBytes - pendingBeforeNextIndex
}

type TimestampIndexEntry struct {
	Timestamp int64
	Offset    base.Offset
}

func (tse *TimestampIndexEntry) Clear() {
	tse.Timestamp = -1
	tse.Offset = -1
}

func (tse *TimestampIndexEntry) GetTimestamp() int64 {
	return tse.Timestamp
}

func (tse *TimestampIndexEntry) SetTimestamp(ts int64) {
	tse.Timestamp = ts
}

func (tse *TimestampIndexEntry) GetOffset() base.Offset {
	return tse.Offset
}

func (tse *TimestampIndexEntry) SetOffset(offset base.Offset) {
	tse.Offset = offset
}

func (tse *TimestampIndexEntry) InitializeFromRaw(raw []byte) {
	tse.Clear()
	dec := gob.NewDecoder(bytes.NewBuffer(raw))
	err := dec.Decode(tse)
	if err != nil {
		glog.Fatalf("Unable to initialize timestamp index entry from raw input: %s due to err: %s",
			string(raw), err.Error())
	}
}

func (tse *TimestampIndexEntry) Serialize() []byte {
	var w bytes.Buffer
	enc := gob.NewEncoder(&w)
	err := enc.Encode(tse)
	if err != nil {
		glog.Fatalf("Unable to prepare index entry due to err: %s", err.Error())
	}
	return w.Bytes()
}
