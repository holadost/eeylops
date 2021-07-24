package segments

import (
	SegmentsFB "eeylops/generated/flatbuf/server/storage/segments"
	"eeylops/server/base"
	flatbuf "github.com/google/flatbuffers/go"
)

func makeMessageValues(values [][]byte, ts int64) (retValues [][]byte, totalSize int64) {
	if len(values) == 0 {
		return
	}
	totalSize = 0
	for _, value := range values {
		totalSize += int64(len(value))
		builder := flatbuf.NewBuilder(len(value) + 8)
		SegmentsFB.MessageStartBodyVector(builder, len(value))
		for ii := len(value) - 1; ii >= 0; ii-- {
			builder.PrependByte(value[ii])
		}
		body := builder.EndVector(len(value))
		SegmentsFB.MessageStart(builder)
		SegmentsFB.MessageAddTimestamp(builder, ts)
		SegmentsFB.MessageAddBody(builder, body)
		msg := SegmentsFB.MessageEnd(builder)
		builder.Finish(msg)
		retValues = append(retValues, builder.FinishedBytes())
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

func fetchValueFromMessage(message []byte) ([]byte, int64) {
	msgFb := SegmentsFB.GetRootAsMessage(message, 0)
	retVal := make([]byte, msgFb.BodyLength())
	for ii := 0; ii < msgFb.BodyLength(); ii++ {
		retVal[ii] = byte(msgFb.Body(ii))
	}
	return retVal, msgFb.Timestamp()
}

func fetchTimestampFromMessage(message []byte) int64 {
	return SegmentsFB.GetRootAsMessage(message, 0).Timestamp()
}
