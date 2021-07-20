package segments

import (
	Message "eeylops/generated/flatbuf/server/storage/segments"
	flatbuf "github.com/google/flatbuffers/go"
)

func makeMessageValues(values [][]byte, ts int64) (retValues [][]byte) {
	if len(values) == 0 {
		return
	}
	for _, value := range values {
		builder := flatbuf.NewBuilder(len(value) + 8)
		Message.MessageStartBodyVector(builder, len(value))
		for ii := len(value) - 1; ii >= 0; ii-- {
			builder.PrependByte(value[ii])
		}
		body := builder.EndVector(len(value))
		Message.MessageStart(builder)
		Message.MessageAddTimestamp(builder, ts)
		Message.MessageAddBody(builder, body)
		msg := Message.MessageEnd(builder)
		builder.Finish(msg)
		retValues = append(retValues, builder.FinishedBytes())
	}
	return
}

func fetchValueFromMessage(message []byte) ([]byte, int64) {
	msgFb := Message.GetRootAsMessage(message, 0)
	retVal := make([]byte, msgFb.BodyLength())
	for ii := 0; ii < msgFb.BodyLength(); ii++ {
		retVal[ii] = byte(msgFb.Body(ii))
	}
	return retVal, msgFb.Timestamp()
}

func fetchTimestampFromMessage(message []byte) int64 {
	return Message.GetRootAsMessage(message, 0).Timestamp()
}
