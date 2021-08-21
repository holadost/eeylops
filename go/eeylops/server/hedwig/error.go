package hedwig

import (
	"eeylops/comm"
	"github.com/golang/glog"
)

type ErrorCode int

func (ec ErrorCode) ToString() string {
	switch ec {
	case KErrNoError:
		return "NoError"
	case KErrInvalidArg:
		return "ErrInvalidArg"
	case KErrNotLeader:
		return "ErrNotLeader"
	case KErrBackendStorage:
		return "ErrBackendStorage"
	case KErrReplication:
		return "ErrReplication"
	case KErrTopicNotFound:
		return "ErrTopicNotFound"
	case KErrSubscriberNotRegistered:
		return "ErrSubscriberNotRegistered"
	case KErrTopicExists:
		return "ErrTopicExists"
	case KErrPartitionNotFound:
		return "ErrPartitionNotFound"
	default:
		glog.Fatalf("Invalid error code: %d", ec)
		return "Invalid error code"
	}
}

const (
	KErrNoError                 ErrorCode = 0
	KErrInvalidArg              ErrorCode = 1
	KErrNotLeader               ErrorCode = 2
	KErrBackendStorage          ErrorCode = 3
	KErrReplication             ErrorCode = 4
	KErrTopicNotFound           ErrorCode = 5
	KErrTopicExists             ErrorCode = 6
	KErrPartitionNotFound       ErrorCode = 7
	KErrSubscriberNotRegistered ErrorCode = 8
)

func makeErrorProto(code ErrorCode, err error, msg string) *comm.Error {
	var ep comm.Error
	ep.ErrorCode = int32(code)
	var fullMsg string
	if code != KErrNoError {
		ep.ErrorMsg = ""
	} else {
		fullMsg += code.ToString()
		fullMsg += ": " + msg
		if err != nil {
			fullMsg += " Error: " + msg
		}
		ep.ErrorMsg = fullMsg
	}
	return &ep
}
