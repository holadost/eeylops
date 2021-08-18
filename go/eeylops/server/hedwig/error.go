package hedwig

import (
	"github.com/golang/glog"
	"strconv"
)

type ErrorCode int

func (ec ErrorCode) ToString() string {
	switch ec {
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
	default:
		glog.Fatalf("Invalid error code: %d", ec)
		return "Invalid error code"
	}
}

const (
	KErrInvalidArg              ErrorCode = 1
	KErrNotLeader               ErrorCode = 2
	KErrBackendStorage          ErrorCode = 3
	KErrReplication             ErrorCode = 4
	KErrTopicNotFound           ErrorCode = 5
	KErrTopicExists             ErrorCode = 6
	KErrSubscriberNotRegistered ErrorCode = 7
)

type hedwigError struct {
	errorCode ErrorCode
	errorMsg  string
}

func (he *hedwigError) Error() string {
	return he.errorMsg
}

func makeHedwigError(ec ErrorCode, err error, additionalMsg string) error {
	var msg string
	msg += ec.ToString()
	msg += "[" + strconv.Itoa(int(ec)) + "]"
	if err != nil {
		msg += " " + err.Error()
	}
	if len(additionalMsg) != 0 {
		msg += ". " + additionalMsg
	}
	retErr := &hedwigError{
		errorCode: ec,
		errorMsg:  msg,
	}
	return retErr
}
