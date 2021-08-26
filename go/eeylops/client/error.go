package client

import (
	"eeylops/comm"
	"fmt"
)

type Error struct {
	errorCode comm.Error_ErrorCodes
	errorMsg  string
}

func newError(code comm.Error_ErrorCodes, msg string) *Error {
	return &Error{
		errorCode: code,
		errorMsg:  msg,
	}
}

func (err *Error) Error() string {
	return fmt.Sprintf("[%s] %s", err.errorCode.String(), err.errorMsg)
}
