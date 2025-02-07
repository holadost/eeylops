package base

import (
	"eeylops/comm"
)

func MakeErrorProto(code comm.Error_ErrorCodes, err error, msg string) *comm.Error {
	var ep comm.Error
	ep.ErrorCode = code
	fullMsg := "[" + code.String() + "]"
	if code == comm.Error_KNoError {
		ep.ErrorMsg = ""
	} else {
		fullMsg += " : " + msg
		ep.ErrorMsg = fullMsg
	}
	return &ep
}
