package server

import (
	"eeylops/comm"
)

func makeErrorProto(code comm.Error_ErrorCodes, err error, msg string) *comm.Error {
	var ep comm.Error
	ep.ErrorCode = code
	fullMsg := code.String()
	if code == comm.Error_KNoError {
		ep.ErrorMsg = ""
	} else {
		fullMsg += " : " + msg
		if err != nil {
			fullMsg += " : " + err.Error()
		}
		ep.ErrorMsg = fullMsg
	}
	return &ep
}
