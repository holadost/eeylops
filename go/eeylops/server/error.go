package server

import (
	"eeylops/comm"
)

func makeErrorProto(code comm.Error_ErrorCodes, err error, msg string) *comm.Error {
	var ep comm.Error
	ep.ErrorCode = code
	var fullMsg string
	if code == comm.Error_KNoError {
		ep.ErrorMsg = ""
	} else {
		fullMsg += "message: " + msg
		if err != nil {
			fullMsg += " error: " + msg
		}
		ep.ErrorMsg = fullMsg
	}
	return &ep
}
