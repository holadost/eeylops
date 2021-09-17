package base

type FSMResponse struct {
	CommandType CmdType
	Error       error
	Response    interface{}
}
