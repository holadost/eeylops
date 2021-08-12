package hedwig

import "eeylops/server/replication"

type PeerAddress struct {
	Host string // Host name.
	Port int    // Port number.
}

type FSMOptions struct {
	DataDirectory string        // Data directory for this FSM.
	BrokerID      string        // Broker ID.
	PeerAddresses []PeerAddress // List of peer addresses. The first address must be the current host's address.
}

type FSM struct {
	raftHandler     *replication.RaftHandler
	topicController *TopicsController
}

func NewFSM(opts *FSMOptions) *FSM {
	var fsm FSM
	var topts TopicControllerOpts
	topts.StoreScanIntervalSecs = 300
	topts.RootDirectory = opts.DataDirectory
	topts.ControllerID = opts.BrokerID
	fsm.topicController = NewTopicController(topts)
	return &fsm
}

func (fsm *FSM) Publish() {

}

func (fsm *FSM) Subscribe() {

}

func (fsm *FSM) Commit() {

}

func (fsm *FSM) GetLastCommitted() {

}

func (fsm *FSM) AddTopic() {

}

func (fsm *FSM) RemoveTopic() {

}

func (fsm *FSM) GetTopic() {

}

func (fsm *FSM) GetAllTopics() {

}
