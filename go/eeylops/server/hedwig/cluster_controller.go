package hedwig

import "eeylops/server/replication"

type PeerAddress struct {
	Host string // Host name.
	Port int    // Port number.
}

type ClusterControllerOptions struct {
	DataDirectory string        // Data directory for this ClusterController.
	ClusterID     string        // Broker ID.
	PeerAddresses []PeerAddress // List of peer addresses. The first address must be the current host's address.
}

type ClusterController struct {
	raftHandler     *replication.RaftHandler
	topicController *StorageController
}

func NewClusterController(opts *ClusterControllerOptions) *ClusterController {
	var cc ClusterController
	var topts StorageControllerOpts
	topts.StoreScanIntervalSecs = 300
	topts.RootDirectory = opts.DataDirectory
	topts.ControllerID = opts.ClusterID
	cc.topicController = NewStorageController(topts)
	return &cc
}

func (cc *ClusterController) Publish() {

}

func (cc *ClusterController) Subscribe() {

}

func (cc *ClusterController) Commit() {

}

func (cc *ClusterController) GetLastCommitted() {

}

func (cc *ClusterController) AddTopic() {

}

func (cc *ClusterController) RemoveTopic() {

}

func (cc *ClusterController) GetTopic() {

}

func (cc *ClusterController) GetAllTopics() {

}
