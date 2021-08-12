package hedwig

import (
	"eeylops/server/replication"
	"eeylops/server/storage"
)

type PeerAddress struct {
	Host string // Host name.
	Port int    // Port number.
}

type InstanceManagerOpts struct {
	DataDirectory string        // Data directory for this InstanceManager.
	ClusterID     string        // Cluster ID.
	PeerAddresses []PeerAddress // List of peer addresses. The first address must be the current host's address.
}

// InstanceManager manages the replication and storage controller for the node.
type InstanceManager struct {
	replicationController *replication.RaftController
	storageController     *storage.StorageController
}

func NewClusterController(opts *InstanceManagerOpts) *InstanceManager {
	var im InstanceManager
	var topts storage.StorageControllerOpts
	topts.StoreGCScanIntervalSecs = 300
	topts.RootDirectory = opts.DataDirectory
	topts.ControllerID = opts.ClusterID
	im.storageController = storage.NewStorageController(topts)
	return &im
}

func (im *InstanceManager) Publish() {

}

func (im *InstanceManager) Subscribe() {

}

func (im *InstanceManager) Commit() {

}

func (im *InstanceManager) GetLastCommitted() {

}

func (im *InstanceManager) AddTopic() {

}

func (im *InstanceManager) RemoveTopic() {

}

func (im *InstanceManager) GetTopic() {

}

func (im *InstanceManager) GetAllTopics() {

}
