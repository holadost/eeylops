package hedwig

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	"sync"
)

type HedwigTopicManager struct {
	store        *storage.TopicStore          // Backing store for topics registered with eeylops.
	topicMap     map[string]*hedwigTopicEntry // In memory map that holds the topics and partitions.
	topicMapLock sync.RWMutex                 // Read-write lock to protect access to topicMap.
}

func NewTopicManager(rootDir string) *HedwigTopicManager {
	tm := &HedwigTopicManager{}
	tm.store = storage.NewTopicStore(rootDir)
	tm.topicMap = make(map[string]*hedwigTopicEntry)
	tm.initialize()
	return tm
}

func (tm *HedwigTopicManager) initialize() {
	// Read all the topics from the topic store and check if the topic directories
	// exist under the given directory.
}

func (tm *HedwigTopicManager) GetTopic(topicName string) {

}

func (tm *HedwigTopicManager) AddTopic(topic base.Topic) {

}

func (tm *HedwigTopicManager) RemoveTopic(topicName string) {
	// Mark topic for removal. The background janitors should take care of it.
}

func (tm *HedwigTopicManager) GetPartition(topicName string, partitionID int) {

}

type hedwigTopicEntry struct {
	topic        *base.Topic
	partitionMap map[int]*storage.Partition
}
