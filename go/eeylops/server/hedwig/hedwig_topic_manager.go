package hedwig

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	"fmt"
	"path"
	"sync"
)

type HedwigTopicManager struct {
	store        *storage.TopicStore          // Backing store for topics registered with eeylops.
	topicMap     map[string]*hedwigTopicEntry // In memory map that holds the topics and partitions.
	topicMapLock sync.RWMutex                 // Read-write lock to protect access to topicMap.
	rootDir      string
}

func NewTopicManager(rootDir string) *HedwigTopicManager {
	tm := &HedwigTopicManager{}
	tm.rootDir = rootDir
	tm.store = storage.NewTopicStore(tm.rootDir)
	tm.topicMap = make(map[string]*hedwigTopicEntry)
	tm.initialize()
	return tm
}

func (tm *HedwigTopicManager) initialize() {
	// Read all the topics from the topic store and check if the topic directories
	// exist under the given directory.
}

func (tm *HedwigTopicManager) GetTopic(topicName string) (base.Topic, error) {
	tm.topicMapLock.RLock()
	defer tm.topicMapLock.RUnlock()
	val, exists := tm.topicMap[topicName]
	if !exists {
		return base.Topic{}, fmt.Errorf("no topic named: %s found in topic map", topicName)
	}
	return *val.topic, nil
}

func (tm *HedwigTopicManager) AddTopic(topic base.Topic) error {
	tm.topicMapLock.RLock()
	_, exists := tm.topicMap[topic.Name]
	tm.topicMapLock.RUnlock()
	if exists {
		// TODO: Create error codes. This error can actually be safely ignored by all FSMs and we need not crash.
		return fmt.Errorf("topic named: %s already exists", topic.Name)
	}
	err := tm.store.AddTopic(topic)
	if err != nil {
		return err
	}
	partMap := make(map[int]*storage.Partition)
	for _, elem := range topic.PartitionIDs {
		part := storage.NewPartition(int(elem), tm.getTopicRootDirectory(topic.Name), 86400*7)
		partMap[int(elem)] = part
	}
	entry := &hedwigTopicEntry{
		topic:        &topic,
		partitionMap: partMap,
	}
	tm.topicMapLock.Lock()
	defer tm.topicMapLock.Unlock()
	tm.topicMap[topic.Name] = entry
	return nil
}

func (tm *HedwigTopicManager) RemoveTopic(topicName string) error {
	tm.topicMapLock.Lock()
	defer tm.topicMapLock.Unlock()
	_, exists := tm.topicMap[topicName]
	if !exists {
		return fmt.Errorf("cannot remove topic: %s as it does not exist", topicName)
	}
	if err := tm.store.MarkTopicForRemoval(topicName); err != nil {
		return fmt.Errorf("unable to mark topic for removal due to err: %w", err)
	}
	delete(tm.topicMap, topicName)
	return nil
}

func (tm *HedwigTopicManager) GetPartition(topicName string, partitionID int) (*storage.Partition, error) {
	tm.topicMapLock.RLock()
	defer tm.topicMapLock.RUnlock()
	entry, exists := tm.topicMap[topicName]
	if !exists {
		return nil, fmt.Errorf("cannot find topic: %s", topicName)
	}
	partition, exists := entry.partitionMap[partitionID]
	if !exists {
		return nil, fmt.Errorf("cannot find partition: %d for topic: %s", partitionID, topicName)
	}
	return partition, nil
}

func (tm *HedwigTopicManager) getTopicRootDirectory(topicName string) string {
	return path.Join(base.GetDataDirectory(), "topics", topicName)
}

/********************************************** TOPICS JANITOR ********************************************************/
// janitor is a long running background goroutine that checks which topics have been marked for removal and
// periodically clears all the partitions of that topic from the underlying storage.
func (tm *HedwigTopicManager) janitor() {

}

type hedwigTopicEntry struct {
	topic        *base.Topic
	partitionMap map[int]*storage.Partition
}
