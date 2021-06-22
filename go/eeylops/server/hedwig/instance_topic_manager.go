package hedwig

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	"fmt"
	"github.com/golang/glog"
	"path"
	"sync"
	"time"
)

type InstanceTopicManager struct {
	store        *storage.TopicStore          // Backing store for topics registered with eeylops.
	topicMap     map[string]*hedwigTopicEntry // In memory map that holds the topics and partitions.
	topicMapLock sync.RWMutex                 // Read-write lock to protect access to topicMap.
	rootDir      string
	disposer     *storage.StorageDisposer
	disposedChan chan string
}

func NewTopicManager(rootDir string) *InstanceTopicManager {
	tm := &InstanceTopicManager{}
	tm.rootDir = rootDir
	tm.store = storage.NewTopicStore(tm.rootDir)
	tm.topicMap = make(map[string]*hedwigTopicEntry)
	tm.disposedChan = make(chan string, 200)
	tm.initialize()
	return tm
}

func (tm *InstanceTopicManager) initialize() {
	// Read all the topics from the topic store and check if the topic directories
	// exist under the given directory.
}

func (tm *InstanceTopicManager) GetTopic(topicName string) (base.Topic, error) {
	tm.topicMapLock.RLock()
	defer tm.topicMapLock.RUnlock()
	val, exists := tm.topicMap[topicName]
	if !exists {
		return base.Topic{}, fmt.Errorf("no topic named: %s found in topic map", topicName)
	}
	return *val.topic, nil
}

func (tm *InstanceTopicManager) AddTopic(topic base.Topic) error {
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

func (tm *InstanceTopicManager) RemoveTopic(topicName string) error {
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

func (tm *InstanceTopicManager) GetPartition(topicName string, partitionID int) (*storage.Partition, error) {
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

func (tm *InstanceTopicManager) getTopicRootDirectory(topicName string) string {
	return path.Join(base.GetDataDirectory(), "topics", topicName)
}

/********************************************** TOPICS JANITOR ********************************************************/
// janitor is a long running background goroutine that periodically checks which topics have been marked for removal and
// removes those topics from the underlying storage.
func (tm *InstanceTopicManager) janitor() {
	disposeTicker := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-disposeTicker.C:
			tm.disposeTopics()
		case topicName := <-tm.disposedChan:
			// The topic was disposed. Remove it from the store.
			serr := tm.store.RemoveTopic(topicName)
			if serr != nil {
				glog.Fatalf("Unable to remove topic from topic store due to err: %s", serr.Error())
			}
		}
	}
}

func (tm *InstanceTopicManager) disposeTopics() {
	topics, err := tm.store.GetAllTopics()
	if err != nil {
		glog.Fatalf("Unable to fetch all topics from topic store due to err: %s", err.Error())
	}
	for _, topic := range topics {
		if topic.ToRemove {
			ds := storage.DefaultDisposer()
			ds.Dispose(tm.getTopicRootDirectory(topic.Name), tm.createDisposeCb(topic.Name))
		}
	}
}

func (tm *InstanceTopicManager) createDisposeCb(topicName string) func(error) {
	cb := func(err error) {
		if err != nil {
			return
		}
		glog.Infof("Topic: %s has been successfully disposed", topicName)
		tm.disposedChan <- topicName
	}
	return cb
}

type hedwigTopicEntry struct {
	topic        *base.Topic
	partitionMap map[int]*storage.Partition
}
