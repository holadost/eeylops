package hedwig

import (
	"eeylops/server/base"
	"eeylops/server/storage"
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
		glog.Errorf("Did not find any topic named: %s", topicName)
		return base.Topic{}, ErrTopicNotFound
	}
	return *val.topic, nil
}

func (tm *InstanceTopicManager) AddTopic(topic base.Topic) error {
	tm.topicMapLock.Lock()
	defer tm.topicMapLock.Unlock()
	_, exists := tm.topicMap[topic.Name]
	if exists {
		glog.Errorf("Topic: %s already exists", topic.Name)
		return ErrTopicExists
	}
	err := tm.store.AddTopic(topic)
	if err != nil {
		glog.Errorf("Unable to add topic to topic: %s store due to err: %s", topic.Name, err.Error())
		return ErrInstanceTopicManager
	}
	partMap := make(map[int]*storage.Partition)
	for _, elem := range topic.PartitionIDs {
		part := storage.NewPartition(elem, tm.getTopicRootDirectory(topic.Name), topic.TTLSeconds)
		partMap[elem] = part
	}
	entry := &hedwigTopicEntry{
		topic:        &topic,
		partitionMap: partMap,
	}
	tm.topicMap[topic.Name] = entry
	return nil
}

func (tm *InstanceTopicManager) RemoveTopic(topicName string) error {
	tm.topicMapLock.Lock()
	defer tm.topicMapLock.Unlock()
	_, exists := tm.topicMap[topicName]
	if !exists {
		glog.Errorf("Topic: %s does not exist. Cannot remove topic", topicName)
		return ErrTopicNotFound
	}
	if err := tm.store.MarkTopicForRemoval(topicName); err != nil {
		glog.Errorf("Unable to mark topic: %s for removal due to err: %s", topicName, err.Error())
		return ErrInstanceTopicManager
	}
	delete(tm.topicMap, topicName)
	return nil
}

func (tm *InstanceTopicManager) GetPartition(topicName string, partitionID int) (*storage.Partition, error) {
	tm.topicMapLock.RLock()
	defer tm.topicMapLock.RUnlock()
	entry, exists := tm.topicMap[topicName]
	if !exists {
		glog.Errorf("Unable to find topic: %s", topicName)
		return nil, ErrTopicNotFound
	}
	partition, exists := entry.partitionMap[partitionID]
	if !exists {
		glog.Errorf("Unable to find partition: %d for topic: %s", partitionID, topicName)
		return nil, ErrPartitionNotFound
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
