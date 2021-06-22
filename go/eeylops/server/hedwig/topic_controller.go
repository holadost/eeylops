package hedwig

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	"github.com/golang/glog"
	"path"
	"sync"
	"time"
)

type TopicController struct {
	store        *storage.TopicStore          // Backing store for topics registered with eeylops.
	topicMap     map[string]*hedwigTopicEntry // In memory map that holds the topics and partitions.
	topicMapLock sync.RWMutex                 // Read-write lock to protect access to topicMap.
	rootDir      string
	disposer     *storage.StorageDisposer
	disposedChan chan string
}

func NewTopicController(rootDir string) *TopicController {
	tc := &TopicController{}
	tc.rootDir = rootDir
	tc.store = storage.NewTopicStore(tc.rootDir)
	tc.topicMap = make(map[string]*hedwigTopicEntry)
	tc.disposedChan = make(chan string, 200)
	tc.initialize()
	return tc
}

func (tc *TopicController) initialize() {
	// Read all the topics from the topic store and check if the topic directories
	// exist under the given directory.
}

func (tc *TopicController) GetTopic(topicName string) (base.Topic, error) {
	tc.topicMapLock.RLock()
	defer tc.topicMapLock.RUnlock()
	val, exists := tc.topicMap[topicName]
	if !exists {
		glog.Errorf("Did not find any topic named: %s", topicName)
		return base.Topic{}, ErrTopicNotFound
	}
	return *val.topic, nil
}

func (tc *TopicController) AddTopic(topic base.Topic) error {
	tc.topicMapLock.Lock()
	defer tc.topicMapLock.Unlock()
	_, exists := tc.topicMap[topic.Name]
	if exists {
		glog.Errorf("Topic: %s already exists", topic.Name)
		return ErrTopicExists
	}
	err := tc.store.AddTopic(topic)
	if err != nil {
		glog.Errorf("Unable to add topic to topic: %s store due to err: %s", topic.Name, err.Error())
		return ErrInstanceTopicManager
	}
	partMap := make(map[int]*storage.Partition)
	for _, elem := range topic.PartitionIDs {
		part := storage.NewPartition(elem, tc.getTopicRootDirectory(topic.Name), topic.TTLSeconds)
		partMap[elem] = part
	}
	entry := &hedwigTopicEntry{
		topic:        &topic,
		partitionMap: partMap,
	}
	tc.topicMap[topic.Name] = entry
	return nil
}

func (tc *TopicController) RemoveTopic(topicName string) error {
	tc.topicMapLock.Lock()
	defer tc.topicMapLock.Unlock()
	_, exists := tc.topicMap[topicName]
	if !exists {
		glog.Errorf("Topic: %s does not exist. Cannot remove topic", topicName)
		return ErrTopicNotFound
	}
	if err := tc.store.MarkTopicForRemoval(topicName); err != nil {
		glog.Errorf("Unable to mark topic: %s for removal due to err: %s", topicName, err.Error())
		return ErrInstanceTopicManager
	}
	delete(tc.topicMap, topicName)
	return nil
}

func (tc *TopicController) GetPartition(topicName string, partitionID int) (*storage.Partition, error) {
	tc.topicMapLock.RLock()
	defer tc.topicMapLock.RUnlock()
	entry, exists := tc.topicMap[topicName]
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

func (tc *TopicController) getTopicRootDirectory(topicName string) string {
	return path.Join(base.GetDataDirectory(), "topics", topicName)
}

/********************************************** TOPICS JANITOR ********************************************************/
// janitor is a long running background goroutine that periodically checks which topics have been marked for removal and
// removes those topics from the underlying storage.
func (tc *TopicController) janitor() {
	disposeTicker := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-disposeTicker.C:
			tc.disposeTopics()
		case topicName := <-tc.disposedChan:
			// The topic was disposed. Remove it from the store.
			serr := tc.store.RemoveTopic(topicName)
			if serr != nil {
				glog.Fatalf("Unable to remove topic from topic store due to err: %s", serr.Error())
			}
		}
	}
}

func (tc *TopicController) disposeTopics() {
	topics, err := tc.store.GetAllTopics()
	if err != nil {
		glog.Fatalf("Unable to fetch all topics from topic store due to err: %s", err.Error())
	}
	for _, topic := range topics {
		if topic.ToRemove {
			ds := storage.DefaultDisposer()
			ds.Dispose(tc.getTopicRootDirectory(topic.Name), tc.createDisposeCb(topic.Name))
		}
	}
}

func (tc *TopicController) createDisposeCb(topicName string) func(error) {
	cb := func(err error) {
		if err != nil {
			return
		}
		glog.Infof("Topic: %s has been successfully disposed", topicName)
		tc.disposedChan <- topicName
	}
	return cb
}

type hedwigTopicEntry struct {
	topic        *base.Topic
	partitionMap map[int]*storage.Partition
}
