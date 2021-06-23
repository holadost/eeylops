package hedwig

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	"github.com/golang/glog"
	"os"
	"path"
	"sync"
	"time"
)

type TopicController struct {
	topicStore    *storage.TopicStore          // Backing topicStore for topics registered with eeylops.
	consumerStore *storage.ConsumerStore       // Consumer store.
	topicMap      map[string]*hedwigTopicEntry // In memory map that holds the topics and partitions.
	topicMapLock  sync.RWMutex                 // Read-write lock to protect access to topicMap.
	rootDir       string                       // Root directory for this topic controller.
	controllerID  string                       // Controller ID.
	disposer      *storage.StorageDisposer     // Disposer to help remove deleted topics.
	disposedChan  chan string                  // Callback channel after disposer has removed topics.
}

type TopicControllerOpts struct {
	// The root directory for this topic controller.
	RootDirectory string // Root directory for the topic controller.

	// The controller ID. This is the same controller ID that we use for Raft as well. A topic controller is tied
	// to a single raft controller.
	ControllerID string // Controller ID.
}

func NewTopicController(opts TopicControllerOpts) *TopicController {
	tc := &TopicController{}
	tc.rootDir = opts.RootDirectory
	tc.topicStore = storage.NewTopicStore(tc.rootDir)
	tc.consumerStore = storage.NewConsumerStore(tc.rootDir)
	tc.topicMap = make(map[string]*hedwigTopicEntry)
	tc.disposedChan = make(chan string, 200)
	tc.initialize()
	return tc
}

func (tc *TopicController) initialize() {
	// Read all the topics from the topic topicStore and check if the topic directories
	// exist under the given directory.
	glog.Infof("Initializing topic controller. Controller ID: %s", tc.controllerID)
	allTopics, err := tc.topicStore.GetAllTopics()
	if err != nil {
		glog.Fatalf("Unable to get all topics in the topic topicStore due to err: %s", err.Error())
	}
	for _, topic := range allTopics {
		if topic.ToRemove {
			glog.Infof("Skipping initializing topic: %s as it has been marked for removal", topic.Name)
			continue
		}
		glog.Infof("Initializing topic: %s for controller: %s", topic.Name, tc.controllerID)
		topicDir := tc.getTopicRootDirectory(topic.Name)
		if err := os.MkdirAll(topicDir, 0774); err != nil {
			glog.Fatalf("Unable to create topic directory for topic: %s due to err: %s",
				topic.Name, err.Error())
			return
		}
		pMap := make(map[int]*storage.Partition)
		for _, ii := range topic.PartitionIDs {
			pMap[ii] = storage.NewPartition(ii, topicDir, topic.TTLSeconds)
		}
		entry := &hedwigTopicEntry{
			topic:        &topic,
			partitionMap: pMap,
		}
		tc.topicMap[topic.Name] = entry
	}
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
	err := tc.topicStore.AddTopic(topic)
	if err != nil {
		glog.Errorf("Unable to add topic to topic: %s topicStore due to err: %s", topic.Name, err.Error())
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
	if err := tc.topicStore.MarkTopicForRemoval(topicName); err != nil {
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
			// The topic was disposed. Remove it from the topicStore.
			serr := tc.topicStore.RemoveTopic(topicName)
			if serr != nil {
				glog.Fatalf("Unable to remove topic from topic topicStore due to err: %s", serr.Error())
			}
		}
	}
}

func (tc *TopicController) disposeTopics() {
	topics, err := tc.topicStore.GetAllTopics()
	if err != nil {
		glog.Fatalf("Unable to fetch all topics from topic topicStore due to err: %s", err.Error())
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
