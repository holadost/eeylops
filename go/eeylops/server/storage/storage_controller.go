package storage

import (
	"eeylops/server/base"
	"eeylops/server/hedwig"
	"github.com/golang/glog"
	"os"
	"path"
	"sync"
	"time"
)

type StorageController struct {
	topicStore            *TopicStore            // Backing topicStore for topics registered with eeylops.
	consumerStore         *ConsumerStore         // Consumer store.
	topicMap              map[string]*topicEntry // In memory map that holds the topics and partitions.
	topicMapLock          sync.RWMutex           // Read-write lock to protect access to topicMap.
	rootDir               string                 // Root directory for this topic controller.
	controllerID          string                 // Controller ID.
	storeScanIntervalSecs int                    // Store scan interval in seconds.
	disposer              *StorageDisposer       // Disposer to help remove deleted topics.
	disposedChan          chan string            // Callback channel after disposer has removed topics.
}

type StorageControllerOpts struct {
	// The root directory for this topic controller.
	RootDirectory string // Root directory for the topic controller.

	// The controller ID. This is the same controller ID that we use for Raft as well. A topic controller is tied
	// to a single raft controller.
	ControllerID string // Controller ID.

	// The interval at which the topic store is scanned by the janitor to dispose removed topics.
	StoreGCScanIntervalSecs int
}

func NewStorageController(opts StorageControllerOpts) *StorageController {
	sc := &StorageController{}
	sc.rootDir = opts.RootDirectory
	sc.controllerID = opts.ControllerID
	sc.storeScanIntervalSecs = opts.StoreGCScanIntervalSecs
	if sc.controllerID == "" {
		glog.Fatalf("No controller ID provided")
	}
	sc.initialize()
	return sc
}

func (sc *StorageController) initialize() {
	sc.topicStore = NewTopicStore(sc.getControllerRootDirectory())
	sc.consumerStore = NewConsumerStore(sc.getControllerRootDirectory())
	sc.topicMap = make(map[string]*topicEntry)
	sc.disposedChan = make(chan string, 200)

	// Read all the topics from the topic topicStore and check if the topic directories
	// exist under the given directory.
	glog.Infof("Initializing topic controller. Controller ID: %s", sc.controllerID)
	allTopics, err := sc.topicStore.GetAllTopics()
	if err != nil {
		glog.Fatalf("Unable to get all topics in the topic topicStore due to err: %s", err.Error())
	}
	for _, topic := range allTopics {
		if topic.ToRemove {
			glog.Infof("Skipping initializing topic: %s as it has been marked for removal", topic.Name)
			continue
		}
		glog.Infof("Initializing topic: %s for controller: %s", topic.Name, sc.controllerID)

		topicDir := sc.getTopicRootDirectory(topic.Name)
		if err := os.MkdirAll(topicDir, 0774); err != nil {
			glog.Fatalf("Unable to create topic directory for topic: %s due to err: %s",
				topic.Name, err.Error())
			return
		}
		pMap := make(map[int]*Partition)
		for _, ii := range topic.PartitionIDs {
			opts := PartitionOpts{
				TopicName:     topic.Name,
				PartitionID:   ii,
				RootDirectory: topicDir,
				TTLSeconds:    topic.TTLSeconds,
			}
			pMap[ii] = NewPartition(opts)
		}
		entry := &topicEntry{
			topic:        &topic,
			partitionMap: pMap,
		}
		sc.topicMap[topic.Name] = entry
	}
	go sc.janitor()
}

func (sc *StorageController) GetTopic(topicName string) (base.Topic, error) {
	sc.topicMapLock.RLock()
	defer sc.topicMapLock.RUnlock()
	val, exists := sc.topicMap[topicName]
	if !exists {
		glog.Errorf("Did not find any topic named: %s", topicName)
		return base.Topic{}, ErrTopicNotFound
	}
	return *val.topic, nil
}

func (sc *StorageController) AddTopic(topic base.Topic) error {
	sc.topicMapLock.Lock()
	defer sc.topicMapLock.Unlock()
	_, exists := sc.topicMap[topic.Name]
	if exists {
		glog.Errorf("Topic: %s already exists", topic.Name)
		return hedwig.ErrTopicExists
	}
	if err := os.MkdirAll(sc.getTopicRootDirectory(topic.Name), 0774); err != nil {
		glog.Errorf("Unable to create directory for topic: %s due to err: %s", topic.Name, err.Error())
		return hedwig.ErrTopicController
	}
	if err := sc.topicStore.AddTopic(topic); err != nil {
		glog.Errorf("Unable to add topic to topic: %s topicStore due to err: %s", topic.Name, err.Error())
		return hedwig.ErrTopicController
	}
	partMap := make(map[int]*Partition)
	for _, elem := range topic.PartitionIDs {
		opts := PartitionOpts{
			TopicName:     topic.Name,
			PartitionID:   elem,
			RootDirectory: sc.getTopicRootDirectory(topic.Name),
			TTLSeconds:    topic.TTLSeconds,
		}
		part := NewPartition(opts)
		partMap[elem] = part
	}
	entry := &topicEntry{
		topic:        &topic,
		partitionMap: partMap,
	}
	sc.topicMap[topic.Name] = entry
	return nil
}

func (sc *StorageController) RemoveTopic(topicName string) error {
	sc.topicMapLock.Lock()
	defer sc.topicMapLock.Unlock()
	_, exists := sc.topicMap[topicName]
	if !exists {
		glog.Errorf("Topic: %s does not exist. Cannot remove topic", topicName)
		return hedwig.ErrTopicNotFound
	}
	if err := sc.topicStore.MarkTopicForRemoval(topicName); err != nil {
		glog.Errorf("Unable to mark topic: %s for removal due to err: %s", topicName, err.Error())
		return hedwig.ErrTopicController
	}
	delete(sc.topicMap, topicName)
	return nil
}

func (sc *StorageController) GetPartition(topicName string, partitionID int) (*Partition, error) {
	sc.topicMapLock.RLock()
	defer sc.topicMapLock.RUnlock()
	entry, exists := sc.topicMap[topicName]
	if !exists {
		glog.Errorf("Unable to find topic: %s", topicName)
		return nil, hedwig.ErrTopicNotFound
	}
	partition, exists := entry.partitionMap[partitionID]
	if !exists {
		glog.Errorf("Unable to find partition: %d for topic: %s", partitionID, topicName)
		return nil, hedwig.ErrPartitionNotFound
	}
	return partition, nil
}

func (sc *StorageController) getControllerRootDirectory() string {
	return path.Join(sc.rootDir, sc.controllerID)
}

func (sc *StorageController) getTopicRootDirectory(topicName string) string {
	return path.Join(sc.getControllerRootDirectory(), "topics", topicName)
}

/********************************************** TOPICS JANITOR ********************************************************/
// janitor is a long running background goroutine that periodically checks which topics have been marked for removal and
// removes those topics from the underlying storage.
func (sc *StorageController) janitor() {
	glog.Infof("Starting janitor for topic controller: %s", sc.controllerID)
	disposeTicker := time.NewTicker(time.Duration(sc.storeScanIntervalSecs) * time.Second)
	for {
		select {
		case <-disposeTicker.C:
			sc.disposeTopics()
		case topicName := <-sc.disposedChan:
			// The topic was disposed. Remove it from the topicStore.
			serr := sc.topicStore.RemoveTopic(topicName)
			if serr != nil {
				glog.Fatalf("Unable to remove topic: %s from topic store due to err: %s",
					topicName, serr.Error())
			}
		}
	}
}

func (sc *StorageController) disposeTopics() {
	topics, err := sc.topicStore.GetAllTopics()
	if err != nil {
		glog.Fatalf("Unable to fesch all topics from topic topicStore due to err: %s", err.Error())
	}
	for _, topic := range topics {
		if topic.ToRemove {
			ds := DefaultDisposer()
			ds.Dispose(sc.getTopicRootDirectory(topic.Name), sc.createDisposeCb(topic.Name))
		}
	}
}

func (sc *StorageController) createDisposeCb(topicName string) func(error) {
	cb := func(err error) {
		if err != nil {
			return
		}
		glog.Infof("Topic: %s has been successfully disposed", topicName)
		sc.disposedChan <- topicName
	}
	return cb
}

type topicEntry struct {
	topic        *base.Topic
	partitionMap map[int]*Partition
}
