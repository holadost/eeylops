package hedwig

import (
	"eeylops/server/base"
	"eeylops/server/storage"
	"github.com/golang/glog"
)

func doesPartitionExist(topic *base.TopicConfig, partID int) bool {
	found := false
	for _, prtID := range topic.PartitionIDs {
		if partID == prtID {
			found = true
			break
		}
	}
	return found
}

func doesTopicExist(topicID base.TopicIDType, sc *storage.StorageController) (*base.TopicConfig, bool) {
	tpc, err := sc.GetTopicByID(topicID)
	if err != nil {
		if err == storage.ErrTopicNotFound {
			return nil, false
		}
		glog.Fatalf("Unexpected error while attempting to check if topic: %d exists. Error: %s",
			topicID, err.Error())
	}
	return &tpc, true
}
