package hedwig

import "eeylops/server/base"

type InstanceSelector struct {
}

func (is *InstanceSelector) GetInstance(topicID base.TopicIDType, partitionID int) (*InstanceManager, error) {
	return nil, nil
}
