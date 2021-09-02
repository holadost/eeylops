package server

import "eeylops/server/base"

type BrokerSelector struct {
}

func (bs *BrokerSelector) GetInstance(topicID base.TopicIDType, partitionID int) (*Broker, error) {
	return nil, nil
}
