package pubsy

import (
	"time"
)

type TopicServiceImpl struct {
	repo *TopicMessageRepository
}

func (t TopicServiceImpl) GetMessagesFromOffset(topic string, offset uint64, limit uint64) (*[]TopicMessage, error) {
	//TODO implement me
	panic("implement me")
}

func (t TopicServiceImpl) GetMessage(topic string, id uint64) (*TopicMessage, error) {
	//TODO implement me
	panic("implement me")
}

func (t TopicServiceImpl) DeleteMessagesOlderThan(topic string, id uint64, duration time.Duration) (*TopicMessage, error) {
	//TODO implement me
	panic("implement me")
}
