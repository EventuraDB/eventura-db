package internal

import (
	"eventura/modules/pubsy/core"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

type Repos struct {
	TopicRepo core.TopicRepository
	SubRepo   core.SubscriptionRepository
}

func SetupRepos(logger *zap.Logger, storagePath string) (*Repos, error) {
	db, err := pebble.Open(storagePath, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	topicRepo := NewTopicRepositoryImpl(db, logger)
	subRepo := NewSubscriptionRepositoryImpl(db, logger)

	return &Repos{
		TopicRepo: topicRepo,
		SubRepo:   subRepo,
	}, nil
}
