package pubsy

type PullSubscription struct {
	Consumer string
	Topic    string
	subRepo  SubscriptionMessageRepository
}

func (s *PullSubscription) Start() error {
	return nil
}

func (s *PullSubscription) Stop() error {
	return nil
}

func NewPullSubscription(consumer string, subRepo SubscriptionMessageRepository) *PullSubscription {
	return &PullSubscription{
		Consumer: consumer,
		subRepo:  subRepo,
	}
}
