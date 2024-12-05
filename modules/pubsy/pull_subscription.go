package pubsy

type PullSubscription struct {
	Consumer string
	Topic    string
}

func (s *PullSubscription) Start() error {
	return nil
}

func (s *PullSubscription) Stop() error {
	return nil
}

func NewPullSubscription(consumer string) *PullSubscription {
	return &PullSubscription{
		Consumer: consumer,
	}
}
