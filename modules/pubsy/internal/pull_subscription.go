package internal

import (
	"eventura/modules/pubsy/core"
	"fmt"
	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"sync"
	"time"
)

type PullSubscription struct {
	Consumer                   string
	Topic                      string
	fetchTopicMessageScheduler gocron.Scheduler
	info                       *core.SubscriptionInfo
	fetchTopicMessageMutex     *sync.Mutex
	repos                      *Repos
	log                        *zap.Logger
	handler                    core.MessageHandler
}

func (s *PullSubscription) register() error {
	//TODO competing consumers (e.g. from multiple instances of same app)
	err := s.getOrCreateSubscriptionInfo(s.Topic, s.Consumer)

	if err != nil {
		s.log.Error("Error getting or creating subscription info", zap.Error(err))
		return fmt.Errorf("error getting or creating subscription info: %w", err)
	}

	return nil
}

func (s *PullSubscription) Start() error {
	s.register()
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		s.log.Error("Error creating scheduler", zap.Error(err))
		return fmt.Errorf("error creating scheduler: %w", err)
	}

	s.setupFetchTopicsJob(scheduler)
	s.setupConsumeMessagesJob(scheduler)

	return nil
}

func (s *PullSubscription) setupFetchTopicsJob(scheduler gocron.Scheduler) error {

	_, err := scheduler.NewJob(
		gocron.DurationJob(
			5*time.Second),
		gocron.NewTask(s.fetchTopicMessages),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
		gocron.WithEventListeners(
			gocron.AfterJobRuns(func(jobID uuid.UUID, jobName string) {
				s.log.Info("FetchTopicMessages Job ran", zap.String("job_id", jobID.String()))
			}),
		),
	)
	if err != nil {
		s.log.Error("Error creating job", zap.Error(err))
		return fmt.Errorf("error creating job: %w", err)
	}

	scheduler.Start()

	s.log.Info("Fetch topic messages job started")

	s.fetchTopicMessageScheduler = scheduler

	return nil
}

// Setup consumeMessagesJob
func (s *PullSubscription) setupConsumeMessagesJob(scheduler gocron.Scheduler) error {
	_, err := scheduler.NewJob(
		gocron.DurationJob(
			5*time.Second),
		gocron.NewTask(s.processSubscriptionMessages),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
		gocron.WithEventListeners(
			gocron.AfterJobRuns(func(jobID uuid.UUID, jobName string) {
				s.log.Info("processSubscriptionMessages Job ran", zap.String("job_id", jobID.String()))
			}),
		),
	)
	if err != nil {
		s.log.Error("Error creating job", zap.Error(err))
		return fmt.Errorf("error creating job: %w", err)
	}

	scheduler.Start()

	return nil
}

// Fetch topic messages starting from a stored fetch offset
func (s *PullSubscription) fetchTopicMessages() error {

	messages, err := s.repos.TopicRepo.GetMessagesFromOffset(s.Topic, s.info.LastFetchedOffset, 10)
	if err != nil {
		s.log.Error("Error fetching messages", zap.Error(err))
		return fmt.Errorf("error fetching messages: %w", err)
	}
	for _, msg := range *messages {
		// InsertSubscriptionMessage message to subscription message table
		err = s.repos.SubRepo.InsertSubscriptionMessage(&core.SubscriptionMessage{
			Topic:   msg.Topic,
			Pointer: msg.ID,
			Status:  core.SubscriptionStatusNew,
		})
		s.log.Debug("Saved subscription message", zap.Uint64("message_id", msg.ID))
		if err != nil {
			s.log.Error("Error saving subscription message", zap.Error(err))
			return fmt.Errorf("error saving subscription message: %w", err)
		}
	}
	//check message id of last index and update last fetched offset
	if len(*messages) > 0 {
		s.info.LastFetchedOffset = (*messages)[len(*messages)-1].ID
		err = s.repos.SubRepo.UpdateSubscriptionConsumedOffset(s.info.ID, s.info.LastFetchedOffset)
		if err != nil {
			s.log.Error("Error updating subscription consumed offset", zap.Error(err))
			return fmt.Errorf("error updating subscription consumed offset: %w", err)
		}
	}

	return nil
}

func (s *PullSubscription) processSubscriptionMessages() error {
	messages, err := s.repos.SubRepo.ConsumeMessagesFromConsumedOffsetByStatus(s.Topic, s.Consumer, core.SubscriptionStatusNew, 0, 10)
	if err != nil {
		s.log.Error("Error fetching messages", zap.Error(err))
		return fmt.Errorf("error fetching messages: %w", err)
	}
	s.log.Info("Fetched messages", zap.Int("count", len(*messages)))
	lastConsumedOffset := uint64(0)
	for _, msg := range *messages {
		msg, err := s.repos.TopicRepo.GetMessage(msg.Topic, msg.Pointer)
		if err != nil {
			s.log.Error("Error fetching message", zap.Error(err))
			return fmt.Errorf("error fetching message: %w", err)
		}
		consumerMsg := core.Message{
			ID:   msg.ID,
			Data: msg.Data,
		}

		s.log.Info("Processing message", zap.Uint64("message_id", msg.ID))
		s.repos.SubRepo.UpdateSubscriptionMessageStatus(msg.Topic, msg.ID, core.SubscriptionStatusPending)
		err = s.handler(&consumerMsg)
		s.repos.SubRepo.UpdateSubscriptionMessageStatus(msg.Topic, msg.ID, core.SubscriptionStatusConsumed)
		if err != nil {
			s.log.Error("Error processing message", zap.Error(err))
			break
		}
		lastConsumedOffset = msg.ID

	}

	s.log.Info("Processed messages", zap.Int("count", len(*messages)), zap.Int("last_consumed_offset", int(lastConsumedOffset)))
	err = s.repos.SubRepo.UpdateSubscriptionConsumedOffset(s.info.ID, lastConsumedOffset)
	if err != nil {
		s.log.Error("Error updating subscription consumed offset", zap.Error(err))
		return fmt.Errorf("error updating subscription consumed offset: %w", err)
	}

	return nil
}

func (s *PullSubscription) Stop() error {
	err := s.fetchTopicMessageScheduler.Shutdown()
	if err != nil {
		s.log.Error("Error shutting down scheduler", zap.Error(err))
		return fmt.Errorf("error shutting down scheduler: %w", err)
	}
	return nil
}

func (s *PullSubscription) getOrCreateSubscriptionInfo(topic string, consumer string) error {
	var subInfo *core.SubscriptionInfo
	subInfo, err := s.repos.SubRepo.FindSubscriptionInfoByTopicAndConsumer(topic, consumer)
	if err != nil {
		s.log.Error("Error finding subscription info", zap.Error(err))
		return err
	}
	if subInfo == nil {
		subInfo = &core.SubscriptionInfo{
			Topic:              topic,
			Consumer:           consumer,
			LastConsumedOffset: 0,
			LastFetchedOffset:  0,
		}
		err = s.repos.SubRepo.InsertSubscriptionInfo(subInfo)
		if err != nil {
			s.log.Error("Error saving subscription info", zap.Error(err))
			return err
		}
	}
	s.info = subInfo
	return nil
}

func NewPullSubscription(topic string, consumer string, handle core.MessageHandler, repos *Repos, log *zap.Logger) core.Subscription {
	return &PullSubscription{
		Topic:                  topic,
		repos:                  repos,
		log:                    log,
		fetchTopicMessageMutex: &sync.Mutex{},
		Consumer:               consumer,
		handler:                handle,
	}
}
