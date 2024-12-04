package pubsub

import (
	"eventura/modules/pubsub/subscription"
	"eventura/modules/pubsub/topic"
	"eventura/modules/pubsub/types"
	"fmt"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	db        *pebble.DB
	log       *zap.Logger
	consumers map[string]*subscription.Subscription
	wg        sync.WaitGroup
}

func NewBroker(path string) *Broker {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		log.Fatalf("failed to open broker db: %v", err)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to create broker logger: %v", err)
	}

	return &Broker{
		db:        db,
		log:       logger,
		consumers: make(map[string]*subscription.Subscription),
	}
}

// Subscribe to a topic with a subscription function
func (b *Broker) Subscribe(topic string, consumeFunc types.ConsumerFunc) *subscription.Subscription {
	b.wg.Add(1)
	b.consumers[con.ID] = con

	go b.startConsumer(con)

	return con
}

func (b *Broker) startConsumer(consumer *subscription.Subscription) {
	defer b.wg.Done() // Ensure WaitGroup counter is decremented

	offsetKey := fmt.Sprintf("subscription:%s", consumer.ID)
	offsetBytes, closer, err := b.db.Get([]byte(offsetKey))
	if err == nil {
		consumer.offset, _ = strconv.ParseUint(string(offsetBytes), 10, 64)
		closer.Close()
	} else {
		consumer.offset = 0
	}

	b.log.Info("Subscription started", zap.String("consumerID", consumer.ID), zap.Uint64("offset", consumer.offset))

	for {
		select {
		case <-consumer.stopCh:
			b.log.Info("Subscription stopped", zap.String("consumerID", consumer.ID))
			return
		default:
			iter, err := b.db.NewIter(&pebble.IterOptions{
				LowerBound: []byte(fmt.Sprintf("%s:%d", consumer.Topic, consumer.offset+1)),
				UpperBound: []byte(fmt.Sprintf("%s~", consumer.Topic)),
			})
			if err != nil {
				b.log.Error("Failed to create iterator", zap.String("consumerID", consumer.ID), zap.Error(err))
				return
			}

			func() {
				defer iter.Close()

				for iter.First(); iter.Valid(); iter.Next() {
					select {
					case <-consumer.stopCh:
						b.log.Info("Subscription stopped during iteration", zap.String("consumerID", consumer.ID))
						return
					default:
					}

					keyParts := strings.Split(string(iter.Key()), ":")
					if len(keyParts) != 2 {
						b.log.Warn("Invalid message key format", zap.String("key", string(iter.Key())))
						continue
					}

					eventID, err := strconv.ParseUint(keyParts[1], 10, 64)
					if err != nil {
						b.log.Warn("Failed to parse event MessageID", zap.String("key", string(iter.Key())))
						continue
					}

					message := types.Message{ID: eventID, Data: iter.Value()}

					// Retry logic with max attempts
					const maxRetries = 5
					retryCount := 0

					for {
						err = func() (resultErr error) {
							defer func() {
								if r := recover(); r != nil {
									b.log.Error("Subscription function panicked", zap.String("consumerID", consumer.ID), zap.Any("error", r))
									resultErr = fmt.Errorf("subscription panic: %v", r)
								}
							}()
							consumer.ConsumeFunc(message)
							return nil
						}()

						if err == nil {
							break // Success: exit retry loop
						}

						retryCount++
						b.log.Warn("Retrying message after subscription function failure",
							zap.String("consumerID", consumer.ID),
							zap.Uint64("messageID", message.ID),
							zap.Int("retryCount", retryCount),
						)

						if retryCount >= maxRetries {
							b.log.Error("Max retries reached for message. Skipping...",
								zap.String("consumerID", consumer.ID),
								zap.Uint64("messageID", message.ID),
							)
							break
						}

						time.Sleep(time.Duration(retryCount) * time.Second) // Exponential backoff
					}

					// Update offset only after max retries or success
					consumer.offset = eventID
					if err := b.db.Set([]byte(offsetKey), []byte(fmt.Sprintf("%d", consumer.offset)), pebble.Sync); err != nil {
						b.log.Error("Failed to update subscription offset", zap.String("consumerID", consumer.ID), zap.Error(err))
					}
				}

				if err := iter.Error(); err != nil {
					b.log.Error("Iterator encountered an error", zap.String("consumerID", consumer.ID), zap.Error(err))
				}
			}()
		}
	}
}

// Publish a message to a topic
func (b *Broker) Publish(topic topic, message types.Message) {
	key := []byte(fmt.Sprintf("message:%s:%d", topic, time.Now().UnixNano()))
	value := message.Data

	if err := b.db.Set(key, value, pebble.Sync); err != nil {
		b.log.Error("failed to publish message", zap.String("topic", topic), zap.Error(err))
	} else {
		b.log.Info("Message published", zap.String("topic", topic), zap.Uint64("messageID", message.ID))
	}
}

// Stop a subscription
func (b *Broker) StopConsumer(consumer *subscription.Subscription) {
	// close(consumer.stopCh)
}

func (b *Broker) Close() error {
	b.log.Info("Stopping all consumers")
	for _, consumer := range b.consumers {
		consumer.Close()
	}
	b.wg.Wait()
	b.consumers = make(map[string]*subscription.Subscription) // Clear the map

	b.log.Info("Closing broker")
	if err := b.db.Close(); err != nil {
		b.log.Error("Failed to close database", zap.Error(err))
		return err
	}
	return nil
}

func (b *Broker) NewTopic(s string) *topic.Topic {
	return topic.NewTopic(s, b.db, b.log)
}
