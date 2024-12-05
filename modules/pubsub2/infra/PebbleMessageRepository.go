package infra

import (
	"eventura/modules/pubsub2/domain"
	"github.com/cockroachdb/pebble"
)

type PebbleMessageRepository struct {
	db *pebble.DB
}

func NewPebbleMessageRepository() domain.MessageRepository {
	db, _ := pebble.Open("sdf", nil)
	return &PebbleMessageRepository{db: db}

}

func (r *PebbleMessageRepository) SaveMessage(message *domain.Message) error {
	return nil
}
