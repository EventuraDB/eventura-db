package pubsy

import (
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

// Implements broker interface
type Pubsy struct {
	log     *zap.Logger
	db      *pebble.DB
	baseDep *BaseDependency
}

func (p *Pubsy) Topic(topic string) *Topic {
	return nil
}
