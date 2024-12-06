package pubsy

import (
	"eventura/modules/pubsy/core"
	"eventura/modules/pubsy/internal"
	"fmt"
	"go.uber.org/zap"
)

type PubsyImpl struct {
	logger      *zap.Logger
	topics      map[string]core.Topic
	storagePath string
	repos       *internal.Repos
}

func (p *PubsyImpl) Topic(name string) core.Topic {
	if p.topics[name] == nil {
		p.topics[name] = internal.NewDurableTopic(name, p.repos, p.logger)
	}

	return p.topics[name]
}

func (p *PubsyImpl) setupInternalDeps() {
	var err error
	p.repos, err = internal.SetupRepos(p.logger, p.storagePath)
	if err != nil {
		p.logger.Error("Error setting up internal repositories", zap.Error(err))
	}
}

func NewPubsy(opts ...Option) core.Pubsy {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Printf("Error creating logger: %v\n", err)
	}
	p := &PubsyImpl{
		logger: logger,
		topics: make(map[string]core.Topic),
	}
	//Defaults
	opts = append(opts, WithStoragePath("db"))
	opts = append(opts, WithLogger(logger))

	for _, opt := range opts {
		opt(p)
	}

	// Initialize internal repos here using p.storagePath (if set), etc.
	p.setupInternalDeps()

	return p
}

func WithLogger(logger *zap.Logger) Option {
	return func(p *PubsyImpl) {
		p.logger = logger
	}
}

// WithStoragePath allows configuring where messages are stored.
func WithStoragePath(path string) Option {
	return func(p *PubsyImpl) {
		p.storagePath = path
	}
}

// Option functional options to configure Pubsy.
type Option func(impl *PubsyImpl)
