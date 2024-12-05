package pubsy

import (
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

type BaseDependency struct {
	Log *zap.Logger
}

func NewBaseDependency(log *zap.Logger) BaseDependency {
	return BaseDependency{Log: log}
}

type DbAwareDependency struct {
	*BaseDependency
	db *pebble.DB
}

func NewDbAwareDependency(base *BaseDependency, db *pebble.DB) DbAwareDependency {
	return DbAwareDependency{
		BaseDependency: base,
		db:             db,
	}
}
