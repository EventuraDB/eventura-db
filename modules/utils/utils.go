package utils

import (
	"encoding/binary"
	"go.uber.org/zap"
)

func UintToBytes(value uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)

	return b
}

func BytesToUint(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func HandleLogAndReturn(action func() error, log *zap.Logger) {
	HandleAndLog(action, log)
	return
}

func HandleAndLog(action func() error, log *zap.Logger) {
	err := action()
	if err != nil {
		log.Error("Error during deferred execution", zap.Error(err))
	}
}
