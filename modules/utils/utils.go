package utils

import (
	"encoding/binary"
	"fmt"
	"go.uber.org/zap"
	"strconv"
)

func UintToBytes(value uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)
	return b
}

func StringToUint64(value string) (uint64, error) {
	return strconv.ParseUint(value, 10, 64)
}

func BytesToUint(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid byte slice length: expected 8, got %d", len(b))
	}
	return binary.BigEndian.Uint64(b), nil
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
