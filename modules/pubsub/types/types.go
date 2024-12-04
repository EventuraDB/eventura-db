package types

import (
	"eventura/modules/utils"
	"fmt"
)

type Message struct {
	ID    uint64 `json:"id"`
	Data  []byte `json:"data"`
	Topic string `json:"topic"`
}

func (m *Message) Key() []byte {

	fmt.Sprintf("message:%s:%d", m.Topic)

	return utils.UintToBytes(m.ID)
}

// create func(types.Message) as type MessageHandler
type MessageHandler func(Message) error
