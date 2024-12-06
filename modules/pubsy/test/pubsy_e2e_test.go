package test

import (
	"eventura/modules/pubsy"
	"eventura/modules/pubsy/core"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPubsyE2E(t *testing.T) {

	p := pubsy.NewPubsy(pubsy.WithStoragePath("temp/db"))

	topic := p.Topic("Test-Topic")

	topic.Publish([]byte("Hello, World!"))

	msgCh := make(chan string)

	count := 0

	topic.Subscribe("Test-Consumer", func(message *core.Message) error {
		t.Logf("Received message: %s", string(message.Data))
		count++
		msgCh <- string(message.Data)
		return nil
	})

	// Wait for the message to be consumed
	msg := <-msgCh

	assert.Equal(t, 1, count, "Count should be 1")
	assert.Equal(t, "Hello, World!", msg, "Message should match")
	time.Sleep(5 * time.Second)

}
