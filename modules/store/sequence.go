package store

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"sync/atomic"
)

const SequenceKeyPrefix = "_sequence:"

type SequenceBuffer struct {
	buffer []uint64
	index  int32 // Use int32 for atomic operations
}

type SequenceGenerator struct {
	db       *pebble.DB
	sequence uint64
	key      string
	buffer   *SequenceBuffer
}

// NewSequenceGenerator initializes the SequenceGenerator and loads the current sequence number.
func NewSequenceGenerator(db *pebble.DB, context string, bufferSize int) (*SequenceGenerator, error) {
	key := SequenceKeyPrefix + context
	sg := &SequenceGenerator{
		db:  db,
		key: key,
		buffer: &SequenceBuffer{
			buffer: make([]uint64, bufferSize),
		},
	}

	seq, closer, err := db.Get([]byte(key))
	if err == nil {
		sg.sequence = binary.BigEndian.Uint64(seq)
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return nil, err
	}

	sg.FillBuffer()
	return sg, nil
}

// FillBuffer pre-generates a buffer of sequence numbers atomically.
func (sg *SequenceGenerator) FillBuffer() {
	lastSeq := atomic.LoadUint64(&sg.sequence)
	for i := 0; i < len(sg.buffer.buffer); i++ {
		lastSeq++
		sg.buffer.buffer[i] = lastSeq
	}
	atomic.StoreUint64(&sg.sequence, lastSeq)

	seqBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBytes, lastSeq)
	_ = sg.db.Set([]byte(sg.key), seqBytes, pebble.Sync)

	atomic.StoreInt32(&sg.buffer.index, 0)
}

// GetNextSequence retrieves the next unique sequence number atomically.
func (sg *SequenceGenerator) GetNextSequence() uint64 {
	index := atomic.AddInt32(&sg.buffer.index, 1) - 1

	if int(index) >= len(sg.buffer.buffer) {
		sg.FillBuffer()
		index = 0
		atomic.StoreInt32(&sg.buffer.index, 1) // Set to 1 since the first sequence is being used now
	}

	return sg.buffer.buffer[index]
}
