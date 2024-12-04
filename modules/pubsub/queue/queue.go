package queue

import (
	"eventura/modules/utils"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
)

type FifoQueue struct {
	db     *pebble.DB
	prefix string
	mu     sync.Mutex
	head   uint64 // Points to the first unprocessed item
	tail   uint64 // Points to the next empty slot
}

// NewFifoQueue initializes a new FIFO queue with a given prefix.
func NewFifoQueue(prefix string) *FifoQueue {
	db, _ := pebble.Open(fmt.Sprintf("db/%squeue.db", prefix), &pebble.Options{}) //TODO path
	q := &FifoQueue{
		db:     db,
		prefix: prefix,
		head:   0,
		tail:   0,
	}

	// Load head and tail pointers from the database
	if err := q.loadPointers(); err != nil {
		return nil
	}

	return q
}

// Push adds a new item (raw bytes) to the queue.
func (q *FifoQueue) Push(value []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Store the value in the database
	key := q.keyForIndex(q.tail)
	if err := q.db.Set([]byte(key), value, pebble.Sync); err != nil {
		return err
	}

	// Increment the tail pointer
	q.tail++
	return q.storePointers()
}

// Remove retrieves and removes the next item (raw bytes) from the queue.
func (q *FifoQueue) Remove() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.isEmpty() {
		return nil, fmt.Errorf("queue is empty")
	}

	// Retrieve the value from the database
	key := q.keyForIndex(q.head)
	value, closer, err := q.db.Get([]byte(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("queue is empty")
		}
		return nil, err
	}
	defer closer.Close()

	// Remove the value from the database
	if err := q.db.Delete([]byte(key), pebble.Sync); err != nil {
		return nil, err
	}

	// Increment the head pointer
	q.head++
	if err := q.storePointers(); err != nil {
		return nil, err
	}

	return value, nil
}

// Peek retrieves the next item (raw bytes) from the queue without removing it.
func (q *FifoQueue) Peek() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.isEmpty() {
		return nil, fmt.Errorf("queue is empty")
	}

	// Retrieve the value from the database
	key := q.keyForIndex(q.head)
	value, closer, err := q.db.Get([]byte(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("queue is empty")
		}
		return nil, err
	}
	defer closer.Close()

	return value, nil
}

// IsEmpty checks if the queue is empty.
func (q *FifoQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.isEmpty()
}

// internal: Check if the queue is empty without locking
func (q *FifoQueue) isEmpty() bool {
	return q.head == q.tail
}

// internal: Generate a key for a specific index
func (q *FifoQueue) keyForIndex(index uint64) string {
	return fmt.Sprintf("%s:%d", q.prefix, index)
}

// internal: Load head and tail pointers from the database
func (q *FifoQueue) loadPointers() error {
	headKey := fmt.Sprintf("%s:head", q.prefix)
	tailKey := fmt.Sprintf("%s:tail", q.prefix)

	// Load the head pointer
	headData, closer, err := q.db.Get([]byte(headKey))
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}
	if err == nil {
		q.head = utils.BytesToUint(headData)
	}

	// Load the tail pointer
	tailData, closer, err := q.db.Get([]byte(tailKey))
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}
	if err == nil {
		q.tail = utils.BytesToUint(tailData)
	}

	return nil
}

// internal: Store head and tail pointers in the database
func (q *FifoQueue) storePointers() error {
	headKey := fmt.Sprintf("%s:head", q.prefix)
	tailKey := fmt.Sprintf("%s:tail", q.prefix)

	if err := q.db.Set([]byte(headKey), utils.UintToBytes(q.head), pebble.Sync); err != nil {
		return err
	}
	if err := q.db.Set([]byte(tailKey), utils.UintToBytes(q.tail), pebble.Sync); err != nil {
		return err
	}

	return nil
}
