package test

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"os"
)

func setupTestDb() (*pebble.DB, func()) {
	db, err := pebble.Open("temp/db", &pebble.Options{})
	if err != nil {
		panic(fmt.Sprintf("Failed to create test Pebble DB: %v", err))
	}
	return db, func() {
		dumpDb(db)
		db.Close()
		os.RemoveAll("temp/db")

	}
}

// dumpDb prints all keys and values from the provided Pebble database.
func dumpDb(db *pebble.DB) {
	iter, _ := db.NewIter(nil)
	defer iter.Close()

	for valid := iter.First(); valid; valid = iter.Next() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("Database dump - Key: %s, Value: %s\n", key, value)
	}
}
