package test

import (
	"eventura/modules/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCompositeKey(t *testing.T) {
	ck := utils.NewCompositeKey("users")
	ck.AddString("John1")
	ck.AddUint64(123234234)
	key := ck.Build()

	fmt.Printf("Generated Key: %s\n", key)

	err := ck.Parse(key)
	if err != nil {
		t.Fatalf("Failed to parse key: %v", err)
	}

	name, err := ck.GetString()
	if err != nil {
		t.Fatalf("Failed to get name: %v", err)
	}

	id, err := ck.GetUint64()
	if err != nil {
		t.Fatalf("Failed to get id: %v", err)
	}

	prefix := ck.GetPrefix()

	assert.Equal(t, "users", prefix)
	assert.Equal(t, "John1", name)
	assert.Equal(t, uint64(123234234), id)

	fmt.Println("Parsed Prefix:", prefix)
	fmt.Println("Parsed Name:", name)
	fmt.Println("Parsed ID:", id)
}

func TestParseRawSingleByteKey(t *testing.T) {
	// Simulate a raw key retrieved from the database
	// Format: prefix + ":" + single-string-field + ":"
	// Here: "users:A:"
	key := []byte("users:A")

	// Create a CompositeKey with the known prefix.
	// We didn't build it via AddString, we just know the prefix beforehand.
	ck := utils.NewCompositeKey("users")

	// Parse the raw key directly
	if err := ck.Parse(key); err != nil {
		t.Fatalf("Failed to parse raw key: %v", err)
	}

	// Now retrieve the first (and only) field as a string
	val, err := ck.GetString()
	if err != nil {
		t.Fatalf("Failed to get string field: %v", err)
	}

	// Check that the value matches what we expect
	if val != "A" {
		t.Errorf("Expected 'A', got %q", val)
	}
}
