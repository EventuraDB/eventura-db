package utils

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const delimiter = ':'

var (
	errNoDelimiter    = errors.New("no delimiter found")
	errNotEnoughBytes = errors.New("not enough bytes")
)

type fieldKind int

const (
	fieldString fieldKind = iota
	fieldUint64
)

type field struct {
	kind fieldKind
	sval string
	uval uint64
}

type CompositeKey struct {
	prefix string
	fields []field
	data   []byte
	pos    int
}

// NewCompositeKey creates a new CompositeKey with a prefix.
func NewCompositeKey(prefix string) *CompositeKey {
	return &CompositeKey{
		prefix: prefix,
	}
}

// AddString queues a string field.
func (ck *CompositeKey) AddString(value string) *CompositeKey {
	ck.fields = append(ck.fields, field{kind: fieldString, sval: value})
	return ck
}

// AddUint64 queues a uint64 field.
func (ck *CompositeKey) AddUint64(value uint64) *CompositeKey {
	ck.fields = append(ck.fields, field{kind: fieldUint64, uval: value})
	return ck
}

// Build constructs the final key:
func (ck *CompositeKey) Build() []byte {
	var buf bytes.Buffer
	buf.WriteString(ck.prefix)
	buf.WriteByte(delimiter)

	for i, f := range ck.fields {
		isLast := (i == len(ck.fields)-1)
		switch f.kind {
		case fieldString:
			buf.WriteString(f.sval)
			if !isLast {
				buf.WriteByte(delimiter)
			}
		case fieldUint64:
			uBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(uBuf, f.uval)
			buf.Write(uBuf)
			if !isLast {
				buf.WriteByte(delimiter)
			}
		}
	}

	return buf.Bytes()
}

// Parse prepares the key for sequential Get operations.
func (ck *CompositeKey) Parse(key []byte) error {
	ck.data = key
	ck.pos = 0

	// Parse prefix
	p, err := ck.readStringFieldAllowNoDelimiter()
	if err != nil {
		return fmt.Errorf("failed to parse prefix: %w", err)
	}
	if p != ck.prefix {
		return fmt.Errorf("prefix mismatch: expected %q, got %q", ck.prefix, p)
	}
	return nil
}

// GetPrefix returns the prefix.
func (ck *CompositeKey) GetPrefix() string {
	return ck.prefix
}

// GetString reads the next field as a string.
// If this is the last field and there's no trailing delimiter, it returns the rest of the data.
func (ck *CompositeKey) GetString() (string, error) {
	return ck.readStringFieldAllowNoDelimiter()
}

// GetUint64 reads the next field as a uint64.
func (ck *CompositeKey) GetUint64() (uint64, error) {
	// Need at least 8 bytes
	if ck.pos+8 > len(ck.data) {
		return 0, errNotEnoughBytes
	}
	val := binary.BigEndian.Uint64(ck.data[ck.pos : ck.pos+8])
	ck.pos += 8
	// If not at the end of the data, we expect a delimiter.
	if ck.pos < len(ck.data) {
		if ck.data[ck.pos] != delimiter {
			// No delimiter means we expected one since there's more data
			return 0, fmt.Errorf("expected delimiter after uint64, got none")
		}
		ck.pos++
	}
	return val, nil
}

// readStringFieldAllowNoDelimiter tries to read until the next delimiter.
// If no delimiter is found, but we still have data left, that means this is the last field
// and we return all remaining data as the field.
func (ck *CompositeKey) readStringFieldAllowNoDelimiter() (string, error) {
	if ck.pos >= len(ck.data) {
		return "", errNoDelimiter
	}
	delPos := bytes.IndexByte(ck.data[ck.pos:], delimiter)
	if delPos == -1 {
		// No delimiter found, take the rest as the final field
		result := string(ck.data[ck.pos:])
		ck.pos = len(ck.data) // move to end
		return result, nil
	}
	// Delimiter found
	result := string(ck.data[ck.pos : ck.pos+delPos])
	ck.pos += delPos + 1
	return result, nil
}
