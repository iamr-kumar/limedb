package skiplist

import (
	"bytes"
	"reflect"
	"testing"
)

func TestIteratorEmpty(t *testing.T) {
	s := NewSkipList()

	it := s.NewIterator()

	if it.Valid() {
		t.Fatalf("expected iterator to be invalid on empty list")
	}

	if it.Key() != nil {
		t.Fatalf("expected nil key on empty iterator")
	}

	if it.Value() != nil {
		t.Fatalf("expected nil value on empty iterator")
	}
}

func TestIteratorTraversalOrder(t *testing.T) {
	s := NewSkipList()

	entries := []struct {
		key   []byte
		value []byte
	}{
		{key: []byte("c"), value: []byte("3")},
		{key: []byte("a"), value: []byte("1")},
		{key: []byte("b"), value: []byte("2")},
	}

	for _, e := range entries {
		s.Insert(e.key, e.value)
	}

	it := s.NewIterator()

	var keys [][]byte
	var values [][]byte

	for it.Valid() {
		k := it.Key()
		v := it.Value()

		keys = append(keys, append([]byte{}, k...))
		values = append(values, append([]byte{}, v...))

		it.Next()
	}

	expectedKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	expectedValues := [][]byte{[]byte("1"), []byte("2"), []byte("3")}

	if !reflect.DeepEqual(keys, expectedKeys) {
		t.Fatalf("expected keys %v, got %v", expectedKeys, keys)
	}

	if !reflect.DeepEqual(values, expectedValues) {
		t.Fatalf("expected values %v, got %v", expectedValues, values)
	}
}

func TestIteratorSeekToFirstResets(t *testing.T) {
	s := NewSkipList()
	s.Insert([]byte("a"), []byte("1"))
	s.Insert([]byte("b"), []byte("2"))

	it := s.NewIterator()

	if !it.Valid() {
		t.Fatalf("expected iterator to be valid after inserts")
	}

	// Advance to the end
	for it.Valid() {
		it.Next()
	}

	if it.Valid() {
		t.Fatalf("expected iterator to be invalid after exhausting elements")
	}

	it.SeekToFirst()

	if !it.Valid() {
		t.Fatalf("expected iterator to be valid after SeekToFirst")
	}

	if !bytes.Equal(it.Key(), []byte("a")) || !bytes.Equal(it.Value(), []byte("1")) {
		t.Fatalf("expected iterator to reset to first element")
	}
}
