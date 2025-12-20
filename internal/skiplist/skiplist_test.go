package skiplist

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
)

func TestInsertAndGet(t *testing.T) {
	s := NewSkipList()

	key := []byte("foo")
	value := []byte("bar")

	s.Insert(key, value)

	got, ok := s.Get(key)
	if !ok {
		t.Fatalf("expected key %s to be present", key)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("expected value %s, got %s", value, got)
	}

	updated := []byte("baz")
	s.Insert(key, updated)

	got, ok = s.Get(key)
	if !ok {
		t.Fatalf("expected key %s to be present after update", key)
	}
	if !bytes.Equal(got, updated) {
		t.Fatalf("expected updated value %s, got %s", updated, got)
	}
}

func TestGetMissing(t *testing.T) {
	s := NewSkipList()
	s.Insert([]byte("present"), []byte("value"))

	if _, ok := s.Get([]byte("missing")); ok {
		t.Fatalf("expected missing key lookup to return false")
	}
}

func TestDelete(t *testing.T) {
	s := NewSkipList()

	s.Insert([]byte("a"), []byte("1"))
	s.Insert([]byte("b"), []byte("2"))
	s.Insert([]byte("c"), []byte("3"))

	if !s.Delete([]byte("b")) {
		t.Fatalf("expected delete to succeed for existing key")
	}

	if _, ok := s.Get([]byte("b")); ok {
		t.Fatalf("expected deleted key to be absent")
	}

	if s.Delete([]byte("b")) {
		t.Fatalf("expected delete to fail for already deleted key")
	}

	if val, ok := s.Get([]byte("a")); !ok || !bytes.Equal(val, []byte("1")) {
		t.Fatalf("expected other keys to remain after delete")
	}
	if val, ok := s.Get([]byte("c")); !ok || !bytes.Equal(val, []byte("3")) {
		t.Fatalf("expected other keys to remain after delete")
	}
}

func TestConcurrentInsertAndGet(t *testing.T) {
	s := NewSkipList()

	const total = 200
	var wg sync.WaitGroup

	for i := 0; i < total; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := []byte(strconv.Itoa(i))
			value := []byte("v" + strconv.Itoa(i))
			s.Insert(key, value)
		}()
	}

	wg.Wait()

	for i := 0; i < total; i++ {
		key := []byte(strconv.Itoa(i))
		expected := []byte("v" + strconv.Itoa(i))

		got, ok := s.Get(key)
		if !ok {
			t.Fatalf("expected key %s to be present after concurrent inserts", key)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("expected value %s for key %s, got %s", expected, key, got)
		}
	}

}

func TestConcurrentOverlapReadsAndWrites(t *testing.T) {
	s := NewSkipList()

	const baseKeys = 50
	const newKeys = 100
	const readLoops = 300

	for i := 0; i < baseKeys; i++ {
		key := []byte("base" + strconv.Itoa(i))
		val := []byte("v" + strconv.Itoa(i))
		s.Insert(key, val)
	}

	start := make(chan struct{})
	var writers sync.WaitGroup
	var readers sync.WaitGroup

	writers.Add(newKeys)
	for i := 0; i < newKeys; i++ {
		i := i
		go func() {
			defer writers.Done()
			<-start
			key := []byte("new" + strconv.Itoa(i))
			val := []byte("nv" + strconv.Itoa(i))
			s.Insert(key, val)
		}()
	}

	readers.Add(baseKeys)
	for i := 0; i < baseKeys; i++ {
		i := i
		go func() {
			defer readers.Done()
			<-start
			key := []byte("base" + strconv.Itoa(i))
			expected := []byte("v" + strconv.Itoa(i))
			for j := 0; j < readLoops; j++ {
				got, ok := s.Get(key)
				if !ok || !bytes.Equal(got, expected) {
					t.Errorf("expected stable read for key %s during concurrent writes", key)
					return
				}
			}
		}()
	}

	close(start)
	writers.Wait()
	readers.Wait()

	for i := 0; i < newKeys; i++ {
		key := []byte("new" + strconv.Itoa(i))
		expected := []byte("nv" + strconv.Itoa(i))
		got, ok := s.Get(key)
		if !ok || !bytes.Equal(got, expected) {
			t.Fatalf("expected key %s to be present after concurrent writes", key)
		}
	}
}
