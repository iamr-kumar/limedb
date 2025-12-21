package filter

import (
	"fmt"
	"math"
	"testing"
)

func TestAddAndMayContain(t *testing.T) {
	bf := NewBloomFilter(100, 1e-9)

	keys := [][]byte{[]byte("apple"), []byte("banana"), []byte("cherry")}
	for _, k := range keys {
		bf.Add(k)
	}

	for _, k := range keys {
		if !bf.MayContain(k) {
			t.Fatalf("expected key %s to be present", k)
		}
	}

	// Extremely low FP rate to make false positive practically impossible for this test
	for _, k := range [][]byte{[]byte("durian"), []byte("elderberry")} {
		if bf.MayContain(k) {
			t.Fatalf("expected key %s to be absent", k)
		}
	}
}

func TestClearResetsFilter(t *testing.T) {
	bf := NewBloomFilter(50, 1e-6)
	key := []byte("reset-me")

	bf.Add(key)
	if !bf.MayContain(key) {
		t.Fatalf("expected key to be present before clear")
	}

	bf.Clear()
	if bf.MayContain(key) {
		t.Fatalf("expected key to be absent after clear")
	}
}

func TestSerializeDeserialize(t *testing.T) {
	bf := NewBloomFilter(200, 1e-6)
	keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	for _, k := range keys {
		bf.Add(k)
	}

	data, err := bf.Serialize()
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}

	var restored BloomFilter
	if err := restored.Deserialize(data); err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	if restored.BitSize() != bf.BitSize() {
		t.Fatalf("bit size mismatch: got %d want %d", restored.BitSize(), bf.BitSize())
	}
	if restored.HashCount() != bf.HashCount() {
		t.Fatalf("hash count mismatch: got %d want %d", restored.HashCount(), bf.HashCount())
	}
	if restored.ByteSize() != bf.ByteSize() {
		t.Fatalf("byte size mismatch: got %d want %d", restored.ByteSize(), bf.ByteSize())
	}

	for _, k := range keys {
		if !restored.MayContain(k) {
			t.Fatalf("expected key %s to be present after restore", k)
		}
	}

	if restored.MayContain([]byte("delta")) {
		t.Fatalf("expected delta to be absent after restore")
	}
}

func TestOptimalParameters(t *testing.T) {
	n := uint64(1000)
	fp := 0.01

	bits := calculateOptimalBits(n, fp)
	expectedBits := uint64(math.Ceil(float64(n) * (-math.Log(fp) / (math.Ln2 * math.Ln2))))
	if bits != expectedBits {
		t.Fatalf("unexpected bit size: got %d want %d", bits, expectedBits)
	}

	hashes := calculateOptimalHashes(bits, n)
	if hashes != 7 {
		t.Fatalf("unexpected hash count: got %d want %d", hashes, 7)
	}
}

func TestEstimatedFillRatio(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	initial := bf.EstimatedFillRatio()
	if initial != 0 {
		t.Fatalf("expected empty filter fill ratio 0, got %f", initial)
	}

	for i := 0; i < 10; i++ {
		bf.Add([]byte(fmt.Sprintf("k-%d", i)))
	}

	filled := bf.EstimatedFillRatio()
	if filled <= initial {
		t.Fatalf("expected fill ratio to increase, initial %f filled %f", initial, filled)
	}
	if filled > 1 {
		t.Fatalf("fill ratio should not exceed 1, got %f", filled)
	}
}
