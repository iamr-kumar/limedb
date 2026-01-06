package sequence

import (
	"math"
	"sync"
	"testing"
)

func TestSequenceManager_Init(t *testing.T) {
	tests := []struct {
		name     string
		start    uint64
		wantNext uint64
	}{
		{
			name:     "init with zero",
			start:    0,
			wantNext: 0,
		},
		{
			name:     "init with positive value",
			start:    100,
			wantNext: 100,
		},
		{
			name:     "init with large value",
			start:    1_000_000_000,
			wantNext: 1_000_000_000,
		},
		{
			name:     "init with max uint64 minus 1",
			start:    math.MaxUint64 - 1,
			wantNext: math.MaxUint64 - 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var manager SequenceManager
			manager.Init(tt.start)
			got := manager.Next()
			if got != tt.wantNext {
				t.Errorf("Init(%d) then Next() = %d, want %d", tt.start, got, tt.wantNext)
			}
		})
	}
}

func TestSequenceManager_Next(t *testing.T) {
	t.Run("sequential calls return incrementing values", func(t *testing.T) {
		var manager SequenceManager
		manager.Init(0)

		for i := uint64(0); i < 100; i++ {
			got := manager.Next()
			if got != i {
				t.Errorf("Next() call %d = %d, want %d", i, got, i)
			}
		}
	})

	t.Run("next after init with non-zero", func(t *testing.T) {
		var manager SequenceManager
		manager.Init(50)

		for i := uint64(50); i < 60; i++ {
			got := manager.Next()
			if got != i {
				t.Errorf("Next() = %d, want %d", got, i)
			}
		}
	})

	t.Run("reinit resets sequence", func(t *testing.T) {
		var manager SequenceManager
		manager.Init(0)

		// Consume some sequence numbers
		for i := 0; i < 10; i++ {
			manager.Next()
		}

		// Reinitialize
		manager.Init(5)
		got := manager.Next()
		if got != 5 {
			t.Errorf("After reinit, Next() = %d, want 5", got)
		}
	})
}

func TestSequenceManager_NextBatch(t *testing.T) {
	tests := []struct {
		name      string
		start     uint64
		batchSize uint64
		wantStart uint64
		wantNext  uint64
	}{
		{
			name:      "batch of 1",
			start:     0,
			batchSize: 1,
			wantStart: 0,
			wantNext:  1,
		},
		{
			name:      "batch of 10",
			start:     0,
			batchSize: 10,
			wantStart: 0,
			wantNext:  10,
		},
		{
			name:      "batch from non-zero start",
			start:     100,
			batchSize: 50,
			wantStart: 100,
			wantNext:  150,
		},
		{
			name:      "large batch",
			start:     0,
			batchSize: 1_000_000,
			wantStart: 0,
			wantNext:  1_000_000,
		},
		{
			name:      "batch of zero",
			start:     10,
			batchSize: 0,
			wantStart: 10,
			wantNext:  10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var manager SequenceManager
			manager.Init(tt.start)

			gotStart := manager.NextBatch(tt.batchSize)
			if gotStart != tt.wantStart {
				t.Errorf("NextBatch(%d) = %d, want %d", tt.batchSize, gotStart, tt.wantStart)
			}

			gotNext := manager.Next()
			if gotNext != tt.wantNext {
				t.Errorf("Next() after NextBatch = %d, want %d", gotNext, tt.wantNext)
			}
		})
	}
}

func TestSequenceManager_MultipleBatches(t *testing.T) {
	var manager SequenceManager
	manager.Init(0)

	// First batch of 10
	start1 := manager.NextBatch(10)
	if start1 != 0 {
		t.Errorf("First batch start = %d, want 0", start1)
	}

	// Second batch of 5
	start2 := manager.NextBatch(5)
	if start2 != 10 {
		t.Errorf("Second batch start = %d, want 10", start2)
	}

	// Single next
	next := manager.Next()
	if next != 15 {
		t.Errorf("Next() = %d, want 15", next)
	}

	// Third batch of 3
	start3 := manager.NextBatch(3)
	if start3 != 16 {
		t.Errorf("Third batch start = %d, want 16", start3)
	}
}

func TestSequenceManager_MixedNextAndBatch(t *testing.T) {
	var manager SequenceManager
	manager.Init(0)

	// Interleave Next and NextBatch calls
	seq := make([]uint64, 0)

	seq = append(seq, manager.Next())       // 0
	seq = append(seq, manager.Next())       // 1
	seq = append(seq, manager.NextBatch(3)) // 2 (reserves 2,3,4)
	seq = append(seq, manager.Next())       // 5
	seq = append(seq, manager.NextBatch(2)) // 6 (reserves 6,7)
	seq = append(seq, manager.Next())       // 8

	expected := []uint64{0, 1, 2, 5, 6, 8}
	for i, want := range expected {
		if seq[i] != want {
			t.Errorf("seq[%d] = %d, want %d", i, seq[i], want)
		}
	}
}

// Race condition tests - use -race flag to detect data races
func TestSequenceManager_ConcurrentNext(t *testing.T) {
	var manager SequenceManager
	manager.Init(0)

	const goroutines = 100
	const iterationsPerGoroutine = 1000
	const totalIterations = goroutines * iterationsPerGoroutine

	var wg sync.WaitGroup
	results := make(chan uint64, totalIterations)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterationsPerGoroutine; j++ {
				results <- manager.Next()
			}
		}()
	}

	wg.Wait()
	close(results)

	// Collect all sequence numbers
	seen := make(map[uint64]bool)
	for seq := range results {
		if seen[seq] {
			t.Errorf("Duplicate sequence number: %d", seq)
		}
		seen[seq] = true
	}

	// Verify we got exactly the expected count of unique sequences
	if len(seen) != totalIterations {
		t.Errorf("Got %d unique sequences, want %d", len(seen), totalIterations)
	}

	// Verify all sequences are in valid range [0, totalIterations)
	for seq := range seen {
		if seq >= totalIterations {
			t.Errorf("Sequence %d out of expected range [0, %d)", seq, totalIterations)
		}
	}
}

func TestSequenceManager_ConcurrentNextBatch(t *testing.T) {
	var manager SequenceManager
	manager.Init(0)

	const goroutines = 50
	const batchSize = 100

	var wg sync.WaitGroup
	results := make(chan uint64, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			results <- manager.NextBatch(batchSize)
		}()
	}

	wg.Wait()
	close(results)

	// Collect all batch starts
	starts := make([]uint64, 0, goroutines)
	for start := range results {
		starts = append(starts, start)
	}

	// Verify no overlapping batches
	seen := make(map[uint64]bool)
	for _, start := range starts {
		for seq := start; seq < start+batchSize; seq++ {
			if seen[seq] {
				t.Errorf("Overlapping batch detected at sequence %d", seq)
			}
			seen[seq] = true
		}
	}

	// Verify total sequences allocated
	expectedTotal := goroutines * batchSize
	if len(seen) != expectedTotal {
		t.Errorf("Got %d unique sequences, want %d", len(seen), expectedTotal)
	}
}

func TestSequenceManager_ConcurrentMixed(t *testing.T) {
	var manager SequenceManager
	manager.Init(0)

	const goroutines = 50
	const iterationsPerGoroutine = 100

	var wg sync.WaitGroup
	type result struct {
		start uint64
		count uint64
	}
	results := make(chan result, goroutines*iterationsPerGoroutine*2)

	// Half the goroutines call Next(), half call NextBatch()
	wg.Add(goroutines * 2)

	// Next() callers
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterationsPerGoroutine; j++ {
				seq := manager.Next()
				results <- result{start: seq, count: 1}
			}
		}()
	}

	// NextBatch() callers with varying batch sizes
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			batchSize := uint64((id % 5) + 1) // Batch sizes 1-5
			for j := 0; j < iterationsPerGoroutine; j++ {
				start := manager.NextBatch(batchSize)
				results <- result{start: start, count: batchSize}
			}
		}(i)
	}

	wg.Wait()
	close(results)

	// Verify no overlapping sequences
	seen := make(map[uint64]bool)
	for res := range results {
		for seq := res.start; seq < res.start+res.count; seq++ {
			if seen[seq] {
				t.Errorf("Duplicate/overlapping sequence at %d", seq)
			}
			seen[seq] = true
		}
	}
}

func TestSequenceManager_ConcurrentInitAndNext(t *testing.T) {
	// This test verifies behavior when Init is called concurrently with Next
	// While this is an unusual use case, the implementation should not crash
	var manager SequenceManager
	manager.Init(0)

	var wg sync.WaitGroup
	const iterations = 1000

	wg.Add(2)

	// Goroutine calling Next
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			manager.Next()
		}
	}()

	// Goroutine calling Init (simulating WAL replay during recovery)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			manager.Init(uint64(i * 100))
		}
	}()

	wg.Wait()
	// If we get here without a panic/race detector complaint, the test passes
}

// Edge case tests
func TestSequenceManager_ZeroValue(t *testing.T) {
	// Test that a zero-value SequenceManager works correctly
	var manager SequenceManager
	// No Init call

	got := manager.Next()
	if got != 0 {
		t.Errorf("Zero-value manager Next() = %d, want 0", got)
	}

	got = manager.Next()
	if got != 1 {
		t.Errorf("Second Next() = %d, want 1", got)
	}
}

func TestSequenceManager_LargeSequenceNumbers(t *testing.T) {
	var manager SequenceManager
	manager.Init(math.MaxUint64 - 10)

	// Should be able to generate sequences near max uint64
	for i := uint64(0); i < 5; i++ {
		got := manager.Next()
		want := math.MaxUint64 - 10 + i
		if got != want {
			t.Errorf("Next() = %d, want %d", got, want)
		}
	}
}

func TestSequenceManager_Overflow(t *testing.T) {
	var manager SequenceManager
	manager.Init(math.MaxUint64)

	// First Next() returns MaxUint64
	got := manager.Next()
	want := uint64(math.MaxUint64)
	if got != want {
		t.Errorf("Next() = %d, want %d", got, want)
	}

	// Second Next() wraps around to 0 (uint64 overflow behavior)
	got = manager.Next()
	if got != 0 {
		t.Errorf("Next() after overflow = %d, want 0", got)
	}
}

func TestSequenceManager_BatchOverflow(t *testing.T) {
	var manager SequenceManager
	start := uint64(math.MaxUint64 - 5)
	manager.Init(start)

	// Request a batch larger than remaining space
	got := manager.NextBatch(10)
	if got != start {
		t.Errorf("NextBatch(10) = %d, want %d", got, start)
	}

	// Next call will have wrapped around
	got = manager.Next()
	// After adding 10 to MaxUint64-5, we get MaxUint64+5 which wraps to 4
	if got != 4 {
		t.Errorf("Next() after batch overflow = %d, want 4", got)
	}
}

// Benchmark tests
func BenchmarkSequenceManager_Next(b *testing.B) {
	var manager SequenceManager
	manager.Init(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.Next()
	}
}

func BenchmarkSequenceManager_NextBatch(b *testing.B) {
	var manager SequenceManager
	manager.Init(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.NextBatch(100)
	}
}

func BenchmarkSequenceManager_Next_Parallel(b *testing.B) {
	var manager SequenceManager
	manager.Init(0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			manager.Next()
		}
	})
}

func BenchmarkSequenceManager_NextBatch_Parallel(b *testing.B) {
	var manager SequenceManager
	manager.Init(0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			manager.NextBatch(10)
		}
	})
}
