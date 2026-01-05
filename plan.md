# **LimeDB Implementation Roadmap (Revised)**

## **Phase 1: Foundation - Core Data Structures**

### **Task 1.1: Basic Skip List (Non-Concurrent)**

**Objective**: Implement a simple, single-threaded skip list data structure.

**Key Concepts to Learn**:

- **Go**: Pointers, struct embedding, `math/rand` for level generation
- **Data Structures**: Skip list theory, probabilistic balancing, O(log n) operations
- **Algorithms**: Binary search concept, linked list traversal

**Implementation Details**:

- Implement `Insert(key, value)`, `Get(key)`, `Delete(key)`
- Tower height: max 12 levels, probability p=0.25 for promotion
- Keys are `[]byte`, values are `[]byte`
- Implement forward-only `Iterator` with `SeekToFirst()`, `Next()`, `Valid()`, `Key()`, `Value()`

**Best Practices**:

- Use sentinel head node to simplify edge cases
- Implement `randomLevel()` function for height selection
- Write comprehensive unit tests with various key patterns

**File Layout After Task**:

```
internal/
	skiplist/
		skiplist.go       # NEW: Basic skip list implementation
		skiplist_test.go  # NEW: Unit tests
		iterator.go       # NEW: Forward iterator

```

**Suggested Improvements**:

- Consider adding `Size()` method
- Think about memory layout for cache efficiency

---

### **Task 1.2: Concurrent Skip List**

**Objective**: Add thread-safety to your skip list for concurrent reads and writes.

**Key Concepts to Learn**:

- **Go**: `sync.RWMutex`, `sync.Mutex`, when to use which
- **Concurrency**: Reader-writer locks, critical sections, lock granularity
- **Testing**: Race detector (`go test -race`)

**Implementation Details**:

- Start with coarse-grained locking (single RWMutex for entire structure)
- `RLock` for reads (`Get`, iteration), `Lock` for writes (`Insert`, `Delete`)
- Ensure iterator holds read lock during iteration OR captures snapshot

**Best Practices**:

- Always use `defer` for unlock to handle panics
- Test with race detector: `go test -race Projects.`
- Write concurrent test: multiple goroutines inserting/reading simultaneously

**File Layout After Task**:

```
internal/
	skiplist/
		skiplist.go       # MODIFY: Add mutex, locking
		skiplist_test.go  # MODIFY: Add concurrent tests
		iterator.go

```

**Suggested Improvements**:

- For now, coarse-grained locking is fine; fine-grained locking is a future optimization
- Document lock ordering to prevent deadlocks

---

### **Task 1.3: Bloom Filter**

**Objective**: Implement a space-efficient probabilistic set membership filter.

**Key Concepts to Learn**:

- **Go**: Bit manipulation (`&`, `|`, `<<`), `[]byte` as bit array
- **Math**: Hash functions, false positive probability: (1−e−kn/m)k
      (1−e−kn/m)k
- **Data Structures**: Probabilistic data structures trade-offs

**Implementation Details**:

- Use double hashing: `hash_i(key) = hash1(key) + i * hash2(key)`
- Use `hash/fnv` for FNV-1a hash (fast, good distribution)
- Implement `Add(key []byte)` and `MayContain(key []byte) bool`
- Constructor: `NewBloomFilter(expectedItems int, falsePositiveRate float64)`

**Best Practices**:

- Calculate optimal bits and hash count from parameters
- Bits per key ≈ −(ln2)2ln(p) where p = false positive rate
      −ln(p)(ln2)2
- Hash count ≈ nm⋅ln2
      mn⋅ln2

**File Layout After Task**:

```
internal/
	filter/
		bloom.go          # NEW: Bloom filter implementation
		bloom_test.go     # NEW: Tests including false positive rate verification

```

**Suggested Improvements**:

- Add `Serialize()` and `Deserialize()` methods (will need for SSTable later)
- Test actual false positive rate matches expected rate

---

### **Task 1.4: Binary Encoding Utilities**

**Objective**: Build utilities for encoding/decoding binary data consistently.

**Key Concepts to Learn**:

- **Go**: `encoding/binary`, big-endian vs little-endian, varint encoding
- **System/OS**: Byte ordering, portability across architectures
- **Database**: Compact encoding for storage efficiency

**Implementation Details**:

- Fixed-width encoding: `PutUint32`, `PutUint64`, `GetUint32`, `GetUint64`
- Variable-width encoding: `PutVarint`, `GetVarint` (smaller numbers = fewer bytes)
- Length-prefixed byte slices: `PutBytes`, `GetBytes`
- Use big-endian for consistent byte ordering

**Best Practices**:

- Varint is great for sequence numbers (often small), fixed for offsets
- Always validate buffer bounds before reading
- Write round-trip tests: encode → decode → compare

**File Layout After Task**:

```
internal/
	encoding/
		binary.go         # NEW: Encoding utilities
		binary_test.go    # NEW: Round-trip tests

```

**Suggested Improvements**:

- Consider adding checksum utilities (CRC32) here
- Document when to use varint vs fixed-width

---

## **Phase 2: Write-Ahead Log (Standalone)**

### **Task 2.1: WAL Entry Format and Serialization**

**Objective**: Refactor and improve your existing WAL entry serialization.

**Key Concepts to Learn**:

- **Go**: Interface design, error wrapping with `%w`
- **Database**: Log entry formats, checksums for integrity
- **System/OS**: Data corruption detection

**Implementation Details**:

- Extract entry types into separate file
- Entry format: `[Length][Checksum][Type][Key][Value]`
- Type: `Put = 1`, `Delete = 2` (remove `Update` - it's semantically same as `Put`)
- Use CRC32 checksum over type + key + value

**What to Change in Your Code**:

- Your current `Entry` has `TTL` and `Timestamp` - keep them but make `TTL` optional
- Remove `OperationUpdate` - a Put operation handles both insert and update
- Simplify: remove `SequenceID` from entry format for now (will add in Phase 4)

**File Layout After Task**:

```
internal/
	wal/
		entry.go          # NEW: Extract from wal.go - Entry struct and serialization
		entry_test.go     # NEW: Serialization round-trip tests
		wal.go            # MODIFY: Remove Entry definition, import from entry.go

```

**Suggested Improvements**:

- Add entry type constants as `iota`
- Consider batch entry format for future (but don't implement yet)

---

**Task 2.3: WAL Truncation and Segments**
_Objective:_ Manage WAL file growth by splitting logs into segments and removing old ones.
_Key Concepts to Learn:_

- **Go:** `os.Rename`, directory reading (`os.ReadDir`).
- **Database:** Log segmentation, safe deletion policies.
- **System/OS:** Atomic file operations.
  _Implementation Details:_
- Instead of one `wal.log`, use `000001.wal`, `000002.wal`.
- **Rotate:** When active segment > 64MB, close it and create new segment.
- **Delete:** Implement `DeleteOlderThan(index int)` to remove segments that have been flushed to SSTables.
- **Map:** Maintain a map of `SegmentID -> MaxSequenceNumber`.
  _File Layout After Task:_

```
internal/
	wal/
		entry.go
		wal.go            # MODIFY: Add segment rotation logic
		segment.go        # NEW: Segment file handling
		wal_test.go       # MODIFY: Test rotation and deletion
		options.go
```

_Suggested Improvements:_

- Add logic to prevent deleting the _active_ segment.

---

## Phase 3: MemTable (In-Memory Storage) — introduces **global per-DB SequenceID**

> New design requirement: a single monotonically increasing SequenceID per DB, shared by WAL and MemTable. Add a lightweight sequence manager that initializes from the highest SequenceID recovered from WAL segments and hands out IDs for every write. Both WAL entries and MemTable entries must use the same SequenceID for ordering, conflict resolution, and recovery.

### **Task 3.1: Basic MemTable Structure + SequenceID plumbing**

_Objective:_ Create MemTable wrapper around the Concurrent Skip List, wired to the global SequenceManager.

_Key Concepts to Learn:_

- **Go:** Struct composition, atomic counters.
- **Database:** InternalKey ordering (UserKey + SequenceID + Type) for last-write-wins.

_Implementation Details:_

- Add `internal/sequence/manager.go` with a per-DB `SequenceManager` that initializes from WAL replay (max seen seq) and hands out new SequenceIDs.
- MemTable entry: `{Key, Value, Type, SequenceID, ExpiresAt}` (keep SequenceID required).
- Write path: obtain `seq := seqManager.Next()`, set it on both `WalEntry.SequenceID` and `MemtableEntry.SequenceID`.
- Define an `InternalKey` comparator: order by `UserKey ASC`, then `SequenceID DESC`, then `Type` to pick newest value first.

_Code Snippet:_

```go
// internal/sequence/manager.go
type SequenceManager struct { next uint64 }

func (m *SequenceManager) Init(start uint64) { atomic.StoreUint64(&m.next, start) }
func (m *SequenceManager) Next() uint64      { return atomic.AddUint64(&m.next, 1) }

// write path (pseudo)
seq := seqMgr.Next()
walEntry.SequenceID = seq
memEntry := memtable.NewMemtableEntry(key, val, entryType, seq, expiresAt)
```

_File Layout After Task:_

```
internal/
	sequence/
		manager.go       # NEW: Per-DB global SequenceID allocator
	memtable/
		memtable.go      # NEW: MemTable implementation
		memtable_test.go # NEW: Tests
		entry.go         # NEW: MemTable entry definition
```

### **Task 3.2: MemTable Size Limits and Immutability (Sequence-aware)**

_Objective:_ Handle the transition from Active to Immutable without losing Sequence ordering.

_Key Concepts to Learn:_

- **Concurrency:** State transitions, read-only guarantees.

_Implementation Details:_

- Track `maxSize` (e.g., 64MB) and current `approxSize`.
- When full, call `MarkImmutable()`; future `Put` should return `ErrMemTableFull`.
- Keep `maxSeqID` on each MemTable to expose the highest SequenceID stored (used to decide WAL segment deletion and flush ordering).

_Code Snippet:_

```go
// memtable.go (excerpt)
type MemTable struct {
		immut     atomic.Bool
		maxSeqID  atomic.Uint64
		approxSz  atomic.Int64
}

func (m *MemTable) Put(e *MemtableEntry) error {
		if m.immut.Load() { return ErrMemTableFull }
		m.skiplist.Insert(e.InternalKey(), e)
		m.maxSeqID.Store(max(m.maxSeqID.Load(), e.SequenceID))
		// update approxSz ...
		return nil
}
```

_File Layout After Task:_

```
internal/
	memtable/
		memtable.go       # MODIFY: Add size tracking, immutable flag, maxSeqID
		memtable_test.go
		entry.go
```

### **Task 3.3: MemTable Iterator (Sequence-aware ordering)**

_Objective:_ Implement sorted iteration for flushing, respecting InternalKey order (newest first per key).

_Key Concepts to Learn:_

- **Go:** Iterator pattern.
- **Ordering:** Sort by `UserKey ASC` then `SequenceID DESC`.

_Implementation Details:_

- Use the skip list comparator with the InternalKey ordering.
- Iterator should surface `UserKey`, `Value`, `Type`, `SequenceID`, `ExpiresAt`.
- Ensure iterator can be used safely while MemTable is immutable.

_Code Snippet:_

```go
// memtable/iterator.go (compare function)
func compareIK(a, b InternalKey) int {
		if cmp := bytes.Compare(a.UserKey, b.UserKey); cmp != 0 { return cmp }
		if a.Seq > b.Seq { return -1 }
		if a.Seq < b.Seq { return 1 }
		return int(a.Type) - int(b.Type)
}
```

_File Layout After Task:_

```
internal/
	memtable/
		memtable.go
		iterator.go       # NEW: MemTable iterator using InternalKey comparator
```

---

## Phase 4: SSTable (Block-Based Format) — carry SequenceID through blocks

### **Task 4.1: Block Builder and Encoding (with InternalKey)**

_Objective:_ Bundle entries into 4KB data blocks, encoding InternalKeys (UserKey + Seq + Type).

_Key Concepts to Learn:_

- **Database:** Block storage, restart points (prefix compression - optional but recommended).

_Implementation Details:_

- Define an `InternalKey` encoder: `[UserKeyLen][UserKey][Seq uint64][Type uint8]`.
- `BlockBuilder` buffers InternalKeys + values until ~4KB.
- Newest versions naturally surface first within a UserKey because of SequenceID ordering.

_Code Snippet:_

```go
// sstable/internal_key.go
type InternalKey struct {
		UserKey []byte
		Seq     uint64
		Type    uint8
}

func (k InternalKey) Encode(dst []byte) []byte {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(k.UserKey)))
		dst = append(dst, k.UserKey...)
		dst = binary.BigEndian.AppendUint64(dst, k.Seq)
		dst = append(dst, k.Type)
		return dst
}
```

_File Layout After Task:_

```
internal/
	sstable/
		block_builder.go  # NEW: Packs InternalKeys + values
		block_test.go     # NEW: Test packing/unpacking preserves Seq order
		internal_key.go   # NEW: Encode/Decode helpers
```

### **Task 4.2: SSTable Writer (Block-Based)**

_Objective:_ Write blocks to disk and track their offsets, preserving SequenceID for conflict resolution on reads.

_Implementation Details:_

- `SSTableWriter` consumes MemTable iterator (already sorted by InternalKey).
- Index entries store the **last InternalKey** of each block (UserKey + highest Seq in that block) for searching.
- Footer and meta remain as planned.

_Code Snippet:_

```go
// writer.go (pseudo)
for it.Valid() {
		ik := it.InternalKey()
		block.Add(ik, it.Value())
		if block.Full() { flushBlock(ik) }
		it.Next()
}
```

_File Layout After Task:_

```
internal/
	sstable/
		writer.go         # NEW: Writes blocks with InternalKeys
		writer_test.go
		format.go         # NEW: Magic numbers, footer layout
```

### **Task 4.3: Partitioned Index (InternalKey aware)**

_Objective:_ Implement the Index to find which Block contains a key, using InternalKey ordering.

_Implementation Details:_

- Index stores `(LastInternalKey, BlockOffset, BlockSize)`.
- Search uses `UserKey` then `SequenceID` to pick the newest block that could contain the desired version.

_Code Snippet:_

```go
// index_block.go (pseudo)
type IndexEntry struct {
		LastKey   InternalKey
		Offset    uint64
		Size      uint32
}
```

_File Layout After Task:_

```
internal/
	sstable/
		writer.go         # MODIFY: Write index block at EOF
		index_block.go    # NEW: Build/search index entries with InternalKey
```

### **Task 4.4: Blocked Bloom Filter**

**Objective:** Implement Bloom filters optimized for CPU cache.

**Key Concepts to Learn:**

- **Performance:** Cache locality.

**Implementation Details:**

- Instead of one giant bitset, use "Blocked Bloom": tiny bloom filters that fit in a CPU cache line.
- Write the Bloom Filter block to the SSTable.
- Update Footer to point to Bloom Filter offset.

**File Layout After Task:**

```
internal/
	filter/
		bloom.go          # MODIFY: Update to support block generation (or keep simple for now)
	sstable/
		writer.go         # MODIFY: Write bloom block
```

---

## Phase 5: Caching Layer (New for v2.0)

**Objective:** Implement the read-latency optimizations defined in the design.

### **Task 5.1: LRU Block Cache**

**Objective:** In-memory cache for uncompressed SSTable blocks.

**Key Concepts to Learn:**

- **Algorithms:** Least Recently Used (LRU) eviction.
- **Concurrency:** Sharded locking to reduce contention.

**Implementation Details:**

- Key: `{FileID, BlockOffset}`.
- Value: `[]byte` (The data block).
- Structure: Hash Map + Doubly Linked List.
- Sharding: Use 16 shards based on hash of the key to avoid a global mutex.

**File Layout After Task:**

```
internal/
	cache/
		lru.go            # NEW: Generic LRU implementation
		shard.go          # NEW: Sharded cache wrapper
		cache_test.go     # NEW: Hit/Miss/Eviction tests
```

### **Task 5.2: SSTable Reader with Cache Integration**

**Objective:** Read blocks from disk, cache them, and serve reads.

**Key Concepts to Learn:**

- **Architecture:** Read-through caching.

**Implementation Details:**

- `SSTableReader`: Open file, read Footer.
- `ReadBlock(offset)`: check cache first, then disk.
- Iterator iterates over block data, using InternalKey order to pick newest version.

**File Layout After Task:**

```
internal/
	sstable/
		reader.go         # NEW: Reader using cache
		iterator.go       # NEW: Iterator logic over blocks
```

### **Task 5.3: Pinned Level 0 Metadata**

**Objective:** Implement the "Pinning" optimization for L0.

**Key Concepts to Learn:**

- **Optimization:** Trading RAM for Disk Seeks.

**Implementation Details:**

- In `SSTableReader`, add a flag `IsLevel0 bool`.
- If `IsLevel0 == true`: load Index Block and Bloom Filter into memory immediately and never evict.
- If `IsLevel0 == false`: load Index/Bloom blocks via the `BlockCache`.

**File Layout After Task:**

```
internal/
	sstable/
		reader.go         # MODIFY: Add pre-loading logic based on level
```

---

## Phase 6: Flush Pipeline — preserve SequenceID metadata

### **Task 6.1: Manifest & File Management**

_Objective:_ Track file levels and IDs, plus highest SequenceID per file to support recovery and WAL truncation decisions.

_Implementation Details:_

- `Manifest`: JSON file tracking `[{FileID, Level, MinKey, MaxKey, MaxSeq}]`.
- Atomic file creation/naming (`000001.sst`).

_Code Snippet:_

```go
type FileMeta struct {
		FileID uint64
		Level  int
		MinKey []byte
		MaxKey []byte
		MaxSeq uint64
}
```

_File Layout After Task:_

```
internal/
	manifest/
		manifest.go
		manifest_test.go
```

### **Task 6.2: Flush Logic (MemTable -> Block SSTable) with Seq tracking**

_Objective:_ Connect MemTable iterator to SSTable Block Writer and surface max SequenceID written.

_Implementation Details:_

- Iterate MemTable (InternalKey order) → `SSTableWriter`.
- Capture `maxSeq := memtable.MaxSeqID()`; store into `FileMeta.MaxSeq` for manifest.
- After successful flush, signal WAL to delete segments with `maxSeqId <= flushedMaxSeq`.

_Code Snippet:_

```go
maxSeq := mem.MaxSeqID()
meta := writer.Finish()
meta.MaxSeq = maxSeq
manifest.Add(meta)
wal.DeleteSegmentsBefore(maxSeq)
```

_File Layout After Task:_

```
internal/
	flush/
		flush.go
```

---

## Phase 7: Unified Storage Engine — batch SequenceID allocation

### **Task 7.1: Engine Write Path (Group Commit)**

_Objective:_ Implement high-throughput writes that batch WAL fsync and share SequenceIDs across WAL and MemTable.

_Implementation Details:_

- `SequenceManager` gains `NextBatch(n int) (start uint64)` helper to allocate contiguous seq range for a batch.
- Batch loop: allocate seqs, set on each `WalEntry`, write all to WAL once, then apply to MemTable.

_Code Snippet:_

```go
start := seqMgr.NextBatch(len(batch))
for i, req := range batch {
		seq := start + uint64(i)
		walEntries[i].SequenceID = seq
		memEntries[i].SequenceID = seq
}
wal.WriteBatch(walEntries)
mem.ApplyBatch(memEntries)
```

_File Layout After Task:_

```
internal/
	engine/
		engine.go
		write_path.go     # NEW: Group commit logic using SequenceManager
```

### **Task 7.2: Engine Read Path (Pinned L0)**

_Objective:_ Implement the optimized read hierarchy.

_Implementation Details:_

- `Get(key)` checks Active and Immutable MemTables (InternalKey ordering picks newest), then L0 pinned SSTables, then leveled SSTables.
- Reads rely on InternalKey ordering to choose the freshest version.

_File Layout After Task:_

```
internal/
	engine/
		read_path.go      # NEW: Leveled read logic
```

### **Task 7.3: Recovery**

_Objective:_ Rebuild state on restart with SequenceID continuity.

_Implementation Details:_

- Load Manifest to know MaxSeq per file.
- Scan WAL segments to find highest SequenceID; initialize `SequenceManager` with `Init(maxSeq)`.
- Replay WAL entries (in order) into a fresh MemTable until all entries applied.

---

## Phase 8: Compaction

### **Task 8.1: Leveled Compaction Strategy**

_Objective:_ Merge L0 -> L1, L1 -> L2.

_Implementation Details:_

- `CompactionPicker`: decides which files to merge (L0 count, level size ratio).
- `Compactor`: merges using InternalKey order; drop overwritten/expired entries by SequenceID.
- When writing new files, compute `MaxSeq` for manifest updates.

_File Layout After Task:_

```
internal/
	compaction/
		picker.go
		worker.go
```

### **Task 8.2: Background Workers**

_Objective:_ Manage Flush and Compaction threads.

_Implementation Details:_

- Start `FlushLoop` and `CompactionLoop` in `Engine.Open`.
- Handle graceful shutdown.

---

## Phase 9: Public API & Polish

### **Task 9.1: Public Interface**

- `DB` Interface: `Get`, `Put`, `Delete`, `Close`.
- `New(options)` constructor.

### **Task 9.2: Metrics & TTL**

- Implement `PutWithTTL`.
- Add metrics for `BlockCacheHit`, `BlockCacheMiss`, `L0MemCheck`.
