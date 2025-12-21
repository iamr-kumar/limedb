package filter

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/twmb/murmur3"
)

// BloomFilter represents a Bloom filter data structure
// bitset - is the underlying bit array (byte slice in Go)
// size - is the number of bits in the bitset
// hashes - is the number of hash functions used
// and the number of bits to set/check for each key
type BloomFilter struct {
	bitset []byte
	size   uint64
	hashes uint64
}

// Creates and returns a new BloomFilter instance
// expectedKeys - estimated number of keys to be added
// falsePositiveRate - desired false positive rate (e.g., 0.01 for 1%)
func NewBloomFilter(expectedKeys uint64, falsePositiveRate float64) *BloomFilter {
	if expectedKeys == 0 {
		expectedKeys = 1
	}

	// Prevent extreme false positive rates
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01 // Default to 1%
	}

	// Calculate size of bitset (m) and number of hash functions (k)
	m := calculateOptimalBits(expectedKeys, falsePositiveRate)

	k := calculateOptimalHashes(m, expectedKeys)

	return &BloomFilter{
		// we need bit array of size m bits
		// but Go byte slices are in bytes
		// so we allocate (m+7)/8 bytes
		bitset: make([]byte, (m+7)/8), // +7 to round up to the nearest byte
		size:   m,
		hashes: k,
	}
}

// Adds a key to the Bloom filter
func (filter *BloomFilter) Add(key []byte) {
	// Sum128 returns two uint64 hashes for the given key
	// We use double hashing to generate multiple hash values
	// h_i = h1 + i * h2
	// This reduces the number of hash computations needed
	// while still providing good distribution
	h1, h2 := murmur3.Sum128(key)

	for i := uint64(0); i < filter.hashes; i++ {
		pos := (h1 + i*h2) % filter.size

		// get the byte index in the byte slice
		byteIndex := pos / 8
		// get the bit index within that byte
		bitIndex := pos % 8
		// set the bit at the calculated position
		filter.bitset[byteIndex] |= 1 << bitIndex
	}
}

// Checks if a key may be present in the Bloom filter
// Returns true if the key may be present (with some false positive probability)
// Returns false if the key is definitely not present
func (filter *BloomFilter) MayContain(key []byte) bool {
	// empty filter, no bits set
	if len(filter.bitset) == 0 {
		return false
	}

	h1, h2 := murmur3.Sum128(key)

	for i := uint64(0); i < filter.hashes; i++ {
		pos := (h1 + i*h2) % filter.size
		byteIndex := pos / 8
		bitIndex := pos % 8

		if filter.bitset[byteIndex]&(1<<bitIndex) == 0 {
			return false
		}
	}
	return true
}

// Serializes the Bloom filter to a byte slice
func (filter *BloomFilter) Serialize() ([]byte, error) {
	// Format: [size: 8 bytes][hashes: 8 bytes][bitset:...]
	buffer := make([]byte, 16+len(filter.bitset))

	// Write size and hashes in little-endian format
	binary.BigEndian.PutUint64(buffer[0:8], filter.size)
	binary.BigEndian.PutUint64(buffer[8:16], filter.hashes)
	// Write bitset
	copy(buffer[16:], filter.bitset)

	return buffer, nil
}

// Deserializes a byte slice into the Bloom filter from disk
func (filter *BloomFilter) Deserialize(data []byte) error {
	if len(data) < 16 {
		return errors.New("bloom filter data too short")
	}

	// Read size and hashes
	filter.size = binary.BigEndian.Uint64(data[0:8])
	filter.hashes = binary.BigEndian.Uint64(data[8:16])

	// get the expected size of the bitset in bytes
	expectedBytes := (filter.size + 7) / 8
	if uint64(len(data)-16) < expectedBytes {
		return errors.New("bloom filter bitset data too short")
	}

	// Read bitset
	filter.bitset = make([]byte, expectedBytes)
	copy(filter.bitset, data[16:16+expectedBytes])
	return nil
}

// Clears the Bloom filter by resetting all bits to 0
func (filter *BloomFilter) Clear() {
	for i := range filter.bitset {
		filter.bitset[i] = 0
	}
}

// Returns the size of the Bloom filter in bits
func (filter *BloomFilter) BitSize() uint64 {
	return filter.size
}

// ByteSize returns the size of the filter in bytes
func (filter *BloomFilter) ByteSize() int {
	return len(filter.bitset)
}

// HashCount returns the number of hash functions
func (filter *BloomFilter) HashCount() uint64 {
	return filter.hashes
}

// EstimatedFillRatio returns the estimated ratio of bits set in the filter
func (filter *BloomFilter) EstimatedFillRatio() float64 {
	setBits := uint64(0)
	for _, b := range filter.bitset {
		// Get the number of bits set in this byte
		setBits += uint64(bitsSetInByte(b))
	}

	return float64(setBits) / float64(filter.size)
}

// Counts the number of bits set in a byte
func bitsSetInByte(b byte) int {
	count := 0
	for b != 0 {
		count += int(b & 1)
		b = b >> 1
	}
	return count
}

// Calculates the optimal number of total bits (m) for the Bloom filter
func calculateOptimalBits(expectedKeys uint64, fpRate float64) uint64 {
	// optimalBits = number of bits * bitsPerKey
	// bitsPerKey = - (ln(p) / (ln(2)^2))

	bitsPerKey := -math.Log(fpRate) / (math.Ln2 * math.Ln2)

	// Multiply first, then convert (preserves precision)
	optimalBits := uint64(math.Ceil(float64(expectedKeys) * bitsPerKey))

	return optimalBits
}

// Calculates the optimal number of hash functions (k) for the Bloom filter
func calculateOptimalHashes(optimalBits, expectedKeys uint64) uint64 {
	// k = (m / n) * ln(2)
	k := (float64(optimalBits) / float64(expectedKeys)) * math.Ln2

	// Round to nearest integer, minimum 1
	optimalHashes := uint64(math.Round(k))
	if optimalHashes < 1 {
		optimalHashes = 1
	}

	return optimalHashes
}
