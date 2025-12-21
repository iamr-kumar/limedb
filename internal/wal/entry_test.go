package wal

import (
	"bytes"
	"testing"
	"time"
)

func TestWalEntry_SerializeDeserialize(t *testing.T) {
	tests := []struct {
		name  string
		entry WalEntry
	}{
		{
			name: "basic set operation",
			entry: WalEntry{
				Timestamp: time.Unix(1703155200, 0),
				Operation: OperationSet,
				Key:       "user:1001",
				Value:     "alice",
				ExpiresAt: 0,
			},
		},
		{
			name: "set with TTL",
			entry: WalEntry{
				Timestamp: time.Unix(1703155200, 0),
				Operation: OperationSetWithTTL,
				Key:       "session:abc",
				Value:     "session_data",
				ExpiresAt: 1703158800, // 1 hour later
			},
		},
		{
			name: "delete operation",
			entry: WalEntry{
				Timestamp: time.Unix(1703155200, 0),
				Operation: OperationDelete,
				Key:       "temp:key",
				Value:     "",
				ExpiresAt: 0,
			},
		},
		{
			name: "large value",
			entry: WalEntry{
				Timestamp: time.Unix(1703155200, 0),
				Operation: OperationSet,
				Key:       "data:large",
				Value:     string(make([]byte, 1024)),
				ExpiresAt: 0,
			},
		},

		{
			name: "max ExpiresAt",
			entry: WalEntry{
				Timestamp: time.Unix(1703155200, 0),
				Operation: OperationSetWithTTL,
				Key:       "forever:key",
				Value:     "data",
				ExpiresAt: 1<<64 - 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			data, err := tt.entry.Serialize()
			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			// Deserialize
			reader := bytes.NewReader(data)
			got, err := DeserializeWalEntry(reader)
			if err != nil {
				t.Fatalf("DeserializeWalEntry() error = %v", err)
			}

			// Compare fields
			if got.Timestamp.Unix() != tt.entry.Timestamp.Unix() {
				t.Errorf("Timestamp = %v, want %v", got.Timestamp, tt.entry.Timestamp)
			}
			if got.Operation != tt.entry.Operation {
				t.Errorf("Operation = %v, want %v", got.Operation, tt.entry.Operation)
			}
			if got.Key != tt.entry.Key {
				t.Errorf("Key = %v, want %v", got.Key, tt.entry.Key)
			}
			if got.Value != tt.entry.Value {
				t.Errorf("Value = %v, want %v", got.Value, tt.entry.Value)
			}
			if got.ExpiresAt != tt.entry.ExpiresAt {
				t.Errorf("ExpiresAt = %v, want %v", got.ExpiresAt, tt.entry.ExpiresAt)
			}
			if got.Checksum == 0 {
				t.Error("Checksum should not be zero")
			}
		})
	}
}

func TestWalEntry_Serialize_EmptyKey(t *testing.T) {
	entry := WalEntry{
		Timestamp: time.Now(),
		Operation: OperationSet,
		Key:       "",
		Value:     "value",
	}

	_, err := entry.Serialize()
	if err == nil {
		t.Error("Serialize() should fail with empty key")
	}
}

func TestWalEntry_Serialize_KeyTooLarge(t *testing.T) {
	entry := WalEntry{
		Timestamp: time.Now(),
		Operation: OperationSet,
		Key:       string(make([]byte, MaxEntrySize+1)),
		Value:     "value",
	}

	_, err := entry.Serialize()
	if err == nil {
		t.Error("Serialize() should fail with key too large")
	}
}

func TestWalEntry_Serialize_ValueTooLarge(t *testing.T) {
	entry := WalEntry{
		Timestamp: time.Now(),
		Operation: OperationSet,
		Key:       "key",
		Value:     string(make([]byte, MaxEntrySize+1)),
	}

	_, err := entry.Serialize()
	if err == nil {
		t.Error("Serialize() should fail with value too large")
	}
}

func TestDeserializeWalEntry_InvalidChecksum(t *testing.T) {
	entry := WalEntry{
		Timestamp: time.Unix(1703155200, 0),
		Operation: OperationSet,
		Key:       "key",
		Value:     "value",
	}

	data, err := entry.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	// Corrupt the value (near the end of the buffer)
	data[len(data)-3] ^= 0xFF

	reader := bytes.NewReader(data)
	_, err = DeserializeWalEntry(reader)
	if err == nil {
		t.Error("DeserializeWalEntry() should fail with corrupted data")
	}
}

func TestDeserializeWalEntry_TruncatedData(t *testing.T) {
	entry := WalEntry{
		Timestamp: time.Unix(1703155200, 0),
		Operation: OperationSet,
		Key:       "key",
		Value:     "value",
	}

	data, err := entry.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	// Truncate the data
	truncated := data[:len(data)/2]
	reader := bytes.NewReader(truncated)
	_, err = DeserializeWalEntry(reader)
	if err == nil {
		t.Error("DeserializeWalEntry() should fail with truncated data")
	}
}

func TestDeserializeWalEntry_EmptyReader(t *testing.T) {
	reader := bytes.NewReader([]byte{})
	_, err := DeserializeWalEntry(reader)
	if err == nil {
		t.Error("DeserializeWalEntry() should fail with empty reader")
	}
}

func TestWalEntry_MultipleEntriesInSequence(t *testing.T) {
	entries := []WalEntry{
		{
			Timestamp: time.Unix(1703155200, 0),
			Operation: OperationSet,
			Key:       "key1",
			Value:     "value1",
		},
		{
			Timestamp: time.Unix(1703155201, 0),
			Operation: OperationSetWithTTL,
			Key:       "key2",
			Value:     "value2",
			ExpiresAt: 1703158800,
		},
		{
			Timestamp: time.Unix(1703155202, 0),
			Operation: OperationDelete,
			Key:       "key1",
			Value:     "",
		},
	}

	// Serialize all entries into a single buffer
	var buf bytes.Buffer
	for _, e := range entries {
		data, err := e.Serialize()
		if err != nil {
			t.Fatalf("Serialize() error = %v", err)
		}
		buf.Write(data)
	}

	// Deserialize all entries
	reader := bytes.NewReader(buf.Bytes())
	for i, expected := range entries {
		got, err := DeserializeWalEntry(reader)
		if err != nil {
			t.Fatalf("DeserializeWalEntry() entry %d error = %v", i, err)
		}
		if got.Key != expected.Key {
			t.Errorf("entry %d: Key = %v, want %v", i, got.Key, expected.Key)
		}
		if got.Value != expected.Value {
			t.Errorf("entry %d: Value = %v, want %v", i, got.Value, expected.Value)
		}
	}
}

func TestGetDataForChecksum_Consistency(t *testing.T) {
	entry := &WalEntry{
		Operation: OperationSet,
		Key:       "test_key",
		Value:     "test_value",
		ExpiresAt: 12345,
	}

	// Call twice, should return identical results
	data1 := getDataForChecksum(entry)
	data2 := getDataForChecksum(entry)

	if !bytes.Equal(data1, data2) {
		t.Error("getDataForChecksum() should return consistent results")
	}
}

func TestWalEntry_SpecialCharactersInKeyValue(t *testing.T) {
	entry := WalEntry{
		Timestamp: time.Unix(1703155200, 0),
		Operation: OperationSet,
		Key:       "key\x00with\nnull\tand\rspecial",
		Value:     "value\x00\x01\x02\xff",
	}

	data, err := entry.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	reader := bytes.NewReader(data)
	got, err := DeserializeWalEntry(reader)
	if err != nil {
		t.Fatalf("DeserializeWalEntry() error = %v", err)
	}

	if got.Key != entry.Key {
		t.Errorf("Key = %q, want %q", got.Key, entry.Key)
	}
	if got.Value != entry.Value {
		t.Errorf("Value = %q, want %q", got.Value, entry.Value)
	}
}

func TestWalEntry_UnicodeKeyValue(t *testing.T) {
	entry := WalEntry{
		Timestamp: time.Unix(1703155200, 0),
		Operation: OperationSet,
		Key:       "用户:1001",
		Value:     "こんにちは世界 🌍",
	}

	data, err := entry.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	reader := bytes.NewReader(data)
	got, err := DeserializeWalEntry(reader)
	if err != nil {
		t.Fatalf("DeserializeWalEntry() error = %v", err)
	}

	if got.Key != entry.Key {
		t.Errorf("Key = %q, want %q", got.Key, entry.Key)
	}
	if got.Value != entry.Value {
		t.Errorf("Value = %q, want %q", got.Value, entry.Value)
	}
}
