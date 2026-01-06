package keys

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeInternalKey(t *testing.T) {
	cases := []struct {
		name string
		key  InternalKey
	}{
		{
			name: "simple value",
			key:  InternalKey{UserKey: []byte("a"), SequenceID: 1, Type: TypeValue},
		},
		{
			name: "tombstone",
			key:  InternalKey{UserKey: []byte("hello"), SequenceID: 42, Type: TypeDeletion},
		},
		{
			name: "empty user key",
			key:  InternalKey{UserKey: []byte{}, SequenceID: 99, Type: TypeValue},
		},
		{
			name: "large sequence",
			key:  InternalKey{UserKey: []byte("z"), SequenceID: ^uint64(0), Type: TypeValue},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.key.Encode()
			decoded, err := DecodeInternalKey(encoded)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !bytes.Equal(decoded.UserKey, tt.key.UserKey) {
				t.Fatalf("user key mismatch: got %q want %q", decoded.UserKey, tt.key.UserKey)
			}
			if decoded.SequenceID != tt.key.SequenceID {
				t.Fatalf("sequence mismatch: got %d want %d", decoded.SequenceID, tt.key.SequenceID)
			}
			if decoded.Type != tt.key.Type {
				t.Fatalf("type mismatch: got %d want %d", decoded.Type, tt.key.Type)
			}
		})
	}
}

func TestDecodeInternalKeyErrors(t *testing.T) {
	badPayloads := [][]byte{
		{},
		{0},
		{0, 1, 2, 3, 4, 5, 6, 7},
		// Invalid type (2) with minimal valid length
		func() []byte {
			buf := make([]byte, 9)
			buf[0] = 2 // invalid type
			// rest zeroes are fine for seq/user key; length is valid but type is not
			return buf
		}(),
	}

	for i, payload := range badPayloads {
		if _, err := DecodeInternalKey(payload); err == nil {
			t.Fatalf("case %d: expected error for payload len %d", i, len(payload))
		}
	}
}

func TestCompareInternalKeys_UserKeyOrdering(t *testing.T) {
	a := &InternalKey{UserKey: []byte("a"), SequenceID: 10, Type: TypeValue}
	b := &InternalKey{UserKey: []byte("b"), SequenceID: 1, Type: TypeValue}

	if got := CompareInternalKeys(a, b); got >= 0 {
		t.Fatalf("expected a<b, got %d", got)
	}
	if got := CompareInternalKeys(b, a); got <= 0 {
		t.Fatalf("expected b>a, got %d", got)
	}
}

func TestCompareInternalKeys_SequenceOrdering(t *testing.T) {
	newer := &InternalKey{UserKey: []byte("a"), SequenceID: 5, Type: TypeValue}
	older := &InternalKey{UserKey: []byte("a"), SequenceID: 1, Type: TypeValue}

	if got := CompareInternalKeys(newer, older); got != -1 {
		t.Fatalf("newer should come first (got %d)", got)
	}
	if got := CompareInternalKeys(older, newer); got != 1 {
		t.Fatalf("older should come after (got %d)", got)
	}
}

func TestComesBeforeAfter(t *testing.T) {
	base := &InternalKey{UserKey: []byte("a"), SequenceID: 2, Type: TypeValue}
	newer := &InternalKey{UserKey: []byte("a"), SequenceID: 3, Type: TypeDeletion}
	otherKey := &InternalKey{UserKey: []byte("b"), SequenceID: 1, Type: TypeValue}

	if !newer.ComesBefore(base) {
		t.Fatalf("newer sequence should come before base")
	}
	if !base.ComesAfter(newer) {
		t.Fatalf("base should come after newer")
	}

	if base.ComesBefore(base) || base.ComesAfter(base) {
		t.Fatalf("equal keys should neither come before nor after")
	}

	if !base.ComesBefore(otherKey) {
		t.Fatalf("user key ordering should rank 'a' before 'b'")
	}
}

func TestDecodeInternalKeyCopiesUserKey(t *testing.T) {
	orig := InternalKey{UserKey: []byte("key"), SequenceID: 7, Type: TypeValue}
	encoded := orig.Encode()
	decoded, err := DecodeInternalKey(encoded)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// mutate decoded key to ensure it does not alias encoded buffer
	decoded.UserKey[0] = 'K'
	if bytes.Equal(encoded[len(encoded)-len(orig.UserKey):], decoded.UserKey) {
		t.Fatalf("decoded UserKey should be a copy, not alias original buffer")
	}
}
