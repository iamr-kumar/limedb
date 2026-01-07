package errors

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidWalEntry      = errors.New("invalid WAL entry")
	ErrMemtableFull         = errors.New("memtable is full, new one would be created")
	ErrMemtableImmutable    = errors.New("memtable is immutable and cannot be modified")
	ErrInvalidInternalKey   = errors.New("invalid internal key")
	ErrMissingRequiredField = func(field string) error {
		return errors.New("missing required field: " + field)
	}
	ErrInvalidValueForParameter = func(param string, value any) error {
		return errors.New("invalid value for parameter " + param + ": " + fmt.Sprint(value))
	}
)
