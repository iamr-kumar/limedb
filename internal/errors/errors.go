package errors

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidWalEntry      = errors.New("invalid WAL entry")
	ErrMissingRequiredField = func(field string) error {
		return errors.New("missing required field: " + field)
	}
	ErrInvalidValueForParameter = func(param string, value any) error {
		return errors.New("invalid value for parameter " + param + ": " + fmt.Sprint(value))
	}
)
