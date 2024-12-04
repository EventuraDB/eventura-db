package types

import "fmt"

type NotFoundError struct {
	OriginalError error
	Message       string
}

func (e *NotFoundError) Error() string {
	if e.OriginalError != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.OriginalError)
	}
	return e.Message
}

func NewNotFoundError(msg string, err error) *NotFoundError {
	return &NotFoundError{
		Message:       msg,
		OriginalError: err,
	}
}
