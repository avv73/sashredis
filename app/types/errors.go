package types

import (
	"errors"
	"fmt"
)

type ErrorType string

const (
	GeneralError ErrorType = "ERR"
	WrongType    ErrorType = "WRONGTYPE"
)

type RedisError struct {
	Type    ErrorType
	Message error
}

// TODO: Add new method for
// support for passing a custom ErrorType from the command handler (string.contains on the error description?)
func RedisErrorFromErr(typ ErrorType, err error) *RedisError {
	return &RedisError{
		Type:    typ,
		Message: err,
	}
}

func NewRedisError(typ ErrorType, message string) *RedisError {
	return &RedisError{
		Type:    typ,
		Message: errors.New(message),
	}
}

func (r *RedisError) Error() string {
	return fmt.Sprintf("%s %s", r.Type, r.Message.Error())
}

func (r *RedisError) Unwrap() error {
	return r.Message
}

func (r *RedisError) AsRedisData() *RedisData {
	return &RedisData{
		Type: Error,
		Data: r.Error(),
	}
}

var ErrWrongType *RedisError = NewRedisError(WrongType, "Operation against a key holding the wrong kind of value")
var ErrValueNotInteger *RedisError = NewRedisError(GeneralError, "value is not an integer or out of range")
var ErrExecWithoutMulti *RedisError = NewRedisError(GeneralError, "EXEC without MULTI")

type BlockError struct {
}

func (r *BlockError) Error() string {
	return "blocking client connection"
}

// ErrBlock is a special kind of error that, when returned from a handler, signals to the event bus to immediately block the client connection and wait for Unblock from the handler.
var ErrBlock *BlockError = &BlockError{}
