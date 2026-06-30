package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type IncrStorage interface {
	Increment(ctx context.Context, key string) (*types.RedisData, error)
}

type IncrHandler struct {
	storage IncrStorage
}

func NewIncrHandler(storage IncrStorage) *IncrHandler {
	return &IncrHandler{
		storage: storage,
	}
}

func (s *IncrHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 1 {
		return nil, errors.New("expected a single argument")
	}

	key := command.Args[0].Data

	result, err := s.storage.Increment(ctx, key)
	if err != nil {
		return nil, err
	}

	return result, nil
}
