package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type GetterStorage interface {
	GetKvp(key string) (*types.RedisData, bool)
}

type GetHandler struct {
	storage GetterStorage
}

func NewGetHandler(storage GetterStorage) *GetHandler {
	return &GetHandler{
		storage: storage,
	}
}

func (s *GetHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 1 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0]

	result, ok := s.storage.GetKvp(key.Data)
	if !ok {
		return types.NullResponse, nil
	}

	return result, nil
}
