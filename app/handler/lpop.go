package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type PopStorage interface {
	PopList(key string) (*types.RedisData, bool, error)
}

type LpopHandler struct {
	storage PopStorage
}

func NewLpopHandler(storage PopStorage) *LpopHandler {
	return &LpopHandler{
		storage: storage,
	}
}

func (l *LpopHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) > 1 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0].Data

	result, ok, err := l.storage.PopList(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return types.NullResponse, nil
	}

	return result, nil
}
