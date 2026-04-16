package handler

import (
	"context"
	"errors"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type LengthStorage interface {
	ListLength(key string) (int, error)
}

type LlenHandler struct {
	storage LengthStorage
}

func NewLlenHandler(storage LengthStorage) *LlenHandler {
	return &LlenHandler{
		storage: storage,
	}
}

func (l *LlenHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 1 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0]

	result, err := l.storage.ListLength(key.Data)
	if err != nil {
		return nil, err
	}

	return &types.RedisData{
		Type: types.Integer,
		Data: strconv.Itoa(result),
	}, nil
}
