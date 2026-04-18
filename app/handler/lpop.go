package handler

import (
	"context"
	"errors"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type PopStorage interface {
	PopList(key string, times int) (*types.RedisData, bool, error)
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
	if len(command.Args) > 2 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0].Data
	count := 1

	if len(command.Args) > 1 {
		var err error
		count, err = strconv.Atoi(command.Args[1].Data)
		if err != nil {
			return nil, errors.New("argument should be a positive integer")
		}
	}

	result, ok, err := l.storage.PopList(key, count)
	if err != nil {
		return nil, err
	}
	if !ok {
		return types.NullResponse, nil
	}

	return result, nil
}
