package handler

import (
	"context"
	"errors"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type ListStorage interface {
	AppendToList(key string, data *types.RedisData) (int, error)
}

type RpushHandler struct {
	storage ListStorage
}

func NewRpushHandler(storage ListStorage) *RpushHandler {
	return &RpushHandler{
		storage: storage,
	}
}

func (r *RpushHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 2 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0]
	value := command.Args[1]

	result, err := r.storage.AppendToList(key.Data, value)
	if err != nil {
		return nil, err
	}

	return &types.RedisData{
		Type: types.Integer,
		Data: strconv.Itoa(result),
	}, nil
}
