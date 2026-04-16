package handler

import (
	"context"
	"errors"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type PrependStorage interface {
	PrependToList(key string, data *types.RedisData) (int, error)
}

type LpushHandler struct {
	storage PrependStorage
}

func NewLpushHandler(storage PrependStorage) *LpushHandler {
	return &LpushHandler{
		storage: storage,
	}
}

func (r *LpushHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) < 2 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0]
	var result int
	var err error

	for _, value := range command.Args[1:] {
		result, err = r.storage.PrependToList(key.Data, value)
		if err != nil {
			return nil, err
		}
	}

	return &types.RedisData{
		Type: types.Integer,
		Data: strconv.Itoa(result),
	}, nil
}
