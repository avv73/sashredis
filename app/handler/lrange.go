package handler

import (
	"context"
	"errors"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type RangeStorage interface {
	FetchFromList(key string, startIdx int, endIdx int) ([]*types.RedisData, error)
}

type LrangeHandler struct {
	storage RangeStorage
}

func NewLrangeHandler(storage RangeStorage) *LrangeHandler {
	return &LrangeHandler{
		storage: storage,
	}
}

func (l *LrangeHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 3 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0]
	start := command.Args[1]
	end := command.Args[2]

	startIdx, err := strconv.Atoi(start.Data)
	if err != nil {
		return nil, errors.New("start index should be a valid integer")
	}

	endIdx, err := strconv.Atoi(end.Data)
	if err != nil {
		return nil, errors.New("end index should be a valid integer")
	}

	result, err := l.storage.FetchFromList(key.Data, startIdx, endIdx)
	if err != nil {
		return nil, err
	}

	return &types.RedisData{
		Type:  types.Array,
		Holds: result,
	}, nil
}
