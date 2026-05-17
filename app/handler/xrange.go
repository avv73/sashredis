package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type XrangeStorage interface {
	QueryStream(ctx context.Context, streamKey string, startId string, endId string) ([]*types.RedisData, error)
}

type XrangeHandler struct {
	storage XrangeStorage
}

func NewXrangeHandler(storage XrangeStorage) *XrangeHandler {
	return &XrangeHandler{
		storage: storage,
	}
}

func (x *XrangeHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 3 {
		return nil, errors.New("unexpected number of arguments")
	}

	streamKey := command.Args[0]
	startId := command.Args[1]
	endId := command.Args[2]

	results, err := x.storage.QueryStream(ctx, streamKey.Data, startId.Data, endId.Data)
	if err != nil {
		return nil, err
	}

	return &types.RedisData{
		Type:  types.Array,
		Holds: results,
	}, nil
}
