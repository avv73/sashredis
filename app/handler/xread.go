package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type XreadStorage interface {
	ReadStream(ctx context.Context, streamKey string, id string) ([]*types.RedisData, error)
}

type XreadHandler struct {
	storage XreadStorage
}

func NewXreadHandler(storage XreadStorage) *XreadHandler {
	return &XreadHandler{
		storage: storage,
	}
}

func (x *XreadHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 3 {
		return nil, errors.New("unexpected number of arguments")
	}
	if command.Args[0].Data != "STREAMS" {
		return nil, errors.New("expected first arg to be STREAMS")
	}

	streamKey := command.Args[1].Data
	entryId := command.Args[2].Data

	result, err := x.storage.ReadStream(ctx, streamKey, entryId)
	if err != nil {
		return nil, err
	}

	return &types.RedisData{
		Type:  types.Array,
		Holds: result,
	}, nil
}
