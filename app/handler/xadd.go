package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/storage"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type AddStreamStorage interface {
	AddToStreamWithCustomEntryKey(ctx context.Context, streamKey string, entryKey string, data []*storage.StreamKvp) (string, error)
}

type XaddHandler struct {
	storage AddStreamStorage
}

func NewXaddHandler(storage AddStreamStorage) *XaddHandler {
	return &XaddHandler{
		storage: storage,
	}
}

func (x *XaddHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) < 1 {
		return nil, errors.New("unexpected number of arguments")
	}

	streamKey := command.Args[0]
	kvpArguments := command.Args[1:]
	var entryId string
	if x.hasExplicitEntryId(command.Args[1:]) {
		entryId = command.Args[1].Data
		kvpArguments = command.Args[2:]
	}

	streamKvp, err := x.parseKvp(kvpArguments)
	if err != nil {
		return nil, err
	}

	xaddResult, err := x.storage.AddToStreamWithCustomEntryKey(ctx, streamKey.Data, entryId, streamKvp)
	if err != nil {
		return nil, err
	}

	return &types.RedisData{
		Type: types.BString,
		Data: xaddResult,
	}, nil
}

func (x *XaddHandler) hasExplicitEntryId(commandArgs []*types.RedisData) bool {
	// Take care of optional args later on.
	return len(commandArgs)%2 != 0
}

func (x *XaddHandler) parseKvp(commandArgs []*types.RedisData) ([]*storage.StreamKvp, error) {
	if len(commandArgs)%2 != 0 {
		return nil, errors.New("invalid number of kvp arguments")
	}

	result := make([]*storage.StreamKvp, 0, len(commandArgs))
	for i, val := range commandArgs {
		if i%2 == 0 {
			result = append(result, &storage.StreamKvp{
				Key: val.Data,
			})
		} else {
			result[len(result)-1].Value = val.Data
		}
	}
	return result, nil
}
