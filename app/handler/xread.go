package handler

import (
	"context"
	"errors"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/storage"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type XreadStorage interface {
	ReadStream(ctx context.Context, streamKeys []string, ids []string) ([]*types.RedisData, error)
	Type(ctx context.Context, key string) storage.StorageType
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
	if len(command.Args) < 3 {
		return nil, errors.New("unexpected number of arguments")
	}
	if strings.ToLower(command.Args[0].Data) != "streams" {
		return nil, errors.New("expected first arg to be STREAMS")
	}

	streamKeys := make([]string, 0)
	entryIds := make([]string, 0)

	var entryStartIdx int
	for i, arg := range command.Args[1:] {
		if x.storage.Type(ctx, arg.Data) != storage.StorageTypeStream && x.isEntryId(arg.Data) {
			entryStartIdx = i
			break
		}
		streamKeys = append(streamKeys, arg.Data)
	}

	for _, entryId := range command.Args[entryStartIdx:] {
		entryIds = append(entryIds, entryId.Data)
	}

	result, err := x.storage.ReadStream(ctx, streamKeys, entryIds)
	if err != nil {
		return nil, err
	}

	return &types.RedisData{
		Type:  types.Array,
		Holds: result,
	}, nil
}

func (x *XreadHandler) isEntryId(input string) bool {
	ms, seq, err := types.ParseStreamEntryKey(input, true)
	return err == nil && ms != nil && seq != nil
}
