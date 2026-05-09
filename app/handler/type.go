package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type TyperStorage interface {
	Type(ctx context.Context, key string) string
}

type TypeHandler struct {
	storage TyperStorage
}

func NewTypeHandler(storage TyperStorage) *TypeHandler {
	return &TypeHandler{
		storage: storage,
	}
}

func (t *TypeHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 1 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0]
	typ := t.storage.Type(ctx, key.Data)

	return &types.RedisData{
		Type: types.SString,
		Data: typ,
	}, nil
}
