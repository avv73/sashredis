package handler

import (
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type SetterStorage interface {
	SetKvp(key string, data *types.RedisData)
}

type SetHandler struct {
	storage SetterStorage
}

func NewSetHandler(storage SetterStorage) *SetHandler {
	return &SetHandler{
		storage: storage,
	}
}

func (s *SetHandler) HandleCommand(command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 2 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0]
	val := command.Args[1]

	s.storage.SetKvp(key.Data, val)
	return types.OkResponse, nil
}
