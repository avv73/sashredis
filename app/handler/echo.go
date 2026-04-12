package handler

import (
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type EchoHandler struct {
}

func NewEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (*EchoHandler) HandleCommand(command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 1 {
		return nil, errors.New("unexpected number of arguments")
	}
	message := command.Args[0]

	return &types.RedisData{
		Type: types.BString,
		Data: message.Data,
	}, nil
}
