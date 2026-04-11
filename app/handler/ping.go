package handler

import (
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type PingHandler struct {
}

func NewPingHandler() *PingHandler {
	return &PingHandler{}
}

func (*PingHandler) HandleCommand(command *types.Command) (*types.RedisData, error) {
	if len(command.Args) > 0 {
		return nil, errors.New("unexpected number of arguments")
	}

	return &types.RedisData{
		Type: types.SString,
		Data: "PONG",
	}, nil
}
