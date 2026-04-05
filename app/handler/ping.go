package handler

import (
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/command"
)

type PingHandler struct {
}

func NewPingHandler() *PingHandler {
	return &PingHandler{}
}

func (*PingHandler) HandleCommand(command *command.Command) (string, error) {
	if len(command.Args) > 0 {
		return "", errors.New("unexpected number of arguments")
	}

	return "+PONG\r\n", nil
}
