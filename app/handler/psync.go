package handler

import (
	"context"
	"errors"
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/types"
	log "github.com/sirupsen/logrus"
)

type PSyncHandler struct {
}

func NewPSyncHandler() *PSyncHandler {
	return &PSyncHandler{}
}

func (*PSyncHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 2 {
		return nil, errors.New("unexpected number of arguments")
	}

	replicationId := command.Args[0].Data
	offset := command.Args[1].Data

	log.Infof("Received PSYNC from replica: replId: %s offset: %s\n", replicationId, offset)

	return &types.RedisData{
		Type: types.SString,
		Data: fmt.Sprintf("FULLRESYNC %s 0", replicationId),
	}, nil
}
