package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
	log "github.com/sirupsen/logrus"
)

type BlockPopStorage interface {
	PopList(key string, times int) (*types.RedisData, bool, error)
	BlockOnPopList(key string) (*types.RedisData, error)
}

type ConnUnblocker interface {
	UnblockConn(ctx context.Context, data *types.RedisData, err error) error
}

type BlpopHandler struct {
	storage       BlockPopStorage
	connUnblocker ConnUnblocker
}

func NewBlpopHandler(storage BlockPopStorage, connUnblocker ConnUnblocker) *BlpopHandler {
	return &BlpopHandler{
		storage:       storage,
		connUnblocker: connUnblocker,
	}
}

func (s *BlpopHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 2 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0].Data
	result, ok, err := s.storage.PopList(key, 1)
	if err != nil {
		return nil, err
	}

	if ok {
		return &types.RedisData{
			Type: types.Array,
			Holds: []*types.RedisData{
				{
					Type: types.BString,
					Data: key,
				},
				result,
			},
		}, nil
	}

	go func() {
		resultData, exErr := s.storage.BlockOnPopList(key)
		result := &types.RedisData{
			Type: types.Array,
			Holds: []*types.RedisData{
				{
					Type: types.BString,
					Data: key,
				},
				resultData,
			},
		}
		if unblockErr := s.connUnblocker.UnblockConn(ctx, result, exErr); unblockErr != nil {
			log.WithError(unblockErr).Error("failed to unblock connection")
		}
	}()

	return nil, types.ErrBlock
}
