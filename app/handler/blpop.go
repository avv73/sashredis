package handler

import (
	"context"
	"errors"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/types"
	log "github.com/sirupsen/logrus"
)

type BlockPopStorage interface {
	PopList(key string, times int) (*types.RedisData, bool, error)
	BlockOnPopList(key string, timeout float64) (*types.RedisData, bool, error)
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
	timeout, err := strconv.ParseFloat(command.Args[1].Data, 32)
	if err != nil {
		return nil, errors.New("expected timeout to be a positive float")
	}

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
		resultData, ok, exErr := s.storage.BlockOnPopList(key, timeout)

		var result *types.RedisData
		if !ok {
			result = types.NullArrayResponse
		} else {
			result = &types.RedisData{
				Type: types.Array,
				Holds: []*types.RedisData{
					{
						Type: types.BString,
						Data: key,
					},
					resultData,
				},
			}
		}

		if unblockErr := s.connUnblocker.UnblockConn(ctx, result, exErr); unblockErr != nil {
			log.WithError(unblockErr).Error("failed to unblock connection")
		}
	}()

	return nil, types.ErrBlock
}
