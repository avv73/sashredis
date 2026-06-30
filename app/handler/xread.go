package handler

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/storage"
	"github.com/codecrafters-io/redis-starter-go/app/types"
	log "github.com/sirupsen/logrus"
)

type XreadStorage interface {
	ScheduleReadStream(ctx context.Context, streamKeys []string, ids []string, timeout int64, callback func([]*types.RedisData, bool)) error
	ReadStream(ctx context.Context, streamKeys []string, ids []string) ([]*types.RedisData, error)
	Type(ctx context.Context, key string) storage.StorageType
}

type XreadHandler struct {
	storage       XreadStorage
	connUnblocker ConnUnblocker
}

func NewXreadHandler(storage XreadStorage, connUnblocker ConnUnblocker) *XreadHandler {
	return &XreadHandler{
		storage:       storage,
		connUnblocker: connUnblocker,
	}
}

func (x *XreadHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) < 3 {
		return nil, errors.New("unexpected number of arguments")
	}

	isBlocking := false

	var blockTimeoutMs int64
	var argsOffset int
	if strings.ToLower(command.Args[0].Data) == "block" {
		isBlocking = true

		var err error
		blockTimeoutMs, err = strconv.ParseInt(command.Args[1].Data, 10, 64)
		if err != nil {
			return nil, errors.New("expected timeout ms to be float")
		}
		argsOffset += 2
	} else if strings.ToLower(command.Args[0].Data) != "streams" {
		return nil, errors.New("expected first arg to be STREAMS")
	}

	streamKeys := make([]string, 0)
	entryIds := make([]string, 0)

	var entryStartIdx int
	for i, arg := range command.Args[1+argsOffset:] {
		if x.storage.Type(ctx, arg.Data) != storage.StorageTypeStream && x.isEntryId(arg.Data) {
			entryStartIdx = i
			break
		}
		streamKeys = append(streamKeys, arg.Data)
	}

	for _, entryId := range command.Args[entryStartIdx+1+argsOffset:] {
		entryIds = append(entryIds, entryId.Data)
	}

	result, err := x.storage.ReadStream(ctx, streamKeys, entryIds)
	if !isBlocking {
		if err != nil {
			return nil, err
		}

		return &types.RedisData{
			Type:  types.Array,
			Holds: result,
		}, nil
	}

	// blocking path
	if err != nil {
		return nil, err
	}

	if !x.isEmptyReadResponse(result) {
		return &types.RedisData{
			Type:  types.Array,
			Holds: result,
		}, nil
	}

	err = x.storage.ScheduleReadStream(ctx, streamKeys, entryIds, blockTimeoutMs, func(rd []*types.RedisData, b bool) {
		var result *types.RedisData
		if !b {
			result = types.NullArrayResponse
		} else {
			result = &types.RedisData{
				Type:  types.Array,
				Holds: rd,
			}
		}

		if unblockErr := x.connUnblocker.UnblockConn(ctx, result, nil); unblockErr != nil {
			log.WithError(unblockErr).Error("failed to unblock connection")
		}
	})

	if err != nil {
		return nil, err
	}

	return nil, types.ErrBlock
}

func (x *XreadHandler) isEntryId(input string) bool {
	ms, seq, err := types.ParseStreamEntryKey(input, true)
	return err == nil && ms != nil && seq != nil
}

func (x *XreadHandler) isEmptyReadResponse(response []*types.RedisData) bool {
	for _, kvp := range response {
		if kvp.Holds[1].Type != types.NullArray {
			return false
		}
	}
	return true
}
