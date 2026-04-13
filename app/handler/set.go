package handler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/storage"
	"github.com/codecrafters-io/redis-starter-go/app/types"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type SetterStorage interface {
	SetKvp(ctx context.Context, key string, data *types.RedisData, opts ...storage.SetKvpOpts)
}

type SetHandler struct {
	storage SetterStorage
}

func NewSetHandler(storage SetterStorage) *SetHandler {
	return &SetHandler{
		storage: storage,
	}
}

func (s *SetHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) < 2 {
		return nil, errors.New("unexpected number of arguments")
	}

	key := command.Args[0]
	val := command.Args[1]

	args, err := s.parseOptionalArgs(command)
	if err != nil {
		return nil, err
	}

	storageOpts := s.setOptionalArgs(args)
	s.storage.SetKvp(ctx, key.Data, val, storageOpts...)

	return types.OkResponse, nil
}

type setArgs struct {
	Ex *int
}

func (s *SetHandler) setOptionalArgs(args *setArgs) []storage.SetKvpOpts {
	result := make([]storage.SetKvpOpts, 0)
	if args.Ex != nil {
		result = append(result, storage.WithMillisecondsExp(*args.Ex))
	}
	return result
}

func (s *SetHandler) parseOptionalArgs(command *types.Command) (*setArgs, error) {
	optionalArgs := &setArgs{}
	var err error
	for i := 2; i < len(command.Args); i += 2 {
		if i+1 >= len(command.Args) {
			return nil, errors.New("invalid number of arguments")
		}

		argName := strings.ToUpper(command.Args[i].Data)
		parameter := command.Args[i+1]

		switch argName {
		case "EX":
			err = errors.Join(err, s.parseEx(parameter, optionalArgs))
		case "PX":
			err = errors.Join(err, s.parsePx(parameter, optionalArgs))
		default:
			err = errors.Join(err, fmt.Errorf("unknown argument %s", argName))
		}
	}

	return optionalArgs, err
}

func (s *SetHandler) parseEx(parameter *types.RedisData, optionalArgs *setArgs) error {
	if optionalArgs.Ex != nil {
		return errors.New("expected only one: ex or px")
	}
	exVal, err := strconv.Atoi(parameter.Data)
	if err != nil {
		return errors.New("EX should be an integer")
	}
	optionalArgs.Ex = utils.ToPtr(exVal * 1000)
	return nil
}

func (s *SetHandler) parsePx(parameter *types.RedisData, optionalArgs *setArgs) error {
	if optionalArgs.Ex != nil {
		return errors.New("expected only one: ex or px")
	}
	pxVal, err := strconv.Atoi(parameter.Data)
	if err != nil {
		return errors.New("EX should be an integer")
	}
	optionalArgs.Ex = &pxVal
	return nil
}
