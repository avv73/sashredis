package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type TransactionHandler interface {
	BeginTransaction(ctx context.Context) error
}

type MultiHandler struct {
	transactionHandler TransactionHandler
}

func NewMultiHandler(transactionHandler TransactionHandler) *MultiHandler {
	return &MultiHandler{
		transactionHandler: transactionHandler,
	}
}

func (l *MultiHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) > 0 {
		return nil, errors.New("unexpected number of arguments")
	}

	err := l.transactionHandler.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}

	return types.OkResponse, nil
}
