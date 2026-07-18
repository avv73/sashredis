package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type TransactionExecHandler interface {
	HasTransaction(ctx context.Context) bool
}

type ExecHandler struct {
	transactionHandler TransactionExecHandler
}

func NewExecHandler(transactionHandler TransactionExecHandler) *ExecHandler {
	return &ExecHandler{
		transactionHandler: transactionHandler,
	}
}

func (e *ExecHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) > 0 {
		return nil, errors.New("unexpected number of arguments")
	}

	ok := e.transactionHandler.HasTransaction(ctx)
	if !ok {
		return nil, types.ErrExecWithoutMulti
	}

	return types.OkResponse, nil
}
