package handler

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type TransactionDiscardHandler interface {
	HasTransaction(ctx context.Context) bool
	AbortTransaction(ctx context.Context) error
}

type DiscardHandler struct {
	transactionHandler TransactionDiscardHandler
}

func NewDiscardHandler(transactionHandler TransactionDiscardHandler) *DiscardHandler {
	return &DiscardHandler{
		transactionHandler: transactionHandler,
	}
}

func (d *DiscardHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) > 0 {
		return nil, errors.New("expected no arguments")
	}

	ok := d.transactionHandler.HasTransaction(ctx)
	if !ok {
		return nil, types.ErrDiscardWithoutMulti
	}

	err := d.transactionHandler.AbortTransaction(ctx)
	if err != nil {
		return nil, err
	}

	return types.OkResponse, nil
}
