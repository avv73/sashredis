package processor

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/exctx"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type TransactionManager struct {
	transactions map[string][]*types.Command
}

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		transactions: make(map[string][]*types.Command),
	}
}

func (t *TransactionManager) HasTransaction(ctx context.Context) bool {
	_, ok := t.transactions[exctx.FromContext(ctx).ConnectionId]
	return ok
}

func (t *TransactionManager) BeginTransaction(ctx context.Context) error {
	if t.HasTransaction(ctx) {
		return errors.New("transaction already in progress")
	}
	t.transactions[exctx.FromContext(ctx).ConnectionId] = make([]*types.Command, 0, 10)
	return nil
}

func (t *TransactionManager) AddToTransaction(ctx context.Context, command *types.Command) error {
	if !t.HasTransaction(ctx) {
		return errors.New("no transaction in progress")
	}
	t.transactions[exctx.FromContext(ctx).ConnectionId] = append(t.transactions[exctx.FromContext(ctx).ConnectionId], command)
	return nil
}
