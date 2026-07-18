package processor

import (
	"context"
	"errors"

	"github.com/codecrafters-io/redis-starter-go/app/exctx"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type TransactionManager struct {
	transactions map[string][]*types.Command
	executor     CommandExecutor
}

type CommandExecutor interface {
	ExecuteCommand(ctx context.Context, command *types.Command) (*types.RedisData, error)
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

func (t *TransactionManager) ExecuteTransaction(ctx context.Context) (*types.RedisData, error) {
	if !t.HasTransaction(ctx) {
		return nil, errors.New("no transaction in progress")
	}
	transactions := t.transactions[exctx.FromContext(ctx).ConnectionId]
	delete(t.transactions, exctx.FromContext(ctx).ConnectionId)

	if len(transactions) == 0 {
		return &types.RedisData{
			Type:  types.Array,
			Holds: make([]*types.RedisData, 0),
		}, nil
	}

	results := make([]*types.RedisData, 0, len(transactions))
	for _, command := range transactions {
		result, err := t.executor.ExecuteCommand(ctx, command)
		if err != nil {
			// Flatten the errors
			result = &types.RedisData{
				Type: types.Error,
				Data: err.Error(),
			}
		}
		results = append(results, result)
	}

	return &types.RedisData{
		Type:  types.Array,
		Holds: results,
	}, nil
}

func (t *TransactionManager) AddToTransaction(ctx context.Context, command *types.Command) error {
	if !t.HasTransaction(ctx) {
		return errors.New("no transaction in progress")
	}
	t.transactions[exctx.FromContext(ctx).ConnectionId] = append(t.transactions[exctx.FromContext(ctx).ConnectionId], command)
	return nil
}

func (t *TransactionManager) RegisterExecutor(executor CommandExecutor) {
	t.executor = executor
}
