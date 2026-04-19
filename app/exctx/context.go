package exctx

import "context"

type contextKey struct{}

type ContextValue struct {
	ConnectionId string
}

func NewContext(ctx context.Context, val *ContextValue) context.Context {
	return context.WithValue(ctx, contextKey{}, val)
}

func FromContext(ctx context.Context) *ContextValue {
	v, ok := ctx.Value(contextKey{}).(*ContextValue)
	if !ok {
		return nil
	}
	return v
}
