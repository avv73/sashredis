package processor

import (
	"context"
	"errors"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/exctx"
	"github.com/codecrafters-io/redis-starter-go/app/types"
	log "github.com/sirupsen/logrus"
)

type commandEvent struct {
	command    *types.Command
	responseCh chan *responseEvent
	execCtx    context.Context
}

type responseEvent struct {
	redisData  *types.RedisData
	blockingCh chan *responseEvent // set to non-nil command/processor to block the client connection, bus should then block on the channel
	err        error
}

type CommEventBus struct {
	commandCh     chan *commandEvent
	blockedConn   map[string]chan *responseEvent
	blockedConnMu sync.Mutex
}

func NewEventBus() *CommEventBus {
	return &CommEventBus{
		commandCh:   make(chan *commandEvent, config.GetConfig().CommandBufferCapacity),
		blockedConn: map[string]chan *responseEvent{},
	}
}

func (c *CommEventBus) Execute(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	event := &commandEvent{
		command:    command,
		responseCh: make(chan *responseEvent),
		execCtx:    ctx,
	}

	c.commandCh <- event
	select {
	case resp := <-event.responseCh:
		if resp.err != nil && errors.Is(resp.err, types.ErrBlock) {
			log.Warn("client connection blocked")
			c.addBlockedConn(ctx, resp.blockingCh)
			newResp := <-resp.blockingCh
			log.Warn("client connection unblocked")
			return newResp.redisData, newResp.err
		}

		return resp.redisData, resp.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type ResultCallback func(*types.RedisData, error)

func (c *CommEventBus) GetCommand(ctx context.Context) (*types.Command, context.Context, ResultCallback, error) {
	select {
	case cmd := <-c.commandCh:
		return cmd.command, cmd.execCtx, func(rd *types.RedisData, err error) {
			event := &responseEvent{
				redisData: rd,
				err:       err,
			}

			if errors.Is(err, types.ErrBlock) {
				event.blockingCh = make(chan *responseEvent, 1)
			}

			cmd.responseCh <- event
		}, nil
	case <-ctx.Done():
		return nil, nil, nil, ctx.Err()
	}
}

func (c *CommEventBus) UnblockConn(ctx context.Context, data *types.RedisData, err error) error {
	c.blockedConnMu.Lock()
	defer c.blockedConnMu.Unlock()
	connId := exctx.FromContext(ctx).ConnectionId
	blockCh, ok := c.blockedConn[connId]
	if !ok {
		return errors.New("no such connection found to unblock")
	}

	delete(c.blockedConn, connId)
	blockCh <- &responseEvent{
		redisData: data,
		err:       err,
	}
	return nil
}

func (c *CommEventBus) addBlockedConn(ctx context.Context, blockCh chan *responseEvent) {
	c.blockedConnMu.Lock()
	defer c.blockedConnMu.Unlock()
	c.blockedConn[exctx.FromContext(ctx).ConnectionId] = blockCh
}
