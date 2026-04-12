package processor

import (
	"context"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type commandEvent struct {
	command    *types.Command
	responseCh chan *responseEvent
}

type responseEvent struct {
	redisData *types.RedisData
	err       error
}

type CommEventBus struct {
	commandCh chan *commandEvent
}

func NewEventBus() *CommEventBus {
	return &CommEventBus{
		commandCh: make(chan *commandEvent, config.GetConfig().CommandBufferCapacity),
	}
}

func (c *CommEventBus) Execute(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	event := &commandEvent{
		command:    command,
		responseCh: make(chan *responseEvent),
	}

	c.commandCh <- event
	select {
	case resp := <-event.responseCh:
		return resp.redisData, resp.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type ResultCallback func(*types.RedisData, error)

func (c *CommEventBus) GetCommand(ctx context.Context) (*types.Command, ResultCallback, error) {
	select {
	case cmd := <-c.commandCh:
		return cmd.command, func(rd *types.RedisData, err error) {
			event := &responseEvent{
				redisData: rd,
				err:       err,
			}
			cmd.responseCh <- event
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}
