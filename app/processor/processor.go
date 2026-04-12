package processor

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type EventBusPublisher interface {
	GetCommand(ctx context.Context) (*types.Command, ResultCallback, error)
}

type CommandHandler interface {
	HandleCommand(command *types.Command) (*types.RedisData, error)
}

type Processor struct {
	eventBus EventBusPublisher
	handlers map[types.CommandName]CommandHandler
}

func NewProcessor(eventBus EventBusPublisher, handlers map[types.CommandName]CommandHandler) *Processor {
	return &Processor{
		eventBus: eventBus,
		handlers: handlers,
	}
}

func (p *Processor) Start(ctx context.Context) {
	go func() {
		log.Info("main processor loop starting")
		for {
			command, callback, err := p.eventBus.GetCommand(ctx)
			if err != nil {
				log.WithError(err).Info("main processor terminating")
				return
			}

			result, err := p.executeCommand(command)
			callback(result, err)
		}
	}()
}

func (p *Processor) executeCommand(command *types.Command) (*types.RedisData, error) {
	handler, ok := p.handlers[command.Command]
	if !ok {
		return nil, fmt.Errorf("command not registered: %s", string(command.Command))
	}

	result, err := handler.HandleCommand(command)
	if err != nil {
		return nil, fmt.Errorf("command execution error: %w", err)
	}

	log.Infof("exec result: %s", result)
	return result, nil
}
