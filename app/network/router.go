package network

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type CommandParser interface {
	ParseCommand(chunk []byte) (*types.Command, error)
}

type ResultEncoder interface {
	Encode(input *types.RedisData) ([]byte, error)
}

type EventBus interface {
	// Execute sends a command to the processor and blocks the caller until a response is received.
	Execute(ctx context.Context, command *types.Command) (*types.RedisData, error)
}

type RequestRouter struct {
	parser  CommandParser
	encoder ResultEncoder
	bus     EventBus
}

func NewRequestRouter(bus EventBus, parser CommandParser, encoder ResultEncoder) *RequestRouter {
	return &RequestRouter{
		bus:     bus,
		parser:  parser,
		encoder: encoder,
	}
}

func (r *RequestRouter) HandleConnection(ctx context.Context, connection net.Conn) error {
	defer connection.Close()
	reader := bufio.NewReader(connection)
	for {
		_, err := reader.ReadByte() // block until first byte
		if err != nil {
			return fmt.Errorf("byte read: %w", err)
		}
		reader.UnreadByte()

		message := make([]byte, reader.Buffered())
		_, err = reader.Read(message)
		if err != nil {
			return fmt.Errorf("connection read: %w", err)
		}

		log.Infof("got message: %q", message)

		command, err := r.parser.ParseCommand(message)
		if err != nil {
			log.WithError(err).Error("failed parsing command")
			r.writeResult(connection, types.NewRedisError(types.GeneralError, "Unable to parse command").AsRedisData())
			continue
		}

		result, err := r.bus.Execute(ctx, command)
		if err != nil {
			log.WithError(err).Error("execution failed")

			if redisErr, ok := errors.AsType[*types.RedisError](err); ok {
				r.writeResult(connection, redisErr.AsRedisData())
				continue
			}

			r.writeResult(connection, types.RedisErrorFromErr(types.GeneralError, err).AsRedisData())
			continue
		}

		encodedResult, err := r.encoder.Encode(result)
		if err != nil {
			log.WithError(err).Error("failed encoding result")
			r.writeResult(connection, types.NewRedisError(types.GeneralError, "Unexpected error occurred").AsRedisData())
			continue
		}

		_, err = connection.Write(encodedResult)
		if err != nil {
			log.WithError(err).Error("failed writing result")
			return fmt.Errorf("connection write: %w", err)
		}
	}
}

func (r *RequestRouter) writeResult(connection net.Conn, result *types.RedisData) error {
	encodedResult, err := r.encoder.Encode(result)
	if err != nil {
		return fmt.Errorf("encode error: %w", err)
	}

	_, err = connection.Write(encodedResult)
	if err != nil {
		return fmt.Errorf("connection write: %w", err)
	}

	return nil
}
