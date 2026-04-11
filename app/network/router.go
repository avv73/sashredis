package network

import (
	"bufio"
	"fmt"
	"io"
	"maps"
	"net"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type CommandHandler interface {
	HandleCommand(command *types.Command) (*types.RedisData, error)
}

type CommandParser interface {
	ParseCommand(chunk []byte) (*types.Command, error)
}

type ResultEncoder interface {
	Encode(input *types.RedisData) ([]byte, error)
}

type RequestRouter struct {
	handlers map[types.CommandName]CommandHandler
	parser   CommandParser
	encoder  ResultEncoder
}

func NewRequestRouter(handlers map[types.CommandName]CommandHandler, parser CommandParser, encoder ResultEncoder) *RequestRouter {
	return &RequestRouter{
		handlers: maps.Clone(handlers),
		parser:   parser,
		encoder:  encoder,
	}
}

func (r *RequestRouter) HandleConnection(connection net.Conn) error {
	defer connection.Close()

	reader := bufio.NewReader(connection)
	log.Infof("handle conn")
	message, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("connection read: %w", err)
	}

	log.Infof("got message: %s", message)

	command, err := r.parser.ParseCommand(message)
	if err != nil {
		return fmt.Errorf("failed parsing command: %w", err)
	}

	result, err := r.route(command)
	if err != nil {
		return fmt.Errorf("exec error: %w", err)
	}

	encodedResult, err := r.encoder.Encode(result)
	if err != nil {
		return fmt.Errorf("encode error: %w", err)
	}

	_, err = connection.Write([]byte(encodedResult))
	if err != nil {
		return fmt.Errorf("connection write: %w", err)
	}

	return nil
}

func (r *RequestRouter) route(command *types.Command) (*types.RedisData, error) {
	handler, ok := r.handlers[command.Command]
	if !ok {
		return nil, fmt.Errorf("command not registered: %s", string(command.Command))
	}

	log.Infoln("handling command")
	result, err := handler.HandleCommand(command)
	if err != nil {
		return nil, fmt.Errorf("command execution error: %w", err)
	}

	log.Infof("route result: %s", result)
	return result, nil
}
