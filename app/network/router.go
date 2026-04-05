package network

import (
	"bufio"
	"fmt"
	"maps"
	"net"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/command"
)

type CommandHandler interface {
	HandleCommand(command *command.Command) (string, error)
}

type RequestRouter struct {
	handlers map[command.CommandName]CommandHandler
}

func NewRequestRouter(handlers map[command.CommandName]CommandHandler) *RequestRouter {
	return &RequestRouter{
		handlers: maps.Clone(handlers),
	}
}

func (r *RequestRouter) HandleConnection(connection net.Conn) error {
	defer connection.Close()
	reader := bufio.NewReader(connection)
	message, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("connection read: %w", err)
	}

	log.Infof("got message: %s", message)

	command := r.parseCommand(message)
	result, err := r.route(command)

	_, err = connection.Write([]byte(result))
	if err != nil {
		return fmt.Errorf("connection write: %w", err)
	}

	return nil
}

func (r *RequestRouter) parseCommand(message string) *command.Command {
	message = strings.ToUpper(strings.TrimSpace(message))
	messageTokens := strings.Split(message, " ")

	msgCommand := messageTokens[0]
	var args []string
	if len(messageTokens) > 0 {
		args = messageTokens[1:]
	}

	return &command.Command{
		Command: command.CommandName(msgCommand),
		Args:    args,
	}
}

func (r *RequestRouter) route(command *command.Command) (string, error) {
	handler, ok := r.handlers[command.Command]
	if !ok {
		return "", fmt.Errorf("command not registered: %s", command)
	}

	log.Infoln("handling command")
	result, err := handler.HandleCommand(command)
	if err != nil {
		return "", fmt.Errorf("command execution error: %w", err)
	}

	log.Infof("route result: %s", result)
	return result, nil
}
