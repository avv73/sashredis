package network

import (
	"context"
	"errors"
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/config"
)

type RequestHandler interface {
	HandleConnection(ctx context.Context, connection net.Conn) error
}

type TCPListener struct {
	port    string
	handler RequestHandler
}

type ConnectionListener interface {
	StartListen(ctx context.Context) error
}

func NewTCPListener(port string, handler RequestHandler) ConnectionListener {
	return &TCPListener{
		port:    port,
		handler: handler,
	}
}

func (t *TCPListener) StartListen(ctx context.Context) error {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.GetConfig().Port))
	if err != nil {
		return errors.New("failed to bind tcp to port 6379")
	}

	for range config.GetConfig().MaxConnections {
		go func() {
			for {
				conn, err := l.Accept()
				if err != nil {
					log.WithError(err).Errorln("Error while accepting connection")
					continue
				}

				err = t.handler.HandleConnection(ctx, conn)
				if err != nil {
					log.WithError(err).Errorln("Error while handling connection")
					continue
				}
			}
		}()
	}

	return nil
}
