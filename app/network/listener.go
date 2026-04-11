package network

import (
	"errors"
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"
)

type RequestHandler interface {
	HandleConnection(connection net.Conn) error
}

type TCPListener struct {
	port    string
	handler RequestHandler
}

type ConnectionListener interface {
	StartListen() error
}

func NewTCPListener(port string, handler RequestHandler) ConnectionListener {
	return &TCPListener{
		port:    port,
		handler: handler,
	}
}

func (t *TCPListener) StartListen() error {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		return errors.New("failed to bind tcp to port 6379")
	}

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered. Error:\n", r)
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.WithError(err).Errorln("Error while accepting connection")
			continue
		}

		log.Info("accepting a new connection!")
		err = t.handler.HandleConnection(conn)
		log.Info("finished accepting")
		if err != nil {
			log.WithError(err).Errorln("Error while handling connection")
			continue
		}
	}

}
