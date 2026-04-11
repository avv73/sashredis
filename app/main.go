package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/handler"
	"github.com/codecrafters-io/redis-starter-go/app/marshal"
	"github.com/codecrafters-io/redis-starter-go/app/network"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

func main() {
	log.Println("===SASHKO REDIS===")

	pingHandler := handler.NewPingHandler()

	handlers := map[types.CommandName]network.CommandHandler{
		types.Ping: pingHandler,
	}

	parser := marshal.NewParser()
	encoder := marshal.NewEncoder()

	router := network.NewRequestRouter(handlers, parser, encoder)

	listener := network.NewTCPListener("6379", router)

	err := listener.StartListen()
	if err != nil {
		log.WithError(err).Errorln("Failed to start listening")
	}
}
