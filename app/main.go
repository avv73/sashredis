package main

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/handler"
	"github.com/codecrafters-io/redis-starter-go/app/network"
)

func main() {
	fmt.Println("===SASHKO REDIS===")

	pingHandler := handler.NewPingHandler()

	router := network.NewRequestRouter(map[command.CommandName]network.CommandHandler{
		command.Ping: pingHandler,
	})

	listener := network.NewTCPListener("6379", router)

	err := listener.StartListen()
	if err != nil {
		log.WithError(err).Errorln("Failed to start listening")
	}
}
