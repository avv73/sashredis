package main

import (
	"context"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/handler"
	"github.com/codecrafters-io/redis-starter-go/app/marshal"
	"github.com/codecrafters-io/redis-starter-go/app/network"
	"github.com/codecrafters-io/redis-starter-go/app/processor"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

func main() {
	log.Println("===SASHREDIS===")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	pingHandler := handler.NewPingHandler()
	echoHandler := handler.NewEchoHandler()

	handlers := map[types.CommandName]processor.CommandHandler{
		types.Ping: pingHandler,
		types.Echo: echoHandler,
	}

	parser := marshal.NewParser()
	encoder := marshal.NewEncoder()

	bus := processor.NewEventBus()
	processor := processor.NewProcessor(bus, handlers)

	router := network.NewRequestRouter(bus, parser, encoder)
	listener := network.NewTCPListener("6379", router)

	processor.Start(ctx)
	err := listener.StartListen(ctx)
	if err != nil {
		log.WithError(err).Errorln("Failed to start listening")
	}

	<-ctx.Done()
	log.Println("===SASHREDIS GOING DOWN===")
}
