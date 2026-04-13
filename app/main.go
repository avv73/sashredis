package main

import (
	"context"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/handler"
	"github.com/codecrafters-io/redis-starter-go/app/marshal"
	"github.com/codecrafters-io/redis-starter-go/app/network"
	"github.com/codecrafters-io/redis-starter-go/app/processor"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

func main() {
	log.Println("===SASHREDIS===")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	storage := storage.NewStorage()

	pingHandler := handler.NewPingHandler()
	echoHandler := handler.NewEchoHandler()
	getHandler := handler.NewGetHandler(storage)
	setHandler := handler.NewSetHandler(storage)

	handlers := map[types.CommandName]processor.CommandHandler{
		types.Ping: pingHandler,
		types.Echo: echoHandler,
		types.Get:  getHandler,
		types.Set:  setHandler,
	}

	parser := marshal.NewParser()
	encoder := marshal.NewEncoder()

	bus := processor.NewEventBus()
	processor := processor.NewProcessor(bus, handlers)

	router := network.NewRequestRouter(bus, parser, encoder)
	listener := network.NewTCPListener(config.GetConfig().Port, router)

	processor.Start(ctx)
	err := listener.StartListen(ctx)
	if err != nil {
		log.WithError(err).Errorln("Failed to start listening")
	}

	<-ctx.Done()
	log.Println("===SASHREDIS GOING DOWN===")
}
