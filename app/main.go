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
	bus := processor.NewEventBus()
	transactionMgr := processor.NewTransactionManager()

	pingHandler := handler.NewPingHandler()
	echoHandler := handler.NewEchoHandler()
	getHandler := handler.NewGetHandler(storage)
	setHandler := handler.NewSetHandler(storage)
	rpushHandler := handler.NewRpushHandler(storage)
	lrangeHandler := handler.NewLrangeHandler(storage)
	lpushHandler := handler.NewLpushHandler(storage)
	llenHandler := handler.NewLlenHandler(storage)
	lpopHandler := handler.NewLpopHandler(storage)
	blpopHandler := handler.NewBlpopHandler(storage, bus)
	typeHandler := handler.NewTypeHandler(storage)
	xaddHandler := handler.NewXaddHandler(storage)
	xrangeHandler := handler.NewXrangeHandler(storage)
	xreadHandler := handler.NewXreadHandler(storage, bus)
	incrHandler := handler.NewIncrHandler(storage)
	multiHandler := handler.NewMultiHandler(transactionMgr)
	execHandler := handler.NewExecHandler(transactionMgr)

	handlers := map[types.CommandName]processor.CommandHandler{
		types.Ping:   pingHandler,
		types.Echo:   echoHandler,
		types.Get:    getHandler,
		types.Set:    setHandler,
		types.Rpush:  rpushHandler,
		types.Lrange: lrangeHandler,
		types.Lpush:  lpushHandler,
		types.Llen:   llenHandler,
		types.Lpop:   lpopHandler,
		types.Blpop:  blpopHandler,
		types.Type:   typeHandler,
		types.Xadd:   xaddHandler,
		types.Xrange: xrangeHandler,
		types.Xread:  xreadHandler,
		types.Incr:   incrHandler,
		types.Multi:  multiHandler,
		types.Exec:   execHandler,
	}

	parser := marshal.NewParser()
	encoder := marshal.NewEncoder()

	processor := processor.NewProcessor(bus, handlers, transactionMgr)

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
