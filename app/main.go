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
	"github.com/codecrafters-io/redis-starter-go/app/replica"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
	"github.com/codecrafters-io/redis-starter-go/app/types"
)

func main() {
	log.Println("===SASHREDIS===")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	dataStorage := storage.NewStorage()
	serverInfoStore := storage.NewServerInfoStore()
	bus := processor.NewEventBus()
	transactionMgr := processor.NewTransactionManager()

	pingHandler := handler.NewPingHandler()
	echoHandler := handler.NewEchoHandler()
	getHandler := handler.NewGetHandler(dataStorage)
	setHandler := handler.NewSetHandler(dataStorage)
	rpushHandler := handler.NewRpushHandler(dataStorage)
	lrangeHandler := handler.NewLrangeHandler(dataStorage)
	lpushHandler := handler.NewLpushHandler(dataStorage)
	llenHandler := handler.NewLlenHandler(dataStorage)
	lpopHandler := handler.NewLpopHandler(dataStorage)
	blpopHandler := handler.NewBlpopHandler(dataStorage, bus)
	typeHandler := handler.NewTypeHandler(dataStorage)
	xaddHandler := handler.NewXaddHandler(dataStorage)
	xrangeHandler := handler.NewXrangeHandler(dataStorage)
	xreadHandler := handler.NewXreadHandler(dataStorage, bus)
	incrHandler := handler.NewIncrHandler(dataStorage)
	multiHandler := handler.NewMultiHandler(transactionMgr)
	execHandler := handler.NewExecHandler(transactionMgr)
	discardHandler := handler.NewDiscardHandler(transactionMgr)
	infoHandler := handler.NewInfoStorage(serverInfoStore)

	handlers := map[types.CommandName]processor.CommandHandler{
		types.Ping:    pingHandler,
		types.Echo:    echoHandler,
		types.Get:     getHandler,
		types.Set:     setHandler,
		types.Rpush:   rpushHandler,
		types.Lrange:  lrangeHandler,
		types.Lpush:   lpushHandler,
		types.Llen:    llenHandler,
		types.Lpop:    lpopHandler,
		types.Blpop:   blpopHandler,
		types.Type:    typeHandler,
		types.Xadd:    xaddHandler,
		types.Xrange:  xrangeHandler,
		types.Xread:   xreadHandler,
		types.Incr:    incrHandler,
		types.Multi:   multiHandler,
		types.Exec:    execHandler,
		types.Discard: discardHandler,
		types.Info:    infoHandler,
	}

	parser := marshal.NewParser()
	encoder := marshal.NewEncoder()

	processor := processor.NewProcessor(bus, handlers, transactionMgr)
	transactionMgr.RegisterExecutor(processor)

	router := network.NewRequestRouter(bus, parser, encoder)
	listener := network.NewTCPListener(config.GetConfig().Port, router)

	replicationMgr := replica.NewManager(serverInfoStore)

	replicationMgr.Initialize()
	processor.Start(ctx)
	err := listener.StartListen(ctx)
	if err != nil {
		log.WithError(err).Errorln("Failed to start listening")
	}

	<-ctx.Done()
	log.Println("===SASHREDIS GOING DOWN===")
}
