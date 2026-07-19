package config

import (
	"flag"
	"sync"

	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	MaxConnections        int `default:"1024"`
	CommandBufferCapacity int `default:"1024"`
	Port                  int `default:"6379"`
	ReplicaOf             string
}

var once sync.Once
var config Config

func GetConfig() Config {
	once.Do(func() {
		// get env variables first
		var c Config
		err := envconfig.Process("sashredis", &c)
		if err != nil {
			log.WithError(err).Fatal("error reading process variables")
		}
		config = c

		// override any env variables with CLI
		portCli := flag.Int("port", 6379, "Port to listen to.")
		replicaOfCli := flag.String("replicaof", "", "Replication information for the master; Sashredis assumes replica role.")
		flag.Parse()

		if portCli != nil {
			config.Port = *portCli
		}

		if replicaOfCli != nil {
			config.ReplicaOf = *replicaOfCli
		}
	})
	return config
}
