package config

import (
	"sync"

	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	MaxConnections        int `default:"1024"`
	CommandBufferCapacity int `default:"1024"`
	Port                  int `default:"6379"`
}

var once sync.Once
var config Config

func GetConfig() Config {
	once.Do(func() {
		var c Config
		err := envconfig.Process("sashredis", &c)
		if err != nil {
			log.WithError(err).Fatal("error reading process variables")
		}
		config = c
	})
	return config
}
