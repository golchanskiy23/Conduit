package config

import(
	"time"
)

type WorkerPoolConfig struct {
	BufferSize int
	JobTimeout time.Duration
}


type Config struct{
	PoolCfg WorkerPoolConfig
	WorkersNum int
}

func NewConfig() *Config{
	return &Config{}
}

