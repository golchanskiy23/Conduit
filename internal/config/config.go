package config

import (
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/viper"
)

const(
	defaultAddr = 8080
	defaultReadTimeout = 5
	defaultWriteTimeout = 10
	defaultIdleTimeout = 60
	defaultShutdownTimeout = 15
	defaultExpiredTimeShutdown = 5
)

type Config struct{
	Server HTTPServer `mapstructure:"server"`
	PoolCfg WorkerPoolConfig `mapstructure:"poolcfg"`
	TTLMap TTLMapConfig `mapstructure:"ttlmap"`
}

type HTTPServer struct{
	Addr string `mapstructure:"address"`
	ReadTimeout time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	IdleTimeout time.Duration `mapstructure:"idle_timeout"`
  	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

type WorkerPoolConfig struct {
	Worker string `mapstructure:"worker"`
	WorkersNum int `mapstructure:"workers_num"`
	BufferSize int `mapstructure:"buffer_size"`
	JobTimeout time.Duration `mapstructure:"job_timeout"`
}

type TTLMapConfig struct{
	ExpiredTimeShutdown time.Duration `mapstructure:"expired_time"`
}

func DefaultConfig() *Config {
	return &Config{
		Server: HTTPServer{
			Addr:            ":"+strconv.Itoa(defaultAddr),
			ReadTimeout:     defaultReadTimeout * time.Second,
			WriteTimeout:    defaultWriteTimeout * time.Second,
			IdleTimeout:     defaultIdleTimeout * time.Second,
			ShutdownTimeout: defaultShutdownTimeout * time.Second,
		},
		TTLMap: TTLMapConfig{
			ExpiredTimeShutdown: defaultExpiredTimeShutdown*time.Minute,
		},
	}
}

func NewConfig() (*Config, error){
	cfg := DefaultConfig()

	v := viper.New()

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./configs")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("fatal error config file: %w", err)
	}

	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("marshaling error: %w", err)
	}
	return cfg, nil
}

