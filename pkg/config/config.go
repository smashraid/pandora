package config

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type DatabaseConfig struct {
	DriverName      string        `envconfig:"DB_DRIVER" default:"postgres"`
	DatabaseURL     string        `envconfig:"DATABASE_URL" default:"postgres://postgres:postgres@localhost:5432/pandora?sslmode=disable"`
	MaxOpenConns    int           `envconfig:"DB_MAX_OPEN_CONNS" default:"25"`
	MaxIdleConns    int           `envconfig:"DB_MAX_IDLE_CONNS" default:"5"`
	ConnMaxLifetime time.Duration `envconfig:"DB_CONN_MAX_LIFETIME" default:"15m"`
}

type ValkeyConfig struct {
	Addr         string        `envconfig:"VALKEY_ADDR" default:"localhost:6379"`
	Username     string        `envconfig:"VALKEY_USERNAME" default:""`
	Password     string        `envconfig:"VALKEY_PASSWORD" default:""`
	DB           int           `envconfig:"VALKEY_DB" default:"0"`
	DialTimeout  time.Duration `envconfig:"VALKEY_DIAL_TIMEOUT" default:"5s"`
	ReadTimeout  time.Duration `envconfig:"VALKEY_READ_TIMEOUT" default:"3s"`
	WriteTimeout time.Duration `envconfig:"VALKEY_WRITE_TIMEOUT" default:"3s"`
	PoolSize     int           `envconfig:"VALKEY_POOL_SIZE" default:"10"`
}

type Config struct {
	Env      string `envconfig:"ENV" default:"development"`
	Database DatabaseConfig
	Valkey   ValkeyConfig
}

func LoadConfig() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, fmt.Errorf("failed to process configuration: %w", err)
	}
	return &cfg, nil
}
