package config

import (
	"log"
	"time"

	"github.com/spf13/viper"
)

type KafkaConfig struct {
	Brokers  []string
	Group    string
	MaxWait  time.Duration
	MinBytes int
	MaxBytes int
}

type AppConfig struct {
	ID string
}

type DatabaseConfig struct {
	DSN string
}

type HTTPConfig struct {
	Addr            string
	ShutdownTimeout time.Duration
}

type Config struct {
	App      AppConfig
	Kafka    KafkaConfig
	Database DatabaseConfig
	HTTP     HTTPConfig
}

func Load() *Config {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")

	viper.AutomaticEnv()

	viper.BindEnv("app.id", "APP_ID")
	viper.BindEnv("kafka.brokers", "KAFKA_BROKERS")
	viper.BindEnv("kafka.group", "KAFKA_GROUP")
	viper.BindEnv("kafka.max_wait", "KAFKA_MAX_WAIT")
	viper.BindEnv("kafka.min_bytes", "KAFKA_MIN_BYTES")
	viper.BindEnv("kafka.max_bytes", "KAFKA_MAX_BYTES")
	viper.BindEnv("http.addr", "HTTP_ADDR")
	viper.BindEnv("http.shutdown_timeout", "HTTP_SHUTDOWN_TIMEOUT")

	viper.BindEnv("PG_DSN")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Printf("No config.toml file found, using environment variables and defaults")
		} else {
			log.Printf("Error reading config file: %v", err)
		}
	}

	config := &Config{
		App: AppConfig{
			ID: viper.GetString("app.id"),
		},
		Kafka: KafkaConfig{
			Brokers:  viper.GetStringSlice("kafka.brokers"),
			Group:    viper.GetString("kafka.group"),
			MaxWait:  viper.GetDuration("kafka.max_wait"),
			MinBytes: viper.GetInt("kafka.min_bytes"),
			MaxBytes: viper.GetInt("kafka.max_bytes"),
		},
		Database: DatabaseConfig{
			DSN: viper.GetString("PG_DSN"),
		},
		HTTP: HTTPConfig{
			Addr:            viper.GetString("http.addr"),
			ShutdownTimeout: viper.GetDuration("http.shutdown_timeout"),
		},
	}

	return config
}
