package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	AppID string `mapstructure:"app_id"`
	Env   string `mapstructure:"app_env"`

	KafkaBrokers        []string `mapstructure:"kafka_brokers"`
	OrchestratorGroupID string   `mapstructure:"kafka_group_orchestrator"`

	TopicExtractRequest   string `mapstructure:"topic_extract_request"`
	TopicExtractCompleted string `mapstructure:"topic_extract_completed"`
	TopicPrepareRequest   string `mapstructure:"topic_prepare_request"`
	TopicPrepareCompleted string `mapstructure:"topic_prepare_completed"`
	TopicPipelineFailed   string `mapstructure:"topic_pipeline_failed"`
	TopicStateChanged     string `mapstructure:"topic_state_changed"`

	HTTPAddr    string `mapstructure:"http_addr"`
	PostgresURL string `mapstructure:"postgres_url"`

	CategorizeEnabled bool `mapstructure:"categorize_enabled"`
}

func Load() Config {
	v := viper.New()

	// Read config file
	v.SetConfigName("config")
	v.SetConfigType("toml")
	v.AddConfigPath(".")

	if err := v.ReadInConfig(); err != nil {
		panic(fmt.Sprintf("failed to read config.toml: %v", err))
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		panic(fmt.Sprintf("failed to unmarshal config: %v", err))
	}

	return cfg
}
