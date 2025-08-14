package config

import (
	"fmt"

	"github.com/quiby-ai/common/pkg/events"
	"github.com/spf13/viper"
)

type Config struct {
	AppID string `mapstructure:"app_id"`

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
}

func Load() Config {
	v := viper.New()

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

	cfg.TopicExtractRequest = events.PipelineExtractRequest
	cfg.TopicExtractCompleted = events.PipelineExtractCompleted
	cfg.TopicPrepareRequest = events.PipelinePrepareRequest
	cfg.TopicPrepareCompleted = events.PipelinePrepareCompleted
	cfg.TopicPipelineFailed = events.PipelineFailed
	cfg.TopicStateChanged = events.SagaStateChanged

	return cfg
}
