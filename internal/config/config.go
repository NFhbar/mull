package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/NFhbar/mull/internal/rpc"
)

type Config struct {
	RPCURL              string        `yaml:"rpc_url"`
	Contract            string        `yaml:"contract"`
	Topics              []string      `yaml:"topics"`
	StartBlock          uint64        `yaml:"start_block"`
	ChunkSize           uint64        `yaml:"chunk_size"`
	PollInterval        time.Duration `yaml:"poll_interval"`
	DBPath              string        `yaml:"db_path"`
	RPCRetryBase        time.Duration `yaml:"rpc_retry_base"`
	RPCRetryMaxDelay    time.Duration `yaml:"rpc_retry_max_delay"`
	RPCRetryMaxAttempts int           `yaml:"rpc_retry_max_attempts"`
	Concurrency         int           `yaml:"concurrency"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.ChunkSize == 0 {
		c.ChunkSize = 1000
	}
	if c.PollInterval == 0 {
		c.PollInterval = 5 * time.Second
	}
	d := rpc.DefaultRetryPolicy()
	if c.RPCRetryBase == 0 {
		c.RPCRetryBase = d.Base
	}
	if c.RPCRetryMaxDelay == 0 {
		c.RPCRetryMaxDelay = d.MaxDelay
	}
	if c.RPCRetryMaxAttempts == 0 {
		c.RPCRetryMaxAttempts = d.MaxAttempts
	}
	if c.Concurrency == 0 {
		c.Concurrency = 1
	}
}

func (c *Config) validate() error {
	if c.RPCURL == "" {
		return fmt.Errorf("rpc_url is required")
	}
	if c.Contract == "" {
		return fmt.Errorf("contract is required")
	}
	if c.DBPath == "" {
		return fmt.Errorf("db_path is required")
	}
	if c.RPCRetryBase < 0 {
		return fmt.Errorf("rpc_retry_base must be >= 0")
	}
	if c.RPCRetryMaxDelay < 0 {
		return fmt.Errorf("rpc_retry_max_delay must be >= 0")
	}
	if c.RPCRetryMaxAttempts < 1 {
		return fmt.Errorf("rpc_retry_max_attempts must be >= 1")
	}
	if c.RPCRetryMaxAttempts > 20 {
		return fmt.Errorf("rpc_retry_max_attempts must be <= 20")
	}
	if c.Concurrency < 1 || c.Concurrency > 8 {
		return fmt.Errorf("concurrency must be between 1 and 8")
	}
	return nil
}
