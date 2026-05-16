package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	RPCURL       string        `yaml:"rpc_url"`
	Contract     string        `yaml:"contract"`
	Topics       []string      `yaml:"topics"`
	StartBlock   uint64        `yaml:"start_block"`
	ChunkSize    uint64        `yaml:"chunk_size"`
	PollInterval time.Duration `yaml:"poll_interval"`
	DBPath       string        `yaml:"db_path"`
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
	return nil
}
