package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/NFhbar/mull/internal/rpc"
)

// Source is one indexing target — a (chain, contract, topics) tuple with its
// own RPC endpoints and start block. Multi-source configs list these under
// `sources:`; a single-source legacy config is wrapped as a synthetic Source
// named "default" by Load.
type Source struct {
	Name                    string        `yaml:"name"`
	RPCURL                  string        `yaml:"rpc_url"`
	WSRPCURL                string        `yaml:"ws_rpc_url"`
	Contract                string        `yaml:"contract"`
	Topics                  []string      `yaml:"topics"`
	StartBlock              uint64        `yaml:"start_block"`
	ChunkSize               uint64        `yaml:"chunk_size"`
	AbiPath                 string        `yaml:"abi_path"`
	HeadSource              string        `yaml:"head_source"`
	HeadSourceFallbackAfter time.Duration `yaml:"head_source_fallback_after"`
}

type Config struct {
	// Sources is the canonical multi-source list. When empty, Load synthesises
	// one Source from the legacy top-level fields (rpc_url, contract, …).
	Sources []Source `yaml:"sources"`

	// Process-global fields. Concurrency is a single global knob; each source
	// gets its own worker pool sized to that value (research deferred per-source
	// concurrency overrides to a follow-up). The aggregate in-flight RPC ceiling
	// is therefore len(Sources) * Concurrency — surfaced by the boot-time WARN
	// when that product exceeds 16.
	DBPath              string        `yaml:"db_path"`
	PollInterval        time.Duration `yaml:"poll_interval"`
	RPCRetryBase        time.Duration `yaml:"rpc_retry_base"`
	RPCRetryMaxDelay    time.Duration `yaml:"rpc_retry_max_delay"`
	RPCRetryMaxAttempts int           `yaml:"rpc_retry_max_attempts"`
	Concurrency         int           `yaml:"concurrency"`
	ReorgDepth          uint64        `yaml:"reorg_depth"`

	// Legacy top-level fields. Tolerated only for the single-source shim; Load
	// folds them into a Source{Name: "default"} when Sources is empty. Setting
	// any of these alongside Sources is a validation error (mixed shape).
	RPCURL                  string        `yaml:"rpc_url"`
	WSRPCURL                string        `yaml:"ws_rpc_url"`
	Contract                string        `yaml:"contract"`
	Topics                  []string      `yaml:"topics"`
	StartBlock              uint64        `yaml:"start_block"`
	ChunkSize               uint64        `yaml:"chunk_size"`
	AbiPath                 string        `yaml:"abi_path"`
	HeadSource              string        `yaml:"head_source"`
	HeadSourceFallbackAfter time.Duration `yaml:"head_source_fallback_after"`
}

// LegacyShimLogger is the destination for the one-time "legacy single-source
// config detected" notice emitted by Load when the shim fires. Defaults to
// slog.Default(); tests inject a captured logger to assert emission.
var LegacyShimLogger = func() *slog.Logger { return slog.Default() }

// RPCPressureWarnLogger is the destination for the one-time WARN emitted by
// validate when len(Sources) * Concurrency > 16. Same indirection as
// LegacyShimLogger — tests inject a captured logger.
var RPCPressureWarnLogger = func() *slog.Logger { return slog.Default() }

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := c.applyShim(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &c, nil
}

// hasLegacyTopLevel reports whether any of the legacy single-source fields are
// set. Used by applyShim + validate to detect mixed-shape configs.
func (c *Config) hasLegacyTopLevel() bool {
	return c.RPCURL != "" ||
		c.WSRPCURL != "" ||
		c.Contract != "" ||
		len(c.Topics) > 0 ||
		c.StartBlock != 0 ||
		c.ChunkSize != 0 ||
		c.AbiPath != "" ||
		c.HeadSource != "" ||
		c.HeadSourceFallbackAfter != 0
}

// applyShim folds legacy top-level fields into a synthetic default Source when
// sources: is absent. Mixed shape (both sources: and legacy) is rejected here
// so validate sees a single consistent representation.
func (c *Config) applyShim() error {
	if len(c.Sources) > 0 && c.hasLegacyTopLevel() {
		return fmt.Errorf("config has both `sources:` and legacy top-level fields (rpc_url/contract/etc); pick one shape — see MIGRATION.md")
	}
	if len(c.Sources) > 0 {
		return nil
	}
	if !c.hasLegacyTopLevel() {
		return nil
	}
	shimEmitOnce.Do(func() {
		LegacyShimLogger().Info(`legacy single-source config detected; wrapped as source name="default". See MIGRATION.md for the multi-source schema.`)
	})
	c.Sources = []Source{{
		Name:                    "default",
		RPCURL:                  c.RPCURL,
		WSRPCURL:                c.WSRPCURL,
		Contract:                c.Contract,
		Topics:                  c.Topics,
		StartBlock:              c.StartBlock,
		ChunkSize:               c.ChunkSize,
		AbiPath:                 c.AbiPath,
		HeadSource:              c.HeadSource,
		HeadSourceFallbackAfter: c.HeadSourceFallbackAfter,
	}}
	c.RPCURL = ""
	c.WSRPCURL = ""
	c.Contract = ""
	c.Topics = nil
	c.StartBlock = 0
	c.ChunkSize = 0
	c.AbiPath = ""
	c.HeadSource = ""
	c.HeadSourceFallbackAfter = 0
	return nil
}

var (
	shimEmitOnce        sync.Once
	rpcPressureEmitOnce sync.Once
)

// ResetEmitOnceForTest re-arms the sync.Once gates the package uses to emit
// "wrapped legacy config" and "high RPC pressure" warnings exactly once per
// process. Exposed for tests that load multiple configs and want to observe
// each emission.
func ResetEmitOnceForTest() {
	shimEmitOnce = sync.Once{}
	rpcPressureEmitOnce = sync.Once{}
}

func (c *Config) applyDefaults() {
	d := rpc.DefaultRetryPolicy()
	if c.PollInterval == 0 {
		c.PollInterval = 5 * time.Second
	}
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
	if c.ReorgDepth == 0 {
		c.ReorgDepth = 64
	}
	for i := range c.Sources {
		c.Sources[i].applyDefaults()
	}
}

func (s *Source) applyDefaults() {
	if s.ChunkSize == 0 {
		s.ChunkSize = 1000
	}
	if s.HeadSource == "" {
		s.HeadSource = "auto"
	}
	if s.HeadSourceFallbackAfter == 0 {
		s.HeadSourceFallbackAfter = 30 * time.Second
	}
}

func (c *Config) validate() error {
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
	if c.ReorgDepth < 1 || c.ReorgDepth > 1024 {
		return fmt.Errorf("reorg_depth must be between 1 and 1024")
	}
	if len(c.Sources) == 0 {
		return fmt.Errorf("sources: is required (or set legacy top-level rpc_url/contract for the single-source shim)")
	}
	seen := make(map[string]int, len(c.Sources))
	for i, s := range c.Sources {
		if err := s.validate(); err != nil {
			return fmt.Errorf("sources[%d] (%q): %w", i, s.Name, err)
		}
		if prev, ok := seen[s.Name]; ok {
			return fmt.Errorf("duplicate source name %q (sources[%d] and sources[%d])", s.Name, prev, i)
		}
		seen[s.Name] = i
	}
	if int(len(c.Sources))*c.Concurrency > 16 {
		rpcPressureEmitOnce.Do(func() {
			RPCPressureWarnLogger().Warn(
				"high aggregate RPC pressure: len(sources) * concurrency exceeds 16; consider lowering concurrency",
				"sources", len(c.Sources),
				"concurrency", c.Concurrency,
				"product", len(c.Sources)*c.Concurrency,
			)
		})
	}
	return nil
}

func (s *Source) validate() error {
	if err := validateSourceName(s.Name); err != nil {
		return err
	}
	if s.RPCURL == "" {
		return fmt.Errorf("rpc_url is required")
	}
	if s.Contract == "" {
		return fmt.Errorf("contract is required")
	}
	switch s.HeadSource {
	case "ws", "poll", "auto":
	default:
		return fmt.Errorf("head_source must be one of ws|poll|auto (got %q)", s.HeadSource)
	}
	if s.WSRPCURL != "" {
		if !strings.HasPrefix(s.WSRPCURL, "ws://") && !strings.HasPrefix(s.WSRPCURL, "wss://") {
			return fmt.Errorf("ws_rpc_url must start with ws:// or wss://")
		}
	}
	if s.HeadSource == "ws" && s.WSRPCURL == "" {
		return fmt.Errorf("head_source: ws requires ws_rpc_url to be set")
	}
	if s.HeadSourceFallbackAfter < time.Second {
		return fmt.Errorf("head_source_fallback_after must be >= 1s")
	}
	return nil
}

// ValidateSourceName is the exported wrapper around validateSourceName so the
// HTTP boundary in internal/serve can reject `?source=` values that wouldn't
// have validated as a config-side source name — keeps one canonical rule.
func ValidateSourceName(name string) error { return validateSourceName(name) }

// validateSourceName enforces [a-z0-9_-]{1,64}. The narrow charset keeps names
// safe everywhere they end up: log structured fields, cursor payloads, table
// name segments (when a future feature uses them that way).
func validateSourceName(name string) error {
	if name == "" {
		return fmt.Errorf("source name is required")
	}
	if len(name) > 64 {
		return fmt.Errorf("source name %q exceeds 64 characters", name)
	}
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
		if !ok {
			return fmt.Errorf("source name %q has invalid character %q (allowed: [a-z0-9_-])", name, r)
		}
	}
	return nil
}
