package config

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// baseValidSource returns a fully-populated Source good enough to pass
// validate(). Tests mutate one field at a time to exercise rejection paths.
func baseValidSource(name string) Source {
	s := Source{
		Name:     name,
		RPCURL:   "http://localhost:8545",
		Contract: "0xabc",
	}
	s.applyDefaults()
	return s
}

// baseValidConfig returns a single-source Config that passes validate().
func baseValidConfig() Config {
	c := Config{
		DBPath:  "./mull.db",
		Sources: []Source{baseValidSource("default")},
	}
	c.applyDefaults()
	return c
}

func TestValidateReorgDepth(t *testing.T) {
	cases := []struct {
		name    string
		depth   uint64
		wantErr string
	}{
		{"default 64 ok", 64, ""},
		{"zero rejected pre-defaults", 0, "reorg_depth must be between 1 and 1024"},
		{"min 1 ok", 1, ""},
		{"max 1024 ok", 1024, ""},
		{"above max rejected", 1025, "reorg_depth must be between 1 and 1024"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseValidConfig()
			c.ReorgDepth = tc.depth
			err := c.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate: %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validate: %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidate_AbiPath(t *testing.T) {
	cases := []struct {
		name    string
		abiPath string
	}{
		{"unset ok", ""},
		{"non-empty ok (shape only — IO happens in cmd/codegen)", "./some/path.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseValidConfig()
			c.Sources[0].AbiPath = tc.abiPath
			if err := c.validate(); err != nil {
				t.Fatalf("validate: %v, want nil", err)
			}
		})
	}
}

func TestApplyDefaultsReorgDepth(t *testing.T) {
	c := Config{}
	c.applyDefaults()
	if c.ReorgDepth != 64 {
		t.Fatalf("default reorg_depth = %d, want 64", c.ReorgDepth)
	}
}

// writeTempConfig writes contents to a unique file under t.TempDir and returns
// the path — saves the call-site boilerplate in every Load test below.
func writeTempConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mull.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func TestLoadMultiSource(t *testing.T) {
	body := `
db_path: ./mull.db
sources:
  - name: usdc_mainnet
    rpc_url: https://mainnet.example
    contract: "0xUSDC"
    topics: ["0xt0"]
    start_block: 100
  - name: usdc_arb
    rpc_url: https://arb.example
    contract: "0xUSDCarb"
    topics: ["0xt0"]
    start_block: 50
    chunk_size: 500
`
	path := writeTempConfig(t, body)
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.Sources) != 2 {
		t.Fatalf("len sources = %d, want 2", len(c.Sources))
	}
	if c.Sources[0].Name != "usdc_mainnet" || c.Sources[1].Name != "usdc_arb" {
		t.Fatalf("source names = %q,%q", c.Sources[0].Name, c.Sources[1].Name)
	}
	if c.Sources[0].ChunkSize != 1000 {
		t.Fatalf("source[0].ChunkSize = %d, want default 1000", c.Sources[0].ChunkSize)
	}
	if c.Sources[1].ChunkSize != 500 {
		t.Fatalf("source[1].ChunkSize = %d, want 500 (explicit)", c.Sources[1].ChunkSize)
	}
	if c.Sources[0].HeadSource != "auto" {
		t.Fatalf("source[0].HeadSource = %q, want default auto", c.Sources[0].HeadSource)
	}
}

func TestLoadLegacyShim(t *testing.T) {
	// Capture the shim logger so we can assert it emitted exactly once.
	var buf bytes.Buffer
	prev := LegacyShimLogger
	t.Cleanup(func() { LegacyShimLogger = prev })
	LegacyShimLogger = func() *slog.Logger {
		return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	ResetEmitOnceForTest()

	body := `
db_path: ./mull.db
rpc_url: https://mainnet.example
contract: "0xC"
topics: ["0xt0"]
start_block: 1
`
	path := writeTempConfig(t, body)
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.Sources) != 1 {
		t.Fatalf("len sources = %d, want 1 (shim)", len(c.Sources))
	}
	if c.Sources[0].Name != "default" {
		t.Fatalf("source[0].Name = %q, want default", c.Sources[0].Name)
	}
	if c.Sources[0].RPCURL != "https://mainnet.example" || c.Sources[0].Contract != "0xC" {
		t.Fatalf("legacy fields not carried into shim source: %+v", c.Sources[0])
	}
	if !strings.Contains(buf.String(), "legacy single-source config detected") {
		t.Fatalf("shim INFO log missing; got: %q", buf.String())
	}
}

func TestValidateRejectsMixedShape(t *testing.T) {
	body := `
db_path: ./mull.db
rpc_url: https://mainnet.example
contract: "0xC"
sources:
  - name: s
    rpc_url: https://other.example
    contract: "0xC"
`
	path := writeTempConfig(t, body)
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load: want error, got nil")
	}
	if !strings.Contains(err.Error(), "both `sources:` and legacy") {
		t.Fatalf("err = %v, want 'both `sources:` and legacy …'", err)
	}
}

func TestValidateRejectsDuplicateSourceNames(t *testing.T) {
	c := Config{
		DBPath: "./mull.db",
		Sources: []Source{
			baseValidSource("main"),
			baseValidSource("main"),
		},
	}
	c.applyDefaults()
	err := c.validate()
	if err == nil {
		t.Fatal("validate: want duplicate-name error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate source name") {
		t.Fatalf("err = %v, want 'duplicate source name'", err)
	}
}

func TestValidateRejectsInvalidSourceName(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
	}{
		{"a", true},
		{"a_b-c0", true},
		{"Main", false},
		{"a@b", false},
		{"", false},
		{strings.Repeat("a", 65), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{
				DBPath:  "./mull.db",
				Sources: []Source{baseValidSource(tc.name)},
			}
			c.applyDefaults()
			err := c.validate()
			if tc.ok && err != nil {
				t.Fatalf("validate(%q): %v, want ok", tc.name, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("validate(%q): nil, want error", tc.name)
			}
		})
	}
}

func TestValidateRejectsEmptySources(t *testing.T) {
	c := Config{DBPath: "./mull.db"}
	c.applyDefaults()
	err := c.validate()
	if err == nil {
		t.Fatal("validate: want error, got nil")
	}
	if !strings.Contains(err.Error(), "sources: is required") {
		t.Fatalf("err = %v, want 'sources: is required'", err)
	}
}

func TestValidateWarnsOnHighRPCPressure(t *testing.T) {
	var buf bytes.Buffer
	prev := RPCPressureWarnLogger
	t.Cleanup(func() { RPCPressureWarnLogger = prev })
	RPCPressureWarnLogger = func() *slog.Logger {
		return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	ResetEmitOnceForTest()

	c := Config{
		DBPath:      "./mull.db",
		Concurrency: 5,
		Sources: []Source{
			baseValidSource("a"), baseValidSource("b"), baseValidSource("c"),
			baseValidSource("d"),
		},
	}
	c.applyDefaults()
	c.Concurrency = 5 // restore after applyDefaults clamped it to default 1
	if err := c.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "high aggregate RPC pressure") {
		t.Fatalf("expected WARN log; got %q", out)
	}
	if !strings.Contains(out, "sources=4") || !strings.Contains(out, "concurrency=5") {
		t.Fatalf("WARN log missing source-count / concurrency fields: %q", out)
	}
}

// TestValidateWarnsOnHighRPCPressure_Once asserts the WARN log is gated by
// sync.Once across multiple validate() calls in the same process — pinned so
// a future refactor that moves the emission out of the gated block surfaces.
func TestValidateWarnsOnHighRPCPressure_Once(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	prev := RPCPressureWarnLogger
	t.Cleanup(func() { RPCPressureWarnLogger = prev })
	RPCPressureWarnLogger = func() *slog.Logger {
		mu.Lock()
		defer mu.Unlock()
		return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	ResetEmitOnceForTest()

	c := Config{
		DBPath:      "./mull.db",
		Concurrency: 5,
		Sources: []Source{
			baseValidSource("a"), baseValidSource("b"), baseValidSource("c"),
			baseValidSource("d"),
		},
	}
	c.applyDefaults()
	c.Concurrency = 5
	for i := 0; i < 3; i++ {
		if err := c.validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
	}
	count := strings.Count(buf.String(), "high aggregate RPC pressure")
	if count != 1 {
		t.Fatalf("WARN emitted %d times, want exactly 1", count)
	}
}
