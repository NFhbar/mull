package config

import (
	"strings"
	"testing"
)

func baseValid() Config {
	c := Config{
		RPCURL:   "http://localhost:8545",
		Contract: "0xabc",
		DBPath:   "./mull.db",
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
			c := baseValid()
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
			c := baseValid()
			c.AbiPath = tc.abiPath
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
