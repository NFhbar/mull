package cmd

import (
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in      string
		want    slog.Level
		wantErr bool
	}{
		{"debug", slog.LevelDebug, false},
		{"INFO", slog.LevelInfo, false},
		{"Warn", slog.LevelWarn, false},
		{"warning", slog.LevelWarn, false},
		{"error", slog.LevelError, false},
		{"trace", 0, true},
		{"", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseLevel(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Fatalf("level = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNewLogger(t *testing.T) {
	if _, err := newLogger("info", "text"); err != nil {
		t.Fatalf("text/info: %v", err)
	}
	if _, err := newLogger("debug", "json"); err != nil {
		t.Fatalf("json/debug: %v", err)
	}
	_, err := newLogger("info", "xml")
	if err == nil || !strings.Contains(err.Error(), "invalid log format") {
		t.Fatalf("bad format err = %v", err)
	}
	_, err = newLogger("verbose", "text")
	if err == nil || !strings.Contains(err.Error(), "invalid log level") {
		t.Fatalf("bad level err = %v", err)
	}
}
