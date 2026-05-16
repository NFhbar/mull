package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// newLogger builds a *slog.Logger from the --log-level and --log-format
// flags. AddSource is enabled at debug level since file:line is mainly
// useful when chasing bugs.
func newLogger(level, format string) (*slog.Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{
		Level:     lvl,
		AddSource: lvl <= slog.LevelDebug,
	}
	handler, err := buildHandler(format, opts)
	if err != nil {
		return nil, err
	}
	return slog.New(handler), nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q (want debug|info|warn|error)", s)
	}
}

func buildHandler(format string, opts *slog.HandlerOptions) (slog.Handler, error) {
	switch strings.ToLower(format) {
	case "text":
		return slog.NewTextHandler(os.Stderr, opts), nil
	case "json":
		return slog.NewJSONHandler(os.Stderr, opts), nil
	default:
		return nil, fmt.Errorf("invalid log format %q (want text|json)", format)
	}
}
