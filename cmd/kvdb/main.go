package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/jcalabro/kvdb/internal/server"
	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "kvdb",
		Usage: "Key-value database",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "log-lvl",
				Value: "info",
				Usage: "Log level",
			},
			&cli.StringFlag{
				Name:  "log-fmt",
				Value: "json",
				Usage: "Log format type (default, json)",
			},
			&cli.BoolFlag{
				Name:  "log-src",
				Value: true,
				Usage: "Include source code line numbers in logs",
			},
		},
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			if err := setDefaultLogger(
				c.String("log-lvl"),
				c.String("log-fmt"),
				c.Bool("log-src"),
			); err != nil {
				return nil, fmt.Errorf("unable to set default logger: %w", err)
			}
			return ctx, nil
		},
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "run the database server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "metrics-addr",
						Value: "0.0.0.0:6060",
						Usage: `bind address of the metrics and pprof server (use "" to disable)`,
					},
					&cli.StringFlag{
						Name:  "redis-addr",
						Value: "0.0.0.0:6379",
						Usage: "bind address of the redis front end",
					},
					&cli.StringFlag{
						Name:  "fdb-cluster-file",
						Value: "foundation.cluster",
						Usage: "path to the foundationdb cluster file for the client",
					},
					&cli.IntFlag{
						Name:  "fdb-api-version",
						Value: 730,
						Usage: "foundationdb api version",
					},
					&cli.Int64Flag{
						Name:  "fdb-transaction-timeout-millis",
						Value: 5000,
						Usage: "max timeout per transaction",
					},
					&cli.Int64Flag{
						Name:  "fdb-transaction-retry-limit",
						Value: 100,
						Usage: "max number of retries per aborted transaction",
					},
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					args := &server.Args{
						MetricsAddr: c.String("metrics-addr"),
						RedisAddr:   c.String("redis-addr"),

						FDBClusterFile:           c.String("fdb-cluster-file"),
						FDBAPIVersion:            c.Int("fdb-api-version"),
						FDBTransactionTimeout:    c.Int64("fdb-transaction-timeout-millis"),
						FDBTransactionRetryLimit: c.Int64("fdb-transaction-retry-limit"),
					}

					if err := server.Run(ctx, args); err != nil {
						return fmt.Errorf("failed to run server: %w", err)
					}

					return nil
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		slog.Error("failed to run command", "err", err)
		os.Exit(1)
	}
}

func setDefaultLogger(llevel, lfmt string, addSource bool) error {
	opts := &slog.HandlerOptions{
		AddSource: addSource,
	}

	switch llevel {
	case "d", "dbg", "debug":
		opts.Level = slog.LevelDebug
	case "i", "info":
		opts.Level = slog.LevelInfo
	case "w", "warn", "warning":
		opts.Level = slog.LevelWarn
	case "e", "err", "error":
		opts.Level = slog.LevelError
	}

	var log *slog.Logger
	switch strings.ToLower(lfmt) {
	case "default":
		log = slog.New(slog.NewTextHandler(os.Stdout, opts))
	case "json":
		log = slog.New(slog.NewJSONHandler(os.Stdout, opts))
	default:
		return fmt.Errorf(`unsupported log format: %s (wanted "default" or "json")`, lfmt)
	}

	slog.SetDefault(slog.New(log.Handler()))
	return nil
}
