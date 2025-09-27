package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/kvdb/internal/env"
	"github.com/bluesky-social/kvdb/internal/server/redis"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Args struct {
	MetricsAddr string

	FDBClusterFile           string
	FDBAPIVersion            int
	FDBTransactionTimeout    int64
	FDBTransactionRetryLimit int64

	RedisAddr      string
	RedisAdminUser string
	RedisAdminPass string
}

type server struct {
	log    *slog.Logger
	tracer trace.Tracer

	fdb       fdb.Database
	redisDirs *redis.Directories
}

func newServer(ctx context.Context, args *Args) (*server, error) {
	// initialize tracing
	if !env.IsProd() {
		otel.SetTracerProvider(noop.NewTracerProvider())
	} else {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create otlp exporter: %w", err)
		}
		tp := tracesdk.NewTracerProvider(
			tracesdk.WithBatcher(exp),
			tracesdk.WithResource(resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("kvdb"),
			)),
		)
		otel.SetTracerProvider(tp)
		tc := propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)
		otel.SetTextMapPropagator(tc)
	}

	s := &server{
		log:    slog.Default().With(slog.String("group", "server")),
		tracer: otel.Tracer("kvdb"),
	}

	err := fdb.APIVersion(730)
	if err != nil {
		return nil, fmt.Errorf("failed to set fdb client api version: %w", err)
	}

	s.fdb, err = fdb.OpenDatabase(args.FDBClusterFile)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize fdb client from cluster file %q: %w", args.FDBClusterFile, err)
	}

	// send a ping to ensure we are configured correctly
	_, err = s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(fdb.Key("PING")).Get()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to ping foundationdb: %w", err)
	}

	if err := s.fdb.Options().SetTransactionTimeout(args.FDBTransactionTimeout); err != nil {
		return nil, fmt.Errorf("failed to set fdb transaction timeout: %w", err)
	}
	if err := s.fdb.Options().SetTransactionRetryLimit(args.FDBTransactionRetryLimit); err != nil {
		return nil, fmt.Errorf("failed to set fdb transaction retry limit: %w", err)
	}

	s.redisDirs, err = redis.InitDirectories(s.fdb)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize redis directories: %w", err)
	}

	return s, nil
}

func Run(ctx context.Context, args *Args) error {
	s, err := newServer(ctx, args)
	if err != nil {
		return err
	}

	defer s.log.Info("server shutdown complete")

	go s.metricsServer(args)

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	done := make(chan any)
	defer close(done)

	wg.Add(1)
	go s.serveRedis(wg, done, args)

	// wait for a termination signal, then gracefully shut down
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	s.log.Info("shutdown signal received, stopping server")

	return nil
}

func (s *server) metricsServer(args *Args) {
	if args.MetricsAddr == "" {
		s.log.Info("metrics server not listening because it has been disabled by the user")
		return
	}

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/version", env.VersionHandler)

	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		_, err := fmt.Fprintf(w, "OK\n")
		if err != nil {
			s.log.Warn("failed to write ping response", "err", err)
			return
		}
	})

	srv := http.Server{
		Addr:         args.MetricsAddr,
		Handler:      http.DefaultServeMux,
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	}

	s.log.Info("metrics server listening", "addr", srv.Addr)

	err := srv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		s.log.Error("error in metrics server", "error", err)
		os.Exit(1)
	}
}

func (s *server) serveRedis(wg *sync.WaitGroup, done <-chan any, args *Args) {
	defer wg.Done()

	l, err := net.Listen("tcp", args.RedisAddr)
	if err != nil {
		s.log.Error("failed to initialize redis listener", "err", err)
		os.Exit(1)
	}

	s.log.Info("redis server listening", "addr", args.RedisAddr)

	go func() {
		// wait until the user requests that the server is shut down, then close the listener
		<-done
		if err := l.Close(); err != nil {
			s.log.Error("failed to close redis server", "err", err)
			os.Exit(1)
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-done:
				s.log.Info("redis server stopped")
				return
			default:
			}

			s.log.Warn("failed to accept client connection", "err", err)
			continue
		}

		sess := redis.NewSession(&redis.NewSessionArgs{
			Conn: conn,
			FDB:  s.fdb,
			Dirs: s.redisDirs,
		})

		go func() {
			defer func() {
				if err := conn.Close(); err != nil {
					s.log.Warn("failed to close redis client connection", "err", err)
				}
			}()

			sess.Serve(context.Background())
		}()
	}
}
