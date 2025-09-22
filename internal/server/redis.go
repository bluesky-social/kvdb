package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/kvdb/internal/metrics"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

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

		go s.handleRedisConn(context.Background(), &redisSession{
			conn:   conn,
			reader: bufio.NewReader(conn),
		})
	}
}

type redisSession struct {
	conn   net.Conn
	reader *bufio.Reader
}

func (sess *redisSession) write(s *server, msg string) {
	_, err := sess.conn.Write([]byte(msg))
	if err != nil {
		s.log.Warn("failed to write message to client", "err", err)
	}
}

func formatSimpleString(str string) string {
	return fmt.Sprintf("+%s\r\n", str)
}

func formatBulkString(str string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
}

func formatNil() string {
	return "_\r\n"
}

func formatBoolAsInt(val bool) string {
	n := 0
	if val {
		n = 1
	}
	return formatInt(n)
}

func formatInt(n int) string {
	return fmt.Sprintf(":%d\r\n", n)
}

func formatError(err error) string {
	return fmt.Sprintf("-ERR %s\r\n", err)
}

func (s *server) handleRedisConn(ctx context.Context, sess *redisSession) {
	defer func() {
		if err := sess.conn.Close(); err != nil {
			s.log.Warn("failed to close client connection", "err", err)
		}
	}()

	ctx, span := s.tracer.Start(ctx, "handleRedisConn")
	defer span.End()

	for {
		cmd, err := s.parseRedisCommand(ctx, sess.reader)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			sess.write(s, formatError(fmt.Errorf("failed to parse command: %w", err)))
			continue
		}

		resp := s.handleRedisCommand(ctx, cmd)
		sess.write(s, resp)
	}
}

func (s *server) parseRedisCommand(ctx context.Context, reader *bufio.Reader) (*resp.Command, error) {
	ctx, span := s.tracer.Start(ctx, "parseRedisCommand") // nolint
	defer span.End()

	cmd, err := resp.ParseCommand(reader)
	if err != nil {
		return nil, recordErr(span, err)
	}

	return cmd, nil
}

func (s *server) handleRedisCommand(ctx context.Context, cmd *resp.Command) string {
	ctx, span := s.tracer.Start(ctx, "handleRedisCommand") // nolint
	defer span.End()

	cmdLower := strings.ToLower(cmd.Name)

	span.SetAttributes(
		attribute.String("cmd", cmdLower),
		attribute.Int("num_args", len(cmd.Args)),
	)

	status := metrics.StatusError
	start := time.Now()
	defer func() {
		metrics.Queries.WithLabelValues(cmd.Name, status).Inc()
		metrics.QueryDuration.WithLabelValues(cmd.Name, status).Observe(time.Since(start).Seconds())
	}()

	var res string
	var err error
	switch cmdLower {
	case "ping":
		res, err = s.handleRedisPing(ctx, cmd.Args)
	case "get":
		res, err = s.handleRedisGet(ctx, cmd.Args)
	case "exists":
		res, err = s.handleRedisExists(ctx, cmd.Args)
	case "set":
		res, err = s.handleRedisSet(ctx, cmd.Args)
	case "del":
		res, err = s.handleRedisDelete(ctx, cmd.Args)
	case "quit":
		res = "+OK\r\n"
	default:
		err := fmt.Errorf("unknown command %q", cmd.Name)
		span.RecordError(err)
		return formatError(err)
	}
	if err != nil {
		span.RecordError(err)
		return formatError(err)
	}

	status = metrics.StatusOK
	return res
}

func extractStringArg(val resp.Value) (string, error) {
	switch val.Type {
	case resp.TypeSimpleString:
		v, ok := val.Value.(string)
		if !ok {
			return "", fmt.Errorf("invalid simple string type %T", val.Value)
		}
		return v, nil
	case resp.TypeBulkString:
		if val.Value == nil {
			return "", fmt.Errorf("null bulk string")
		}
		v, ok := val.Value.(string)
		if !ok {
			return "", fmt.Errorf("invalid bulk string type %T", val.Value)
		}
		return v, nil
	case resp.TypeInteger:
		v, ok := val.Value.(int64)
		if !ok {
			return "", fmt.Errorf("invalid integer type %T", val.Value)
		}
		return strconv.FormatInt(v, 10), nil
	case resp.TypeDouble:
		v, ok := val.Value.(float64)
		if !ok {
			return "", fmt.Errorf("invalid double type %T", val.Value)
		}
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case resp.TypeBoolean:
		v, ok := val.Value.(bool)
		if !ok {
			return "", fmt.Errorf("invalid bool type %T", val.Value)
		}
		if v {
			return "1", nil
		}
		return "0", nil
	default:
		return "", fmt.Errorf("unsupported type for string conversion: %v", val.Type)
	}
}

func (s *server) handleRedisPing(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisPing") // nolint
	defer span.End()

	const resp = "+PONG\r\n"
	if len(args) == 0 {
		return resp, nil
	}

	// echo back the first argument
	if arg, err := extractStringArg(args[0]); err == nil {
		return formatBulkString(arg), nil
	}

	return resp, nil
}

func (s *server) handleRedisGet(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisGet") // nolint
	defer span.End()

	val, err := s.redisGet(args)
	if err != nil {
		return "", recordErr(span, err)
	}

	if len(val) == 0 {
		// not found
		return formatNil(), nil
	}

	return formatBulkString(string(val)), nil
}

func (s *server) handleRedisExists(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisExists") // nolint
	defer span.End()

	val, err := s.redisGet(args)
	if err != nil {
		return "", recordErr(span, err)
	}

	return formatBoolAsInt(len(val) > 0), nil
}

func (s *server) redisGet(args []resp.Value) ([]byte, error) {
	arg, err := extractStringArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument: %w", err)
	}

	res, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(fdb.Key(arg)).Get()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get value: %w", err)
	}

	val, ok := res.([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid value type: %T", res)
	}

	return val, nil
}

func (s *server) handleRedisSet(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisSet") // nolint
	defer span.End()

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	value, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse value argument: %w", err))
	}

	_, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		tx.Set(fdb.Key(key), []byte(value))
		return nil, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to set value: %w", err))
	}

	return formatSimpleString("OK"), nil
}

func (s *server) handleRedisDelete(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisDelete") // nolint
	defer span.End()

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	exists := false
	_, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		val, err := tx.Get(fdb.Key(key)).Get()
		if err != nil {
			return nil, err
		}
		exists = len(val) > 0

		tx.Clear(fdb.Key(key))
		return nil, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to delete value: %w", err))
	}

	return formatBoolAsInt(exists), nil
}

func recordErr(span trace.Span, err error) error {
	span.RecordError(err)
	return err
}
