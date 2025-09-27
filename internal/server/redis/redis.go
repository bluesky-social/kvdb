package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/kvdb/internal/metrics"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type session struct {
	log    *slog.Logger
	tracer trace.Tracer

	fdb    fdb.Database
	conn   io.ReadWriter
	reader *bufio.Reader
}

type NewSessionArgs struct {
	FDB  fdb.Database
	Conn io.ReadWriter
}

func NewSession(args *NewSessionArgs) *session {
	return &session{
		log:    slog.Default().With(slog.String("group", "redis")),
		tracer: otel.Tracer("redis"),
		fdb:    args.FDB,
		conn:   args.Conn,
		reader: bufio.NewReader(args.Conn),
	}
}

func (s *session) write(msg string) {
	_, err := s.conn.Write([]byte(msg))
	if err != nil {
		s.log.Warn("failed to write message to client", "err", err)
	}
}

func (s *session) Serve(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "Serve")
	defer span.End()

	for {
		cmd, err := s.parseCommand(ctx, s.reader)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			s.write(resp.FormatError(fmt.Errorf("failed to parse command: %w", err)))
			continue
		}

		s.write(s.handleCommand(ctx, cmd))
	}
}

func (s *session) parseCommand(ctx context.Context, reader *bufio.Reader) (*resp.Command, error) {
	ctx, span := s.tracer.Start(ctx, "parseCommand") // nolint
	defer span.End()

	cmd, err := resp.ParseCommand(reader)
	if err != nil {
		return nil, recordErr(span, err)
	}

	return cmd, nil
}

func (s *session) handleCommand(ctx context.Context, cmd *resp.Command) string {
	ctx, span := s.tracer.Start(ctx, "handleCommand")
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
	case "quit":
		res = resp.FormatSimpleString("OK")
	case "ping":
		res, err = s.handlePing(ctx, cmd.Args)
	case "get":
		res, err = s.handleGet(ctx, cmd.Args)
	case "exists":
		res, err = s.handleExists(ctx, cmd.Args)
	case "set":
		res, err = s.handleSet(ctx, cmd.Args)
	case "del":
		res, err = s.handleDelete(ctx, cmd.Args)
	default:
		err := fmt.Errorf("unknown command %q", cmd.Name)
		span.RecordError(err)
		return resp.FormatError(err)
	}
	if err != nil {
		span.RecordError(err)
		return resp.FormatError(err)
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

func validateNumArgs(args []resp.Value, num int) error {
	if len(args) < num {
		return fmt.Errorf("invalid arguments")
	}
	return nil
}

func recordErr(span trace.Span, err error) error {
	span.RecordError(err)
	return err
}
