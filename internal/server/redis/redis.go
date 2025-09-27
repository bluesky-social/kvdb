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
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/bluesky-social/kvdb/internal/metrics"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Directories struct {
	// The top-level redis directory under which all redis-related data will be saved
	redis directory.DirectorySubspace

	// The directory that contiains information on database suers
	user directory.DirectorySubspace
}

func InitDirectories(db fdb.Database) (dir *Directories, err error) {
	dir = &Directories{}

	dir.redis, err = directory.CreateOrOpen(db, []string{"redis"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis directory: %w", err)
	}

	dir.user, err = dir.redis.CreateOrOpen(db, []string{"_user"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create user directory: %w", err)
	}

	return
}

type session struct {
	log    *slog.Logger
	tracer trace.Tracer

	conn   io.ReadWriter
	reader *bufio.Reader

	fdb  fdb.Database
	dirs *Directories

	// Only set once the user has authenticated
	user   *sessionUser
	userMu *sync.RWMutex
}

type sessionUser struct {
	dir directory.DirectorySubspace
}

type NewSessionArgs struct {
	Conn io.ReadWriter
	FDB  fdb.Database
	Dirs *Directories
}

func NewSession(args *NewSessionArgs) *session {
	return &session{
		log:    slog.Default().With(slog.String("group", "redis")),
		tracer: otel.Tracer("redis"),
		conn:   args.Conn,
		reader: bufio.NewReader(args.Conn),
		fdb:    args.FDB,
		dirs:   args.Dirs,
		userMu: &sync.RWMutex{},
	}
}

func (s *session) Serve(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "Serve")
	defer span.End()

	for {
		cmd, err := s.parseCommand(ctx, s.reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.write(resp.FormatError(fmt.Errorf("failed to parse command: %w", err)))
			continue
		}

		s.write(s.handleCommand(ctx, cmd))
	}

	span.SetStatus(codes.Ok, "session complete")
}

func (s *session) userKey(tup tuple.Tuple) (fdb.Key, error) {
	s.userMu.RLock()
	defer s.userMu.RUnlock()

	if s.user == nil {
		return nil, fmt.Errorf("authentication is required")
	}

	return s.user.dir.Pack(tup), nil
}

func (s *session) write(msg string) {
	_, err := s.conn.Write([]byte(msg))
	if err != nil {
		s.log.Warn("failed to write message to client", "err", err)
	}
}

func (s *session) parseCommand(ctx context.Context, reader *bufio.Reader) (*resp.Command, error) {
	ctx, span := s.tracer.Start(ctx, "parseCommand") // nolint
	defer span.End()

	cmd, err := resp.ParseCommand(reader)
	if err != nil {
		return nil, recordErr(span, err)
	}

	span.SetStatus(codes.Ok, "command parsed")
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

	// allow serving unauthenticated commands regardless of whether the user has provided credentials
	var res string
	var err error
	switch cmdLower {
	case "quit":
		res = resp.FormatSimpleString("OK")
	case "ping":
		res, err = s.handlePing(ctx, cmd.Args)
	case "auth":
		res, err = s.handleAuth(ctx, cmd.Args)
	}

	if err != nil {
		span.RecordError(err)
		return resp.FormatError(err)
	}
	if res != "" {
		status = metrics.StatusOK
		span.SetStatus(codes.Ok, "command handled")
		return res
	}

	// only allow the following commands to be executed if the user is logged in
	if s.user == nil {
		err := fmt.Errorf("authentication is required")
		span.RecordError(err)
		return resp.FormatError(err)
	}

	switch cmdLower {
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
	span.SetStatus(codes.Ok, "command handled")
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

func cast[T any](item any) (T, error) {
	if res, ok := item.(T); ok {
		return res, nil
	}

	var t T
	return t, fmt.Errorf("failed to cast item to %T", t)
}
