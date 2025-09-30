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
	"github.com/bluesky-social/kvdb/internal/types"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// Seeds the database with an admin user on first-time starutp. Once an admin user has been created
// on the cluster, no others will be created.
func InitAdminUser(db fdb.Database, dirs *Directories, username, password string) error {
	existsKey := dirs.user.Pack(tuple.Tuple{"_admin_user_initialized"})

	existsAny, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		buf, err := tx.Get(existsKey).Get()
		if err != nil {
			return false, nil
		}
		if len(buf) == 0 {
			return false, nil
		}

		return strconv.ParseBool(string(buf))
	})
	if err != nil {
		return fmt.Errorf("failed to check if admin user has already been set once: %w", err)
	}

	exists, err := cast[bool](existsAny)
	if err != nil {
		return err
	}
	if exists {
		return nil // admin already created; nothing to do
	}

	passHash, err := hashPassword(password)
	if err != nil {
		return fmt.Errorf("failed to hash admin password: %w", err)
	}

	now := timestamppb.Now()
	user := &types.User{
		Username:     username,
		PasswordHash: passHash,
		Created:      now,
		LastLogin:    now,
		Enabled:      true,
		Rules: []*types.UserACLRule{{
			Level: types.UserAccessLevel_USER_ACCESS_LEVEL_CLUSTER_ADMIN,
		}},
	}

	userBuf, err := proto.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to proto marshal user: %w", err)
	}

	usernameKey := dirs.user.Pack(tuple.Tuple{username})

	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		existingBuf, err := tx.Get(usernameKey).Get()
		if err != nil {
			return nil, err
		}
		if len(existingBuf) > 0 {
			return nil, fmt.Errorf("user with username %q already exists", username)
		}

		tx.Set(usernameKey, userBuf)
		tx.Set(existsKey, []byte("true"))
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("failed to create admin user: %w", err)
	}

	return nil
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
	objDir        directory.DirectorySubspace
	metaDir       directory.DirectorySubspace
	uidDir        directory.DirectorySubspace
	reverseUIDDir directory.DirectorySubspace

	user *types.User
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

// Returns the FDB key of an object in the global user directory
func (s *session) userKey(username string) fdb.Key {
	s.userMu.RLock()
	defer s.userMu.RUnlock()

	return s.dirs.user.Pack(tuple.Tuple{username})
}

// Returns the FDB key of an object in the per-user object directory
func (s *session) objectKey(id string, offset uint32) (fdb.Key, error) {
	s.userMu.RLock()
	defer s.userMu.RUnlock()

	if s.user == nil {
		return nil, fmt.Errorf("authentication is required")
	}

	return s.user.objDir.Pack(tuple.Tuple{id, int(offset)}), nil
}

// Returns the FDB key of an object in the per-user meta directory
func (s *session) metaKey(id string) (fdb.Key, error) {
	s.userMu.RLock()
	defer s.userMu.RUnlock()

	if s.user == nil {
		return nil, fmt.Errorf("authentication is required")
	}

	return s.user.metaDir.Pack(tuple.Tuple{id}), nil
}

// Returns the FDB key of an object in the per-user uid directory
func (s *session) uidKey(id string) (fdb.Key, error) {
	s.userMu.RLock()
	defer s.userMu.RUnlock()

	if s.user == nil {
		return nil, fmt.Errorf("authentication is required")
	}

	return s.user.uidDir.Pack(tuple.Tuple{id}), nil
}

// Returns the FDB key of an object in the per-user uid directory
func (s *session) reverseUIDKey(id string) (fdb.Key, error) {
	s.userMu.RLock()
	defer s.userMu.RUnlock()

	if s.user == nil {
		return nil, fmt.Errorf("authentication is required")
	}

	return s.user.reverseUIDDir.Pack(tuple.Tuple{id}), nil
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
	case "acl":
		res, err = s.handleACL(ctx, cmd.Args)
	case "get":
		res, err = s.handleGet(ctx, cmd.Args)
	case "exists":
		res, err = s.handleExists(ctx, cmd.Args)
	case "set":
		res, err = s.handleSet(ctx, cmd.Args)
	case "del":
		res, err = s.handleDelete(ctx, cmd.Args)
	case "incr":
		res, err = s.handleIncr(ctx, cmd.Args)
	case "incrby":
		res, err = s.handleIncrBy(ctx, cmd.Args)
	case "incrbyfloat":
		res, err = s.handleIncrByFloat(ctx, cmd.Args)
	case "decr":
		res, err = s.handleDecr(ctx, cmd.Args)
	case "decrby":
		res, err = s.handleDecrBy(ctx, cmd.Args)
	case "sadd":
		res, err = s.handleSetAdd(ctx, cmd.Args)
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
	return t, fmt.Errorf("failed to cast to %T", t)
}

func (s *session) setProtoItem(key fdb.Key, item proto.Message) error {
	buf, err := proto.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to proto marshal item: %w", err)
	}

	_, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		tx.Set(key, buf)
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("failed to write item to database: %w", err)
	}

	return nil
}

func getItem[T any](s *session, key fdb.Key) (T, error) {
	var item T
	itemAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(key).Get()
	})
	if err != nil {
		return item, err
	}

	if itemAny == nil {
		return item, nil // item does not exist
	}

	return cast[T](itemAny)
}

func (s *session) getProtoItem(key fdb.Key, item proto.Message) (bool, error) {
	buf, err := getItem[[]byte](s, key)
	if err != nil {
		return false, err
	}

	if len(buf) == 0 {
		return false, nil // item does not exist
	}

	if err := proto.Unmarshal(buf, item); err != nil {
		return false, err
	}

	return true, nil
}

func (s *session) getUser(username string) (*types.User, error) {
	user := &types.User{}
	exists, err := s.getProtoItem(s.userKey(username), user)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	return user, nil
}

func userIsAdmin(user *types.User) bool {
	if !user.Enabled {
		return false
	}

	for _, acl := range user.Rules {
		if acl.Level == types.UserAccessLevel_USER_ACCESS_LEVEL_CLUSTER_ADMIN {
			return true
		}
	}

	return false
}

// Returns the associated metadata for an object. Returns nil if the object does not exist.
func (s *session) getObjectMeta(tx fdb.ReadTransaction, id string) (fdb.Key, *types.ObjectMeta, error) {
	metaKey, err := s.metaKey(id)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get meta key: %w", err)
	}

	metaBuf, err := tx.Get(metaKey).Get()
	if err != nil {
		return metaKey, nil, fmt.Errorf("failed to get object meta from database: %w", err)
	}

	if len(metaBuf) == 0 {
		return metaKey, nil, nil
	}

	meta := &types.ObjectMeta{}
	if err = proto.Unmarshal(metaBuf, meta); err != nil {
		return metaKey, nil, fmt.Errorf("failed to proto unmarshal object meta: %w", err)
	}

	return metaKey, meta, nil
}

func (s *session) getObject(tx fdb.ReadTransaction, id string) (*types.ObjectMeta, []byte, error) {
	_, meta, err := s.getObjectMeta(tx, id)
	if err != nil {
		return nil, nil, err
	}
	if meta == nil {
		return nil, nil, nil
	}

	// @TODO (jrc): update last_accessed out of band
	// @TODO (jrc): read all chunks in parallel

	buf := []byte{}
	for ndx := range meta.NumChunks {
		chunkKey, err := s.objectKey(id, ndx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get object chunk key: %w", err)
		}

		chunk, err := tx.Get(chunkKey).Get()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get object chunk: %w", err)
		}

		buf = append(buf, chunk...)
	}

	return meta, buf, nil
}

func (s *session) writeObject(tx fdb.Transaction, id string, data []byte) error {
	now := timestamppb.Now()

	// check if the object already exists and should be overwritten
	metaKey, meta, err := s.getObjectMeta(tx, id)
	if err != nil {
		return err
	}
	if meta != nil {
		// delete then update existing
		if err := s.deleteObject(tx, id, meta); err != nil {
			return err
		}
	} else {
		// create new
		meta = &types.ObjectMeta{
			Created: now,
		}
	}
	meta.Updated = now
	meta.LastAccess = now

	const maxValBytes = 100_000
	length := uint32(len(data))
	meta.NumChunks = (length / maxValBytes) + 1
	if length%maxValBytes == 0 {
		meta.NumChunks = length / maxValBytes
	}

	// write the meta object
	metaBuf, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to proto marshal object meta: %w", err)
	}
	tx.Set(metaKey, metaBuf)

	// write all object data chunks
	for ndx := range meta.NumChunks {
		start := ndx * maxValBytes
		end := min((ndx+1)*maxValBytes, length)

		chunkKey, err := s.objectKey(id, ndx)
		if err != nil {
			return fmt.Errorf("failed to get object chunk key: %w", err)
		}

		tx.Set(chunkKey, data[start:end])
	}

	return nil
}

func (s *session) deleteObject(tx fdb.Transaction, id string, meta *types.ObjectMeta) error {
	// delete the meta object
	metaKey, err := s.metaKey(id)
	if err != nil {
		return fmt.Errorf("failed to get meta key: %w", err)
	}
	tx.Clear(metaKey)

	// clear all object chunks
	begin, err := s.objectKey(id, 0)
	if err != nil {
		return fmt.Errorf("failed to get object begin key: %w", err)
	}
	end, err := s.objectKey(id, meta.NumChunks-1)
	if err != nil {
		return fmt.Errorf("failed to get object start key: %w", err)
	}
	tx.ClearRange(fdb.KeyRange{
		Begin: begin,
		End:   end,
	})

	return nil
}
