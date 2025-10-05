package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
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

	dir.redis, err = directory.CreateOrOpen(db, []string{"redis_v0"}, nil)
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
	listDir       directory.DirectorySubspace

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

	metrics.SpanOK(span)
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

// Returns the FDB key of an object in the per-user, per-list item directory
func (s *session) listItemMetaKey(listID, itemID string) (fdb.Key, error) {
	s.userMu.RLock()
	defer s.userMu.RUnlock()

	if s.user == nil {
		return nil, fmt.Errorf("authentication is required")
	}

	dir, err := s.user.listDir.CreateOrOpen(s.fdb, []string{listID}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open per-list directory: %w", err)
	}

	return dir.Pack(tuple.Tuple{itemID}), nil
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

	metrics.SpanOK(span)
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
		metrics.SpanOK(span)
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
	case "expire":
		res, err = s.handleExpire(ctx, cmd.Args)
	case "sadd":
		res, err = s.handleSetAdd(ctx, cmd.Args)
	case "srem":
		res, err = s.handleSetRemove(ctx, cmd.Args)
	case "sismember":
		res, err = s.handleSetIsMember(ctx, cmd.Args)
	case "scard":
		res, err = s.handleSetCard(ctx, cmd.Args)
	case "smembers":
		res, err = s.handleSetMembers(ctx, cmd.Args)
	case "sinter":
		res, err = s.handleSetInter(ctx, cmd.Args)
	case "sunion":
		res, err = s.handleSetUnion(ctx, cmd.Args)
	case "sdiff":
		res, err = s.handleSetDiff(ctx, cmd.Args)
	case "llen":
		res, err = s.handleLLen(ctx, cmd.Args)
	case "lpush":
		res, err = s.handleLPush(ctx, cmd.Args)
	case "rpush":
		res, err = s.handleRPush(ctx, cmd.Args)
	case "lindex":
		res, err = s.handleLIndex(ctx, cmd.Args)
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
	metrics.SpanOK(span)
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

func containsWhitespace(s string) bool {
	whitespace := []byte{'\t', '\n', '\v', '\f', '\r', ' '}
	return strings.ContainsAny(s, string(whitespace))
}

func recordErr(span trace.Span, err error) error {
	span.RecordError(err)
	return err
}

func recoverErr(loc string, r any) error {
	return fmt.Errorf("caught a panic in %s: %v", loc, r)
}

func cast[T any](item any) (T, error) {
	if res, ok := item.(T); ok {
		return res, nil
	}

	var t T
	return t, fmt.Errorf("failed to cast item of type %T to %T", item, t)
}

func setProtoItem(tx fdb.Transaction, key fdb.Key, item proto.Message) error {
	buf, err := proto.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to proto marshal item: %w", err)
	}

	tx.Set(key, buf)
	return nil
}

func getItem[T any](tx fdb.ReadTransaction, key fdb.Key) (T, error) {
	var item T
	itemAny, err := tx.Get(key).Get()
	if err != nil {
		return item, err
	}
	if itemAny == nil {
		return item, nil // item does not exist
	}

	return cast[T](itemAny)
}

func getProtoItem(tx fdb.ReadTransaction, key fdb.Key, item proto.Message) (bool, error) {
	buf, err := getItem[[]byte](tx, key)
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
	existsAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return getProtoItem(tx, s.userKey(username), user)
	})
	if err != nil {
		return nil, err
	}

	exists, err := cast[bool](existsAny)
	if err != nil {
		return nil, err
	}

	if !exists {
		user = nil
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

func (s *session) getMeta(ctx context.Context, tx fdb.ReadTransaction, id string, meta proto.Message) (bool, fdb.Key, error) {
	ctx, span := s.tracer.Start(ctx, "getMeta") // nolint
	defer span.End()

	metaKey, err := s.metaKey(id)
	if err != nil {
		span.RecordError(err)
		return false, nil, fmt.Errorf("failed to get meta key: %w", err)
	}

	metaBuf, err := tx.Get(metaKey).Get()
	if err != nil {
		span.RecordError(err)
		return false, metaKey, fmt.Errorf("failed to get object meta: %w", err)
	}

	if len(metaBuf) == 0 {
		metrics.SpanOK(span)
		return false, metaKey, nil
	}

	if err = proto.Unmarshal(metaBuf, meta); err != nil {
		span.RecordError(err)
		return false, metaKey, fmt.Errorf("failed to proto unmarshal object meta: %w", err)
	}

	metrics.SpanOK(span)
	return true, metaKey, nil
}

// Returns the associated metadata for an object of any type. Returns nil if it not exist.
func (s *session) getObjectMeta(ctx context.Context, tx fdb.ReadTransaction, id string) (fdb.Key, *types.ObjectMeta, error) {
	ctx, span := s.tracer.Start(ctx, "getObjectMeta")
	defer span.End()

	meta := &types.ObjectMeta{}
	exists, key, err := s.getMeta(ctx, tx, id, meta)
	if err != nil {
		span.RecordError(err)
		return nil, nil, err
	}
	if !exists {
		meta = nil
	}

	metrics.SpanOK(span)
	return key, meta, nil
}

// Returns the associated metadata for an object. Returns nil if the object does not exist.
func (s *session) getListMeta(ctx context.Context, tx fdb.ReadTransaction, id string) (fdb.Key, *types.ListMeta, error) {
	ctx, span := s.tracer.Start(ctx, "getListMeta")
	defer span.End()

	key, objMeta, err := s.getObjectMeta(ctx, tx, id)
	if err != nil {
		span.RecordError(err)
		return nil, nil, fmt.Errorf("failed to get object meta: %w", err)
	}
	if objMeta == nil {
		// item does not exist
		metrics.SpanOK(span)
		return key, nil, nil
	}

	listMeta, err := cast[*types.ObjectMeta_List](objMeta.Type)
	if err != nil {
		span.RecordError(err)
		return nil, nil, err
	}

	metrics.SpanOK(span)
	return key, listMeta.List, nil
}

// Interns, stores, and returns a new UID. The upper 16 bits are a random sequence number, and the lower
// 48 bits are a sequential number within that space. This allows for many unique IDs per sequence and
// very low contention when allocating new IDs.
func (s *session) allocateNewUID(ctx context.Context, tx fdb.Transaction) (uint64, error) {
	ctx, span := s.tracer.Start(ctx, "allocateNewUID") // nolint
	defer span.End()

	const (
		uidSequenceIDBitWidth = 16
		maxSequenceID         = 1 << uidSequenceIDBitWidth
		maxAssignedUID        = (1 << (64 - uidSequenceIDBitWidth)) - 1
	)

	// sequenceNum is the random sequence we are using for this allocation
	var sequenceNum uint64
	var sequenceKey fdb.Key

	// assignedUID is the uint32 within the sequence we will assign
	var assignedUID uint64

	for range 20 {
		// pick a random number as the sequence we will be using for this member
		sequenceNum = rand.Uint64N(maxSequenceID)

		var err error
		sequenceKey, err = s.uidKey(strconv.FormatUint(uint64(sequenceNum), 10))
		if err != nil {
			span.RecordError(err)
			return 0, fmt.Errorf("failed to get uid key: %w", err)
		}

		val, err := tx.Get(sequenceKey).Get()
		if err != nil {
			return 0, recordErr(span, fmt.Errorf("failed to get last uid: %w", err))
		}

		if len(val) == 0 {
			assignedUID = 1
		} else {
			lastUID, err := strconv.ParseUint(string(val), 10, 64)
			if err != nil {
				span.RecordError(err)
				return 0, fmt.Errorf("failed to parse last uid: %w", err)
			}

			// if we have exhausted this sequence, pick a new random sequence
			if lastUID >= maxAssignedUID {
				continue
			}

			assignedUID = lastUID + 1
		}
	}

	// if we failed to find a sequence after several tries, return an error
	if assignedUID == 0 {
		err := fmt.Errorf("failed to allocate new uid after multiple attempts")
		span.RecordError(err)
		return 0, err
	}

	// assemble the 64-bit UID from the sequence and assigned UID
	newUID := (uint64(sequenceNum) << 32) | uint64(assignedUID)

	// store the assigned UID back to the sequence key for the next allocation
	tx.Set(sequenceKey, []byte(strconv.FormatUint(uint64(assignedUID), 10)))

	// return the full 64-bit UID
	return newUID, nil
}

// Returns the UID for the given member string, creating a new one if it does not exist
func (s *session) getOrAllocateUID(ctx context.Context, tx fdb.Transaction, member string) (uint64, error) {
	ctx, span := s.tracer.Start(ctx, "getOrAllocateUID")
	defer span.End()

	memberToUIDKey, err := s.reverseUIDKey(member)
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to get uid key: %w", err)
	}

	// check if we've already assigned a UID to this member
	val, err := tx.Get(memberToUIDKey).Get()
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to get member to uid mapping: %w", err)
	}

	if len(val) == 0 {
		// allocate a new UID for this member string
		uid, err := s.allocateNewUID(ctx, tx)
		if err != nil {
			span.RecordError(err)
			return 0, fmt.Errorf("failed to allocate new uid: %w", err)
		}

		uidStr := strconv.FormatUint(uid, 10)
		uidToMemberKey, err := s.uidKey(uidStr)
		if err != nil {
			span.RecordError(err)
			return 0, fmt.Errorf("failed to get uid key: %w", err)
		}

		// store the bi-directional mapping
		tx.Set(memberToUIDKey, []byte(uidStr))
		tx.Set(uidToMemberKey, []byte(member))

		val = []byte(uidStr)
	}

	uid, err := strconv.ParseUint(string(val), 10, 64)
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to parse uid: %w", err)
	}

	metrics.SpanOK(span)
	return uid, nil
}

// Returns the UID for the given member string. It will only look up the UID; it will
// never create a new one. UIDs start at 1, so 0 is never a valid UID.
func (s *session) peekUID(ctx context.Context, tx fdb.ReadTransaction, member string) (uint64, error) {
	ctx, span := s.tracer.Start(ctx, "peekUID") // nolint
	defer span.End()

	memberToUIDKey, err := s.reverseUIDKey(member)
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to get uid key: %w", err)
	}

	// check if we've already assigned a UID to this member
	val, err := tx.Get(memberToUIDKey).Get()
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to get member to uid mapping: %w", err)
	}
	if len(val) == 0 {
		val = []byte("0")
	}

	uid, err := strconv.ParseUint(string(val), 10, 64)
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to parse uid: %w", err)
	}

	metrics.SpanOK(span)
	return uid, nil
}

func (s *session) memberFromUID(ctx context.Context, tx fdb.ReadTransaction, uid uint64) (string, error) {
	ctx, span := s.tracer.Start(ctx, "memberFromUID") // nolint
	defer span.End()

	key, err := s.uidKey(strconv.FormatUint(uid, 10))
	if err != nil {
		span.RecordError(err)
		return "", fmt.Errorf("failed to get uid key: %w", err)
	}

	member, err := tx.Get(key).Get()
	if err != nil {
		span.RecordError(err)
		return "", fmt.Errorf("failed to get member for UID %d: %w", uid, err)
	}

	if len(member) == 0 {
		err := fmt.Errorf("uid %d not found", uid)
		span.RecordError(err)
		return "", err
	}

	metrics.SpanOK(span)
	return string(member), nil
}

func parseVariadicArguments(args []resp.Value) ([]string, error) {
	members := make([]string, 0, len(args)-1)
	for ndx, arg := range args[1:] {
		member, err := extractStringArg(arg)
		if err != nil {
			return nil, fmt.Errorf("failed to parse argument at index %d: %w", ndx, err)
		}
		members = append(members, member)
	}

	return members, nil
}
