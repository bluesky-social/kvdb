package redis

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"

	roaring "github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/go-util/pkg/concurrent"
	"github.com/bluesky-social/kvdb/internal/types"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	errInvalidCredentials = errors.New("invalid credentials")
)

func (s *session) handlePing(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handlePing") // nolint
	defer span.End()

	if len(args) > 1 {
		return "", recordErr(span, fmt.Errorf("invalid number of arguments"))
	}

	res := "+PONG\r\n"
	if len(args) > 0 {
		// echo back the first argument
		arg, err := extractStringArg(args[0])
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse argument: %w", err))
		}
		res = resp.FormatSimpleString(arg)
	}

	span.SetStatus(codes.Ok, "ping handled")
	return res, nil
}

func (s *session) handleAuth(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleAuth") // nolint
	defer span.End()

	s.userMu.RLock()
	alreadyAuthed := s.user != nil
	s.userMu.RUnlock()
	if alreadyAuthed {
		span.SetStatus(codes.Ok, "already authed")
		return resp.FormatError(fmt.Errorf("session is already authenticated")), nil
	}

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	username, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse username argument: %w", err))
	}

	pass, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse password argument: %w", err))
	}

	user, err := s.getUser(username)
	if err != nil {
		return "", recordErr(span, err)
	}
	if user == nil {
		span.SetStatus(codes.Ok, "user not found")
		return resp.FormatError(errInvalidCredentials), nil
	}
	if !user.Enabled {
		span.SetStatus(codes.Ok, "user is disabled")
		return resp.FormatError(errInvalidCredentials), nil
	}
	if err := comparePasswords(user.PasswordHash, pass); err != nil {
		span.SetStatus(codes.Ok, "invalid password")
		return resp.FormatError(errInvalidCredentials), nil
	}

	// set the user directory on the session
	userDir, err := s.dirs.redis.CreateOrOpen(s.fdb, []string{user.Username}, nil)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to initialize user directory: %w", err))
	}

	objDir, err := userDir.CreateOrOpen(s.fdb, []string{"obj"}, nil)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to initialize user object directory: %w", err))
	}

	metaDir, err := userDir.CreateOrOpen(s.fdb, []string{"meta"}, nil)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to initialize user meta directory: %w", err))
	}

	uidDir, err := userDir.CreateOrOpen(s.fdb, []string{"uid"}, nil)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to initialize user uid directory: %w", err))
	}

	reverseUIDDir, err := userDir.CreateOrOpen(s.fdb, []string{"ruid"}, nil)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to initialize user reverse uid directory: %w", err))
	}

	s.userMu.Lock()
	s.user = &sessionUser{
		objDir:        objDir,
		metaDir:       metaDir,
		uidDir:        uidDir,
		reverseUIDDir: reverseUIDDir,
		user:          user,
	}
	s.userMu.Unlock()

	span.SetStatus(codes.Ok, "auth handled")
	return resp.FormatSimpleString("OK"), nil
}

// Implements a small subset of the standard redis functionality for creating a new user.
//
// Example: `ACL SETUSER newusername on >password123 ~* &* +@all`
func (s *session) handleACL(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleACL") // nolint
	defer span.End()

	//
	// Only admin users are allowed to create other users
	//

	err := func() error {
		s.userMu.RLock()
		defer s.userMu.RUnlock()

		if !userIsAdmin(s.user.user) {
			return fmt.Errorf("only admin users are allowed to run ACL commands")
		}

		return nil
	}()
	if err != nil {
		return "", recordErr(span, err)
	}

	//
	// Validate and parse basic arguments required to create a user and
	// password that's allowed read+write on all keys and commands
	//

	if err := validateNumArgs(args, 4); err != nil {
		return "", recordErr(span, err)
	}

	cmd, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse acl command argument: %w", err))
	}
	if strings.ToLower(cmd) != "setuser" {
		return "", recordErr(span, fmt.Errorf("only SETUSER is supported"))
	}

	username, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse username argument: %w", err))
	}

	on, err := extractStringArg(args[2])
	if err != nil || strings.ToLower(on) != "on" {
		return "", recordErr(span, fmt.Errorf(`failed to parse "on" argument`))
	}

	password, err := extractStringArg(args[3])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse password argument: %w", err))
	}
	if !strings.HasPrefix(password, ">") {
		return "", recordErr(span, fmt.Errorf(`only ">" passwords are supported`))
	}
	password = strings.TrimPrefix(password, ">")

	if err := validateUsername(username); err != nil {
		return "", recordErr(span, fmt.Errorf("invalid username: %w", err))
	}

	if err := validatePassword(password); err != nil {
		return "", recordErr(span, fmt.Errorf("invalid password: %w", err))
	}

	//
	// Ensure the user does not already exist
	//

	existing, err := s.getUser(username)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to check if user already exists: %w", err))
	}
	if existing != nil {
		return "", recordErr(span, fmt.Errorf("failed to create user because a user with username %q already exists", username))
	}

	//
	// Create the user in the users directory
	//

	passHash, err := hashPassword(password)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to hash password: %w", err))
	}

	now := timestamppb.Now()
	user := &types.User{
		Username:     username,
		PasswordHash: passHash,
		Created:      now,
		LastLogin:    now,
		Enabled:      true,
		Rules: []*types.UserACLRule{{
			Level: types.UserAccessLevel_USER_ACCESS_LEVEL_READ_WRITE,
		}},
	}

	if err := s.setProtoItem(s.userKey(username), user); err != nil {
		return "", recordErr(span, fmt.Errorf("failed to save user to database: %w", err))
	}

	span.SetStatus(codes.Ok, "acl handled")
	return resp.FormatSimpleString("OK"), nil
}

func validateUsername(u string) error {
	if u == "" {
		return fmt.Errorf("username cannot be empty")
	}
	if strings.HasPrefix(u, "_") {
		return fmt.Errorf("username starts with a restricted character")
	}
	if containsWhitespace(u) {
		return fmt.Errorf("username cannot contain whitespace")
	}

	return nil
}

func validatePassword(p string) error {
	if p == "" {
		return fmt.Errorf("password cannot be empty")
	}
	if len(p) < 8 {
		return fmt.Errorf("password is too short")
	}
	if containsWhitespace(p) {
		return fmt.Errorf("password cannot contain whitespace")
	}

	return nil
}

func containsWhitespace(s string) bool {
	whitespace := []byte{'\t', '\n', '\v', '\f', '\r', ' '}
	return strings.ContainsAny(s, string(whitespace))
}

func (s *session) handleGet(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleGet") // nolint
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", fmt.Errorf("failed to parse argument: %w", err)
	}

	bufAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		_, buf, err := s.getObject(tx, key)
		return buf, err
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get value: %w", err))
	}

	buf, err := cast[[]byte](bufAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to cast result value: %w", err))
	}

	res := resp.FormatNil()
	if len(buf) > 0 {
		res = resp.FormatBulkString(string(buf))
	}

	span.SetStatus(codes.Ok, "get handled")
	return res, nil
}

func (s *session) handleExists(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleExists") // nolint
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", fmt.Errorf("failed to parse argument: %w", err)
	}

	existsAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		_, meta, err := s.getObjectMeta(tx, key)
		if err != nil {
			return nil, err
		}
		return meta != nil, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to check if item exists: %w", err))
	}

	exists, err := cast[bool](existsAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to cast result value: %w", err))
	}

	span.SetStatus(codes.Ok, "exists handled")
	return resp.FormatBoolAsInt(exists), nil
}

func (s *session) handleSet(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSet") // nolint
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	val, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse value argument: %w", err))
	}

	_, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		return nil, s.writeObject(tx, key, []byte(val))
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to set value: %w", err))
	}

	span.SetStatus(codes.Ok, "set handled")
	return resp.FormatSimpleString("OK"), nil
}

func (s *session) handleDelete(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleDelete") // nolint
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	existsAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		_, meta, err := s.getObjectMeta(tx, key)
		if err != nil {
			return false, err
		}
		if meta == nil {
			return false, nil // object does not exist
		}

		err = s.deleteObject(tx, key, meta)
		if err != nil {
			return false, err
		}

		return true, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to delete value: %w", err))
	}

	exists, err := cast[bool](existsAny)
	if err != nil {
		return "", recordErr(span, err)
	}

	span.SetStatus(codes.Ok, "delete handled")
	return resp.FormatBoolAsInt(exists), nil
}

func (s *session) handleIncr(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleIncr")
	defer span.End()

	res, err := s.handleIncrDecr(ctx, args, "1")
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to write value: %w", err))
	}

	span.SetStatus(codes.Ok, "incr handled")
	return resp.FormatInt(res), nil
}

func (s *session) handleIncrBy(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleIncrBy")
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	by, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse increment argument: %w", err))
	}

	res, err := s.handleIncrDecr(ctx, args, by)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to write value: %w", err))
	}

	span.SetStatus(codes.Ok, "incrby handled")
	return resp.FormatInt(res), nil
}

func (s *session) handleDecr(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleDecr")
	defer span.End()

	res, err := s.handleIncrDecr(ctx, args, "-1")
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to write value: %w", err))
	}

	span.SetStatus(codes.Ok, "decr handled")
	return resp.FormatInt(res), nil
}

func (s *session) handleDecrBy(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleDecrBy")
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	by, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse decrement argument: %w", err))
	}

	// negate
	if !strings.HasPrefix(by, "-") {
		by = "-" + by
	} else {
		by = strings.TrimPrefix(by, "-")
	}

	res, err := s.handleIncrDecr(ctx, args, by)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to write value: %w", err))
	}

	span.SetStatus(codes.Ok, "decrby handled")
	return resp.FormatInt(res), nil
}

func (s *session) handleIncrDecr(ctx context.Context, args []resp.Value, byStr string) (int64, error) {
	ctx, span := s.tracer.Start(ctx, "handleIncrDecr") // nolint
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return 0, err
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return 0, fmt.Errorf("failed to parse key argument: %w", err)
	}

	by, err := strconv.ParseInt(byStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid increment %q: %w", byStr, err)
	}

	resAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		_, buf, err := s.getObject(tx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get existing value")
		}
		if len(buf) == 0 {
			buf = []byte("0") // create a new value
		}

		num, err := strconv.ParseInt(string(buf), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse stored numeric: %w", err)
		}

		num += by

		buf = []byte(strconv.FormatInt(num, 10))
		if err = s.writeObject(tx, key, buf); err != nil {
			return nil, fmt.Errorf("failed to write value to database: %w", err)
		}

		return num, nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to delete value: %w", err)
	}

	res, err := cast[int64](resAny)
	if err != nil {
		return 0, recordErr(span, err)
	}

	span.SetStatus(codes.Ok, "incr decr handled")
	return res, nil
}

func (s *session) handleIncrByFloat(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleIncrByFloat") // nolint
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	byStr, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse increment argument: %w", err))
	}

	by, err := strconv.ParseFloat(byStr, 64)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid increment %q: %w", byStr, err))
	}

	resAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		_, buf, err := s.getObject(tx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get existing value")
		}
		if len(buf) == 0 {
			buf = []byte("0") // create a new value
		}

		num, err := strconv.ParseFloat(string(buf), 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse stored numeric: %w", err)
		}

		num += by

		buf = []byte(strconv.FormatFloat(num, 'g', -1, 64))
		if err = s.writeObject(tx, key, buf); err != nil {
			return nil, fmt.Errorf("failed to write value to database: %w", err)
		}

		return buf, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to delete value: %w", err))
	}

	res, err := cast[[]byte](resAny)
	if err != nil {
		return "", recordErr(span, recordErr(span, err))
	}

	span.SetStatus(codes.Ok, "incrbyfloat handled")
	return resp.FormatBulkString(string(res)), nil
}

// Interns, stores, and returns a new UID. The upper 32 bits are a random sequence number, and the lower
// 32 bits are a sequential number within that space. This allows for up to 4 billion unique IDs per
// sequence and very low contention when allocating new IDs
func (s *session) allocateNewUID(ctx context.Context, tx fdb.Transaction) (uint64, error) {
	ctx, span := s.tracer.Start(ctx, "allocateNewUID") // nolint
	defer span.End()

	// sequenceNum is the random uint32 sequence we are using for this allocation
	var sequenceNum uint32
	var sequenceKey fdb.Key

	// assignedUID is the uint32 within the sequence we will assign
	var assignedUID uint32

	for range 20 {
		// pick a random uint32 as the sequence we will be using for this member
		sequenceNum = rand.Uint32()

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
			lastUID, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				span.RecordError(err)
				return 0, fmt.Errorf("failed to parse last uid: %w", err)
			}

			// if we have exhausted this sequence, pick a new random sequence
			if lastUID >= 0xFFFFFFFF {
				continue
			}

			assignedUID = uint32(lastUID) + 1
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

// Returns the UID for the given member string, creating a new one if it does not exist.
// If peek is true, it will only look up the UID without creating a new one. If peeking
// and the member does not exist, it returns 0 without an error. UIDs start at 1, so 0
// is never a valid UID.
func (s *session) getUID(ctx context.Context, tx fdb.Transaction, member string, peek bool) (uint64, error) {
	ctx, span := s.tracer.Start(ctx, "getUID") // nolint
	defer span.End()

	memberToUIDKey, err := s.reverseUIDKey(member)
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to get uid key: %w", err)
	}

	val, err := tx.Get(memberToUIDKey).Get()
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to get member to uid mapping: %w", err)
	}
	if peek {
		// just look up the UID without creating a new one
		if len(val) == 0 {
			val = []byte("0")
		}
	} else {
		// check if we've already assigned a UID to this member
		val, err = tx.Get(memberToUIDKey).Get()
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
	}
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to get or create uid: %w", err)
	}

	uid, err := strconv.ParseUint(string(val), 10, 64)
	if err != nil {
		span.RecordError(err)
		return 0, fmt.Errorf("failed to parse uid: %w", err)
	}

	span.SetStatus(codes.Ok, "getUID ok")
	return uid, nil
}

func (s *session) handleSetAdd(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetAdd")
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	members := make([]string, 0, len(args)-1)
	for ndx, arg := range args[1:] {
		member, err := extractStringArg(arg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse member argument at index %d: %w", ndx, err))
		}
		members = append(members, member)
	}

	addedAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		// allocate a new UID for each member
		r := concurrent.New[string, uint64]()
		uids, err := r.Do(ctx, members, func(member string) (n uint64, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = recoverErr("assigning new uids", r)
				}
			}()

			return s.getUID(ctx, tx, member, false)
		})
		if err != nil {
			return nil, recordErr(span, fmt.Errorf("failed to get uids for members: %w", err))
		}

		// get the bitmap if it exists
		_, blob, err := s.getObject(tx, key)
		if err != nil {
			return int64(0), fmt.Errorf("failed to get existing set: %w", err)
		}

		// if the key doesn't exist, create a new bitmap
		bitmap := roaring.New()
		if len(blob) > 0 {
			bitmap = roaring.New()
			if err := bitmap.UnmarshalBinary(blob); err != nil {
				return int64(0), fmt.Errorf("failed to unmarshal existing bitmap: %w", err)
			}
		}

		// diff against the existing map to determine how many members are new
		added := int64(0)
		for _, uid := range uids {
			if !bitmap.Contains(uid) {
				added += 1
			}
		}

		// add the new UIDs to the bitmap
		bitmap.AddMany(uids)

		// serialize and store the updated bitmap
		data, err := bitmap.MarshalBinary()
		if err != nil {
			return int64(0), fmt.Errorf("failed to marshal bitmap: %w", err)
		}

		if err = s.writeObject(tx, key, data); err != nil {
			return int64(0), fmt.Errorf("failed to write set: %w", err)
		}

		return added, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to add members to set: %w", err))
	}

	added, err := cast[int64](addedAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	span.SetStatus(codes.Ok, "sadd handled")
	return resp.FormatInt(added), nil
}

func (s *session) handleSetRemove(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetRemove")
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	members := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		member, err := extractStringArg(arg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse member argument: %w", err))
		}
		members = append(members, member)
	}

	removedAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		// lookup the UID for each member
		r := concurrent.New[string, uint64]()
		uids, err := r.Do(ctx, members, func(member string) (n uint64, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = recoverErr("looking up uids", r)
				}
			}()

			return s.getUID(ctx, tx, member, true)
		})
		if err != nil {
			return nil, recordErr(span, fmt.Errorf("failed to get uids for members: %w", err))
		}

		// get the bitmap if it exists
		objMeta, blob, err := s.getObject(tx, key)
		if err != nil {
			return int64(0), fmt.Errorf("failed to get existing set: %w", err)
		}

		// if the bitmap doesn't exist, there's nothing to remove
		if len(blob) == 0 {
			return int64(0), nil
		}

		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(blob); err != nil {
			return int64(0), fmt.Errorf("failed to unmarshal existing bitmap: %w", err)
		}

		// remove from the bitmap and count
		removed := int64(0)
		for _, uid := range uids {
			if bitmap.Contains(uid) {
				bitmap.Remove(uid)
				removed++
			}
		}

		// if the set is now empty, delete the object
		if bitmap.IsEmpty() {
			if err := s.deleteObject(tx, key, objMeta); err != nil {
				return int64(0), fmt.Errorf("failed to delete object: %w", err)
			}
			return removed, nil
		}

		// serialize and store the updated bitmap
		data, err := bitmap.MarshalBinary()
		if err != nil {
			return int64(0), fmt.Errorf("failed to marshal bitmap: %w", err)
		}

		if err = s.writeObject(tx, key, data); err != nil {
			return int64(0), fmt.Errorf("failed to write set: %w", err)
		}

		return removed, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to remove members from set: %w", err))
	}

	removed, err := cast[int64](removedAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	span.SetStatus(codes.Ok, "srem handled")
	return resp.FormatInt(removed), nil
}

func (s *session) handleSetIsMember(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetIsMember") // nolint
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	member, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set member argument: %w", err))
	}

	containsAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		// lookup the UID for the member
		memberUID, err := s.getUID(ctx, tx, member, true)
		if err != nil {
			return false, recordErr(span, fmt.Errorf("failed to get uid for member: %w", err))
		}

		// get the bitmap if it exists
		_, blob, err := s.getObject(tx, key)
		if err != nil {
			return false, fmt.Errorf("failed to get existing set: %w", err)
		}

		// if the bitmap doesn't exist, the member does not exist in the set
		if len(blob) == 0 {
			return false, nil
		}

		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(blob); err != nil {
			return false, fmt.Errorf("failed to unmarshal existing bitmap: %w", err)
		}

		return bitmap.Contains(memberUID), nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to check if member is in set: %w", err))
	}

	contains, err := cast[bool](containsAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	span.SetStatus(codes.Ok, "srem handled")
	return resp.FormatBoolAsInt(contains), nil
}
