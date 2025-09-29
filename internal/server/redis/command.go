package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/bluesky-social/kvdb/internal/types"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/proto"
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

	user, err := s.getUser(ctx, username)
	if err != nil {
		return "", recordErr(span, err)
	}

	if user == nil {
		span.SetStatus(codes.Ok, "invalid username")
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

	s.userMu.Lock()
	s.user = &sessionUser{
		dir:  userDir,
		user: user,
	}
	s.userMu.Unlock()

	span.SetStatus(codes.Ok, "auth handled")
	return resp.FormatSimpleString("OK"), nil
}

func (s *session) getUser(ctx context.Context, username string) (*types.User, error) {
	ctx, span := s.tracer.Start(ctx, "getUser") // nolint
	defer span.End()

	userAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		key := s.dirs.user.Pack(tuple.Tuple{username})
		buf, err := tx.Get(key).Get()
		if err != nil {
			return nil, err
		}

		if len(buf) == 0 {
			return nil, nil
		}

		user := &types.User{}
		err = proto.Unmarshal(buf, user)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal user payload: %w", err)
		}

		return user, nil
	})
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	if userAny == nil {
		return nil, nil // user does not exist
	}

	user, err := cast[*types.User](userAny)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.SetStatus(codes.Ok, "got user")
	return user, nil
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
		CreatedAt:    now,
		LastLogin:    now,
		Enabled:      true,
		Rules: []*types.UserACLRule{{
			Level: types.UserAccessLevel_USER_ACCESS_LEVEL_READ_WRITE,
		}},
	}

	userBuf, err := proto.Marshal(user)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to proto marshal user: %w", err))
	}

	usernameKey := s.dirs.user.Pack(tuple.Tuple{username})
	_, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		tx.Set(usernameKey, userBuf)
		return nil, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to create user object: %w", err))
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

	val, err := s.redisGet(args)
	if err != nil {
		return "", recordErr(span, err)
	}

	res := resp.FormatNil()
	if len(val) > 0 {
		res = resp.FormatBulkString(string(val))
	}

	span.SetStatus(codes.Ok, "get handled")
	return res, nil
}

func (s *session) handleExists(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleExists") // nolint
	defer span.End()

	val, err := s.redisGet(args)
	if err != nil {
		return "", recordErr(span, err)
	}

	span.SetStatus(codes.Ok, "exists handled")
	return resp.FormatBoolAsInt(len(val) > 0), nil
}

func (s *session) redisGet(args []resp.Value) ([]byte, error) {
	if err := validateNumArgs(args, 1); err != nil {
		return nil, err
	}

	keyStr, err := extractStringArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument: %w", err)
	}

	key, err := s.userKey(tuple.Tuple{keyStr})
	if err != nil {
		return nil, err
	}

	valAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(key).Get()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get value: %w", err)
	}

	val, err := cast[[]byte](valAny)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (s *session) handleSet(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSet") // nolint
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	keyStr, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	val, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse value argument: %w", err))
	}

	key, err := s.userKey(tuple.Tuple{keyStr})
	if err != nil {
		return "", recordErr(span, err)
	}

	_, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		tx.Set(key, []byte(val))
		return nil, nil
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

	keyStr, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	key, err := s.userKey(tuple.Tuple{keyStr})
	if err != nil {
		return "", recordErr(span, err)
	}

	existsAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		val, err := tx.Get(key).Get()
		if err != nil {
			return false, err
		}

		tx.Clear(key)
		return len(val) > 0, nil
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
