package redis

import (
	"context"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/bluesky-social/kvdb/internal/types"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"
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

	if err := bcrypt.CompareHashAndPassword(user.PasswordHash, []byte(pass)); err != nil {
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
		dir: userDir,
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

		return nil, nil
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
