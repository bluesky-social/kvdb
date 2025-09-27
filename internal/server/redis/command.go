package redis

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
)

func (s *session) handlePing(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handlePing") // nolint
	defer span.End()

	if len(args) > 1 {
		return "", recordErr(span, fmt.Errorf("incorrect number of arguments for ping"))
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

	return res, nil
}

func (s *session) handleGet(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleGet") // nolint
	defer span.End()

	val, err := s.redisGet(args)
	if err != nil {
		return "", recordErr(span, err)
	}

	if len(val) == 0 {
		// not found
		return resp.FormatNil(), nil
	}

	return resp.FormatBulkString(string(val)), nil
}

func (s *session) handleExists(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleExists") // nolint
	defer span.End()

	val, err := s.redisGet(args)
	if err != nil {
		return "", recordErr(span, err)
	}

	return resp.FormatBoolAsInt(len(val) > 0), nil
}

func (s *session) redisGet(args []resp.Value) ([]byte, error) {
	if err := validateNumArgs(args, 1); err != nil {
		return nil, err
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument: %w", err)
	}

	res, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(fdb.Key(key)).Get()
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

func (s *session) handleSet(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSet") // nolint
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", err
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
		tx.Set(fdb.Key(key), []byte(val))
		return nil, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to set value: %w", err))
	}

	return resp.FormatSimpleString("OK"), nil
}

func (s *session) handleDelete(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleDelete") // nolint
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", err
	}

	keyStr, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}
	key := fdb.Key(keyStr)

	res, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
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

	exists, ok := res.(bool)
	if !ok {
		return "", recordErr(span, fmt.Errorf("failed to cast result type %T to a bool", res))
	}

	return resp.FormatBoolAsInt(exists), nil
}
