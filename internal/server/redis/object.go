package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel/codes"
)

func (s *session) handleGet(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleGet")
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", fmt.Errorf("failed to parse argument: %w", err)
	}

	bufAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		_, buf, err := s.getBasicObject(ctx, tx, key)
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
	ctx, span := s.tracer.Start(ctx, "handleExists")
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", fmt.Errorf("failed to parse argument: %w", err)
	}

	existsAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		_, meta, err := s.getMeta(ctx, tx, key)
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
	ctx, span := s.tracer.Start(ctx, "handleSet")
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
		return nil, s.writeBasicObject(ctx, tx, key, []byte(val))
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to set value: %w", err))
	}

	span.SetStatus(codes.Ok, "set handled")
	return resp.FormatSimpleString("OK"), nil
}

func (s *session) handleDelete(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleDelete")
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	existsAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		_, meta, err := s.getMeta(ctx, tx, key)
		if err != nil {
			return false, err
		}
		if meta == nil {
			return false, nil // object does not exist
		}

		err = s.deleteBasicObject(ctx, tx, key, meta)
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
	ctx, span := s.tracer.Start(ctx, "handleIncrDecr")
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
		_, buf, err := s.getBasicObject(ctx, tx, key)
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
		if err = s.writeBasicObject(ctx, tx, key, buf); err != nil {
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
	ctx, span := s.tracer.Start(ctx, "handleIncrByFloat")
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
		_, buf, err := s.getBasicObject(ctx, tx, key)
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
		if err = s.writeBasicObject(ctx, tx, key, buf); err != nil {
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
