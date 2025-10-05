package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/kvdb/internal/metrics"
	"github.com/bluesky-social/kvdb/internal/types"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"github.com/jcalabro/gt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type objectKind int64

const (
	objectKindBasic objectKind = 1 << iota
	objectKindSet
	objectKindList
	objectKindListItem
)

var (
	errWrongKind = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

func getNumChunks(meta *types.ObjectMeta, kind gt.Option[objectKind]) (uint32, error) {
	var (
		numChunks uint32
		objKind   objectKind
	)

	switch typ := meta.Type.(type) {
	case *types.ObjectMeta_Basic:
		objKind = objectKindBasic
		numChunks = typ.Basic.NumChunks
	case *types.ObjectMeta_Set:
		objKind = objectKindSet
		numChunks = typ.Set.NumChunks
	case *types.ObjectMeta_ListItem:
		objKind = objectKindListItem
		numChunks = typ.ListItem.NumChunks
	default:
		return 0, fmt.Errorf("object of type %T cannot be retrieved by getObject", meta.Type)
	}

	if kind.HasVal() {
		if kind.Val() != objKind {
			return 0, errWrongKind
		}
	}

	return numChunks, nil
}

func (s *session) getObject(ctx context.Context, tx fdb.ReadTransaction, kind objectKind, id string) (*types.ObjectMeta, []byte, error) {
	ctx, span := s.tracer.Start(ctx, "getObject")
	defer span.End()

	_, meta, err := s.getObjectMeta(ctx, tx, id)
	if err != nil {
		span.RecordError(err)
		return nil, nil, err
	}
	if meta == nil {
		metrics.SpanOK(span)
		return nil, nil, nil
	}

	// @TODO (jrc): update last_accessed out of band

	if meta.Expires != nil {
		if time.Now().After(meta.Expires.AsTime()) {
			metrics.SpanOK(span)
			return nil, nil, nil
		}
	}

	numChunks, err := getNumChunks(meta, gt.Some(kind))
	if err != nil {
		span.RecordError(err)
		return nil, nil, err
	}

	// @TODO (jrc): read all chunks in parallel
	buf := []byte{}
	for ndx := range numChunks {
		chunkKey, err := s.objectKey(id, ndx)
		if err != nil {
			span.RecordError(err)
			return nil, nil, fmt.Errorf("failed to get object chunk key: %w", err)
		}

		chunk, err := tx.Get(chunkKey).Get()
		if err != nil {
			span.RecordError(err)
			return nil, nil, fmt.Errorf("failed to get object chunk: %w", err)
		}

		buf = append(buf, chunk...)
	}

	metrics.SpanOK(span)
	return meta, buf, nil
}

func (s *session) writeObject(ctx context.Context, tx fdb.Transaction, id string, kind objectKind, data []byte) error {
	ctx, span := s.tracer.Start(ctx, "writeObject")
	defer span.End()

	now := timestamppb.Now()

	// check if the object already exists and should be overwritten
	metaKey, meta, err := s.getObjectMeta(ctx, tx, id)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if meta != nil {
		// delete then update existing
		if err := s.deleteObject(ctx, tx, id, meta); err != nil {
			span.RecordError(err)
			return err
		}
	} else {
		// create new
		meta = &types.ObjectMeta{
			Created: now,
		}
		switch kind {
		case objectKindBasic:
			meta.Type = &types.ObjectMeta_Basic{Basic: &types.BasicObjectMeta{}}
		case objectKindSet:
			meta.Type = &types.ObjectMeta_Set{Set: &types.SetMeta{}}
		case objectKindListItem:
			meta.Type = &types.ObjectMeta_ListItem{ListItem: &types.ListItemMeta{}}
		}
	}

	meta.Updated = now
	meta.LastAccess = now

	// set the new number of chunks
	const maxValBytes = 100_000
	length := uint32(len(data))
	numChunks := (length / maxValBytes) + 1
	if length%maxValBytes == 0 {
		numChunks = length / maxValBytes
	}

	switch typ := meta.Type.(type) {
	case *types.ObjectMeta_Basic:
		typ.Basic.NumChunks = numChunks
	case *types.ObjectMeta_Set:
		typ.Set.NumChunks = numChunks
	case *types.ObjectMeta_ListItem:
		typ.ListItem.NumChunks = numChunks
	}

	// write the meta object
	metaBuf, err := proto.Marshal(meta)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to proto marshal object meta: %w", err)
	}
	tx.Set(metaKey, metaBuf)

	// write all object data chunks
	for ndx := range numChunks {
		start := ndx * maxValBytes
		end := min((ndx+1)*maxValBytes, length)

		chunkKey, err := s.objectKey(id, ndx)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to get object chunk key: %w", err)
		}

		tx.Set(chunkKey, data[start:end])
	}

	metrics.SpanOK(span)
	return nil
}

func (s *session) deleteObject(ctx context.Context, tx fdb.Transaction, id string, meta *types.ObjectMeta) error {
	ctx, span := s.tracer.Start(ctx, "deleteObject") // nolint
	defer span.End()

	// delete the meta object
	metaKey, err := s.metaKey(id)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to get meta key: %w", err)
	}
	tx.Clear(metaKey)

	numChunks, err := getNumChunks(meta, gt.None[objectKind]())
	if err != nil {
		span.RecordError(err)
		return err
	}

	// clear all object chunks
	begin, err := s.objectKey(id, 0)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to get object begin key: %w", err)
	}
	end, err := s.objectKey(id, numChunks-1)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to get object start key: %w", err)
	}
	tx.ClearRange(fdb.KeyRange{
		Begin: begin,
		End:   end,
	})

	metrics.SpanOK(span)
	return nil
}

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
		_, buf, err := s.getObject(ctx, tx, objectKindBasic, key)
		return buf, err
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get object: %w", err))
	}

	buf, err := cast[[]byte](bufAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	res := resp.FormatNil()
	if len(buf) > 0 {
		res = resp.FormatBulkString(string(buf))
	}

	metrics.SpanOK(span)
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
		_, meta, err := s.getObjectMeta(ctx, tx, key)
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
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	metrics.SpanOK(span)
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
		return nil, s.writeObject(ctx, tx, key, objectKindBasic, []byte(val))
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to set value: %w", err))
	}

	metrics.SpanOK(span)
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
		_, meta, err := s.getObjectMeta(ctx, tx, key)
		if err != nil {
			return false, err
		}
		if meta == nil {
			return false, nil // object does not exist
		}

		err = s.deleteObject(ctx, tx, key, meta)
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

	metrics.SpanOK(span)
	return resp.FormatBoolAsInt(exists), nil
}

func (s *session) handleIncr(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleIncr")
	defer span.End()

	res, err := s.handleIncrDecr(ctx, args, "1")
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to write value: %w", err))
	}

	metrics.SpanOK(span)
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

	metrics.SpanOK(span)
	return resp.FormatInt(res), nil
}

func (s *session) handleDecr(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleDecr")
	defer span.End()

	res, err := s.handleIncrDecr(ctx, args, "-1")
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to write value: %w", err))
	}

	metrics.SpanOK(span)
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

	metrics.SpanOK(span)
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
		_, buf, err := s.getObject(ctx, tx, objectKindBasic, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get existing value: %w", err)
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
		if err = s.writeObject(ctx, tx, key, objectKindBasic, buf); err != nil {
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

	metrics.SpanOK(span)
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
		_, buf, err := s.getObject(ctx, tx, objectKindBasic, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get existing value: %w", err)
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
		if err = s.writeObject(ctx, tx, key, objectKindBasic, buf); err != nil {
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

	metrics.SpanOK(span)
	return resp.FormatBulkString(string(res)), nil
}

func (s *session) handleExpire(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleExpire")
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	secsStr, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse expire seconds argument: %w", err))
	}
	secs, err := strconv.ParseInt(secsStr, 10, 64)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid expire seconds %q: %w", secsStr, err))
	}
	delta := time.Duration(secs) * time.Second

	resAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		metaKey, meta, err := s.getObjectMeta(ctx, tx, key)
		if err != nil {
			return 0, fmt.Errorf("failed to get object meta: %w", err)
		}
		if meta == nil {
			// no item exists, no expiry will be set
			return 0, nil
		}

		now := timestamppb.Now().AsTime()
		meta.Expires = timestamppb.New(now.Add(delta))

		if err := setProtoItem(tx, metaKey, meta); err != nil {
			return 0, fmt.Errorf("failed to write object meta: %w", err)
		}

		return 1, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to delete value: %w", err))
	}

	res, err := cast[int](resAny)
	if err != nil {
		return "", recordErr(span, recordErr(span, err))
	}

	metrics.SpanOK(span)
	return resp.FormatInt(int64(res)), nil
}
