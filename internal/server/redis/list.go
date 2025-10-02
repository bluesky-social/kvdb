package redis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/kvdb/internal/types"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *session) handleLLen(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleLLen")
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse list key argument: %w", err))
	}

	numAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		_, listMeta, err := s.getListMeta(ctx, tx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get list meta: %w", err)
		}

		if listMeta == nil {
			return uint64(0), nil
		}

		// @TODO (jrc): update last_accessed out of band

		return listMeta.NumItems, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get value: %w", err))
	}

	num, err := cast[uint64](numAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	span.SetStatus(codes.Ok, "llen handled")
	return resp.FormatUint(num), nil
}

func (s *session) handleLPush(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleLPush")
	defer span.End()

	span.SetStatus(codes.Ok, "lpush handled")
	return s.handlePush(ctx, args, true)
}

func (s *session) handleRPush(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRPush")
	defer span.End()

	span.SetStatus(codes.Ok, "rpush handled")
	return s.handlePush(ctx, args, false)
}

func (s *session) handlePush(ctx context.Context, args []resp.Value, left bool) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handlePush")
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse list key argument: %w", err))
	}

	members, err := parseVariadicArguments(args)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse list value argument(s): %w", err))
	}

	_, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		metaKey, listMeta, err := s.getListMeta(ctx, tx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get list meta: %w", err)
		}

		now := timestamppb.Now()
		if listMeta == nil {
			// create a new list
			listMeta = &types.ListMeta{
				Created: now,
			}
		}

		// create each item meta object and store the blob itself
		for _, member := range members {
			objIDInt, err := s.allocateNewUID(ctx, tx)
			if err != nil {
				return nil, fmt.Errorf("failed to allocate new uid: %w", err)
			}
			objID := strconv.FormatUint(objIDInt, 10)

			objMeta := &types.ListObjectMeta{
				Created: now,
				Updated: now,
				Id:      objID,
			}

			listMeta.NumItems += 1
			if left {
				objMeta.Next = listMeta.ItemHead
				listMeta.ItemHead = objID
			} else {
				objMeta.Previous = listMeta.ItemTail
				listMeta.ItemTail = objID
			}

			objMetaKey, err := s.listObjMetaKey(key, objID)
			if err != nil {
				return nil, fmt.Errorf("failed to get list object meta key: %w", err)
			}

			if err := s.setProtoItem(objMetaKey, objMeta); err != nil {
				return nil, fmt.Errorf("failed to write list object meta: %w", err)
			}

			if err := s.writeObject(ctx, tx, objID, []byte(member)); err != nil {
				return nil, fmt.Errorf("failed to create object: %w", err)
			}
		}

		// update and save our list meta
		listMeta.Updated = now
		listMeta.LastAccess = now

		metaBuf, err := proto.Marshal(listMeta)
		if err != nil {
			return nil, fmt.Errorf("failed to proto marshal list meta: %w", err)
		}
		tx.Set(metaKey, metaBuf)

		return nil, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get value: %w", err))
	}

	span.SetStatus(codes.Ok, "push handled")
	return resp.FormatInt(int64(len(members))), nil
}
