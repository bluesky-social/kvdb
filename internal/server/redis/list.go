package redis

import (
	"context"
	"fmt"
	"math"
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
		return "", fmt.Errorf("failed to parse argument: %w", err)
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

	span.SetStatus(codes.Ok, "lpush handled")
	return resp.FormatUint(num), nil
}

func (s *session) handleLPush(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleLPush")
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", fmt.Errorf("failed to parse argument: %w", err)
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
				Created:   now,
				ItemStart: math.MaxUint64 / 2,
			}
		}

		// assign slots for these new list items and create object blobs for each
		for _, member := range members {
			listMeta.ItemStart -= 1
			listMeta.NumItems += 1

			objKey := strconv.FormatUint(listMeta.ItemStart, 10)
			if err := s.writeObject(ctx, tx, objKey, []byte(member)); err != nil {
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

	span.SetStatus(codes.Ok, "lpush handled")
	return resp.FormatInt(int64(len(members))), nil
}
