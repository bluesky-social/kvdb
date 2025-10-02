package redis

import (
	"context"
	"fmt"

	roaring "github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/go-util/pkg/concurrent"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel/codes"
)

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

	members, err := parseVariadicArguments(args)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set value argument(s): %w", err))
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

			return s.getOrAllocateUID(ctx, tx, member)
		})
		if err != nil {
			return nil, recordErr(span, fmt.Errorf("failed to get uids for members: %w", err))
		}

		// get the bitmap if it exists
		_, blob, err := s.getObject(ctx, tx, key)
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

		if err = s.writeObject(ctx, tx, key, data); err != nil {
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

	members, err := parseVariadicArguments(args)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set value argument(s): %w", err))
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

			return s.peekUID(ctx, tx, member)
		})
		if err != nil {
			return nil, recordErr(span, fmt.Errorf("failed to get uids for members: %w", err))
		}

		// get the bitmap if it exists
		objMeta, blob, err := s.getObject(ctx, tx, key)
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
			if err := s.deleteObject(ctx, tx, key, objMeta); err != nil {
				return int64(0), fmt.Errorf("failed to delete object: %w", err)
			}
			return removed, nil
		}

		// serialize and store the updated bitmap
		data, err := bitmap.MarshalBinary()
		if err != nil {
			return int64(0), fmt.Errorf("failed to marshal bitmap: %w", err)
		}

		if err = s.writeObject(ctx, tx, key, data); err != nil {
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
	ctx, span := s.tracer.Start(ctx, "handleSetIsMember")
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

	type result struct {
		memberUID uint64
		blob      []byte
	}

	resAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		// lookup the UID for the member
		memberUID, err := s.peekUID(ctx, tx, member)
		if err != nil {
			return nil, recordErr(span, fmt.Errorf("failed to get uid for member: %w", err))
		}

		// get the bitmap if it exists
		_, blob, err := s.getObject(ctx, tx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get existing set: %w", err)
		}

		return &result{
			memberUID: memberUID,
			blob:      blob,
		}, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get set from the database: %w", err))
	}

	res, err := cast[*result](resAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	isMember := false
	if len(res.blob) > 0 {
		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(res.blob); err != nil {
			return "", recordErr(span, fmt.Errorf("failed to unmarshal existing bitmap: %w", err))
		}

		isMember = bitmap.Contains(res.memberUID)
	}

	span.SetStatus(codes.Ok, "sismember handled")
	return resp.FormatBoolAsInt(isMember), nil
}

func (s *session) handleSetCard(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetCard")
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	blobAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		// get the bitmap if it exists
		_, blob, err := s.getObject(ctx, tx, key)
		if err != nil {
			return uint64(0), fmt.Errorf("failed to get existing set: %w", err)
		}
		return blob, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to check if member is in set: %w", err))
	}

	blob, err := cast[[]byte](blobAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	cardinality := int64(0)
	if len(blob) > 0 {
		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(blob); err != nil {
			return "", recordErr(span, fmt.Errorf("failed to unmarshal existing bitmap: %w", err))
		}

		cardinality = int64(bitmap.GetCardinality())
	}

	span.SetStatus(codes.Ok, "scard handled")
	return resp.FormatInt(cardinality), nil
}

func (s *session) handleSetMembers(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetMembers")
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	membersAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		// get the bitmap if it exists
		_, blob, err := s.getObject(ctx, tx, key)
		if err != nil {
			return []string{}, fmt.Errorf("failed to get existing set: %w", err)
		}

		// if the bitmap doesn't exist, the member does not exist in the set
		if len(blob) == 0 {
			return []string{}, nil
		}

		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(blob); err != nil {
			return []string{}, fmt.Errorf("failed to unmarshal existing bitmap: %w", err)
		}

		uids := bitmap.ToArray()

		r := concurrent.New[uint64, string]()
		members, err := r.Do(ctx, uids, func(uid uint64) (string, error) {
			defer func() {
				if r := recover(); r != nil {
					err = recoverErr("retreiving members from uids", r)
				}
			}()

			return s.memberFromUID(ctx, tx, uid)
		})
		if err != nil {
			return []string{}, fmt.Errorf("failed to get member from uid: %w", err)
		}

		return members, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to check if member is in set: %w", err))
	}

	members, err := cast[[]string](membersAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	span.SetStatus(codes.Ok, "smembers handled")
	return resp.FormatArrayOfBulkStrings(members), nil
}

func (s *session) handleSetInter(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetInter")
	defer span.End()

	resp, err := s.handleMultiSetOperation(ctx, args, func(bitmaps []*roaring.Bitmap) *roaring.Bitmap {
		// intersect all bitmaps
		resultBitmap := bitmaps[0]
		for _, bitmap := range bitmaps[1:] {
			if bitmap == nil || bitmap.IsEmpty() {
				// intersection with empty is empty
				return roaring.New()
			}
			resultBitmap.And(bitmap)
		}
		return resultBitmap
	})
	if err != nil {
		return "", err
	}

	span.SetStatus(codes.Ok, "sinter handled")
	return resp, nil
}

func (s *session) handleSetUnion(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetUnion")
	defer span.End()

	resp, err := s.handleMultiSetOperation(ctx, args, func(bitmaps []*roaring.Bitmap) *roaring.Bitmap {
		// union all bitmaps
		resultBitmap := bitmaps[0]
		for _, bitmap := range bitmaps[1:] {
			resultBitmap.Or(bitmap)
		}
		return resultBitmap
	})
	if err != nil {
		return "", err
	}

	span.SetStatus(codes.Ok, "sunion handled")
	return resp, nil
}

func (s *session) handleSetDiff(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetDiff")
	defer span.End()

	resp, err := s.handleMultiSetOperation(ctx, args, func(bitmaps []*roaring.Bitmap) *roaring.Bitmap {
		// diff all bitmaps
		resultBitmap := bitmaps[0]
		for _, bitmap := range bitmaps[1:] {
			resultBitmap.AndNot(bitmap)
		}
		return resultBitmap
	})
	if err != nil {
		return "", err
	}

	span.SetStatus(codes.Ok, "sdiff handled")
	return resp, nil
}

type setOperationFunc func([]*roaring.Bitmap) *roaring.Bitmap

func (s *session) handleMultiSetOperation(ctx context.Context, args []resp.Value, fn setOperationFunc) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleMultiSetOperation")
	defer span.End()

	if err := validateNumArgs(args, 1); err != nil {
		return "", recordErr(span, err)
	}

	var setKeys []string
	for ndx, arg := range args {
		setKey, err := extractStringArg(arg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse set key argument at index %d: %w", ndx, err))
		}
		setKeys = append(setKeys, setKey)
	}

	membersAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		r := concurrent.New[string, []byte]()
		blobs, err := r.Do(ctx, setKeys, func(skey string) (blob []byte, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = recoverErr("retreiving members from uids", r)
				}
			}()

			_, blob, err = s.getObject(ctx, tx, skey)
			return
		})
		if err != nil {
			return []string{}, fmt.Errorf("failed to get member from uid: %w", err)
		}

		// unmarshal all bitmaps
		r2 := concurrent.New[[]byte, *roaring.Bitmap]()
		bitmaps, err := r2.Do(ctx, blobs, func(blob []byte) (bitmap *roaring.Bitmap, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = recoverErr("unmarshalling bitmaps", r)
				}
			}()

			bitmap = roaring.New()
			if len(blob) == 0 {
				return
			}

			if err := bitmap.UnmarshalBinary(blob); err != nil {
				return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
			}
			return
		})
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to unmarshal bitmaps: %w", err))
		}

		// call the user-provided function to execute some logic on the list of bitmaps
		resultBitmap := fn(bitmaps)

		// convert UIDs back to members
		uids := resultBitmap.ToArray()
		r3 := concurrent.New[uint64, string]()
		members, err := r3.Do(ctx, uids, func(uid uint64) (member string, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = recoverErr("retreiving members from uids", r)
				}
			}()

			return s.memberFromUID(ctx, tx, uid)
		})
		if err != nil {
			return nil, recordErr(span, fmt.Errorf("failed to get members for UIDs: %w", err))
		}

		return members, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to check if member is in set: %w", err))
	}

	members, err := cast[[]string](membersAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	span.SetStatus(codes.Ok, "multi-set operation handled")
	return resp.FormatArrayOfBulkStrings(members), nil
}
