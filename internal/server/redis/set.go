package redis

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	roaring "github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/bluesky-social/go-util/pkg/concurrent"
	"github.com/bluesky-social/kvdb/internal/metrics"
	"github.com/bluesky-social/kvdb/internal/types"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"google.golang.org/protobuf/proto"
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
		numAdded, _, err := s.createOrAddToSet(ctx, tx, objectKindSet, key, members, nil)
		return numAdded, err
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to add members to set: %w", err))
	}

	added, err := cast[uint64](addedAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	metrics.SpanOK(span)
	return resp.FormatUint(added), nil
}

func (s *session) createOrAddToSet(ctx context.Context, tx fdb.Transaction, kind objectKind, key string, members []string, scores []float32) (uint64, []uint64, error) {
	useScores := len(scores) > 0
	if useScores && len(members) != len(scores) {
		return 0, nil, fmt.Errorf("number of members does not match number of scores")
	}

	// look up or allocate a new UID for each set member
	uids := make([]uint64, 0, len(members))
	for ndx, member := range members {
		item := &types.UIDItem{Member: member}
		if useScores {
			item.Score = &scores[ndx]
		}

		uid, err := s.getOrAllocateUID(ctx, tx, item)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to get uid for member: %w", err)
		}
		uids = append(uids, uid)
		item.Uid = uid
	}

	// get the bitmap if it exists
	meta, blob, err := s.getObject(ctx, tx, kind, key)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get existing set: %w", err)
	}

	if meta != nil {
		// ensure that you can't sadd to a sorted set and vice versa
		kindOK := false
		switch meta.Type.(type) {
		case *types.ObjectMeta_Set:
			kindOK = kind == objectKindSet
		case *types.ObjectMeta_SortedSet:
			kindOK = kind == objectKindSortedSet
		}
		if !kindOK {
			return 0, nil, errWrongKind
		}
	}

	// if the key doesn't exist, create a new bitmap
	bitmap := roaring.New()
	if len(blob) > 0 {
		if err := bitmap.UnmarshalBinary(blob); err != nil {
			return 0, nil, fmt.Errorf("failed to unmarshal existing bitmap: %w", err)
		}
	}

	// diff against the existing map to determine how many members are new
	added := uint64(0)
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
		return 0, nil, fmt.Errorf("failed to marshal bitmap: %w", err)
	}

	if err = s.writeObject(ctx, tx, key, kind, data); err != nil {
		return 0, nil, fmt.Errorf("failed to write set: %w", err)
	}

	return added, uids, nil
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

		return s.removeFromSet(ctx, tx, key, uids)
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to remove members from set: %w", err))
	}

	removed, err := cast[uint64](removedAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	metrics.SpanOK(span)
	return resp.FormatUint(removed), nil
}

func (s *session) removeFromSet(ctx context.Context, tx fdb.Transaction, key string, uids []uint64) (uint64, error) {
	ctx, span := s.tracer.Start(ctx, "removeFromSet")
	defer span.End()

	// get the bitmap if it exists
	objMeta, blob, err := s.getObject(ctx, tx, objectKindSet, key)
	if err != nil {
		return uint64(0), fmt.Errorf("failed to get existing set: %w", err)
	}

	// if the bitmap doesn't exist, there's nothing to remove
	if len(blob) == 0 {
		return uint64(0), nil
	}

	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(blob); err != nil {
		return uint64(0), fmt.Errorf("failed to unmarshal existing bitmap: %w", err)
	}

	// remove from the bitmap and count
	removed := uint64(0)
	for _, uid := range uids {
		if bitmap.Contains(uid) {
			bitmap.Remove(uid)
			removed++
		}
	}

	// if the set is now empty, delete the object
	if bitmap.IsEmpty() {
		if err := s.deleteObject(ctx, tx, key, objMeta); err != nil {
			return uint64(0), fmt.Errorf("failed to delete object: %w", err)
		}
		return removed, nil
	}

	// serialize and store the updated bitmap
	data, err := bitmap.MarshalBinary()
	if err != nil {
		return uint64(0), fmt.Errorf("failed to marshal bitmap: %w", err)
	}

	if err = s.writeObject(ctx, tx, key, objectKindSet, data); err != nil {
		return uint64(0), fmt.Errorf("failed to write set: %w", err)
	}

	return removed, nil
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
		_, blob, err := s.getObject(ctx, tx, objectKindSet, key)
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

	metrics.SpanOK(span)
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
		_, blob, err := s.getObject(ctx, tx, objectKindSet, key)
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

	metrics.SpanOK(span)
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
		_, blob, err := s.getObject(ctx, tx, objectKindSet, key)
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
		members, err := r.Do(ctx, uids, func(uid uint64) (member string, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = recoverErr("retreiving members from uids", r)
				}
			}()

			m, err := s.memberFromUID(ctx, tx, uid)
			if err != nil {
				return "", fmt.Errorf("failed to get member from uid: %w", err)
			}

			member = m.Member
			return
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

	metrics.SpanOK(span)
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

	metrics.SpanOK(span)
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

	metrics.SpanOK(span)
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

	metrics.SpanOK(span)
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
			return "", recordErr(span, fmt.Errorf("failed to parse argument %q at index %d: %w", arg, ndx, err))
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

			_, blob, err = s.getObject(ctx, tx, objectKindSet, skey)
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

			m, err := s.memberFromUID(ctx, tx, uid)
			if err != nil {
				return "", fmt.Errorf("failed to get member from uid: %w", err)
			}

			member = m.Member
			return
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

	metrics.SpanOK(span)
	return resp.FormatArrayOfBulkStrings(members), nil
}

func (s *session) handleZAdd(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleZAdd")
	defer span.End()

	if err := validateNumArgs(args, 3); err != nil {
		return "", recordErr(span, err)
	}

	if len(args)%2 != 1 {
		return "", recordErr(span, fmt.Errorf("invalid number of arguments"))
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	var (
		scores  []float32
		members []string
	)
	for ndx := 1; ndx < len(args); ndx += 2 {
		scoreArg := args[ndx]
		scoreStr, err := extractStringArg(scoreArg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse score argument %q at index %d: %w", scoreArg, ndx, err))
		}

		score, err := strconv.ParseFloat(scoreStr, 32)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse score argument %q as float: %w", scoreStr, err))
		}

		memberArg := args[ndx+1]
		member, err := extractStringArg(memberArg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse member argument %q at index %d: %w", memberArg, ndx, err))
		}

		scores = append(scores, float32(score))
		members = append(members, member)
	}

	addedAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		numAdded, uids, err := s.createOrAddToSet(ctx, tx, objectKindSortedSet, key, members, scores)
		if err != nil {
			return uint64(0), err
		}

		scoreDir, err := s.sortedSetScoreDir(key)
		if err != nil {
			return uint64(0), fmt.Errorf("failed to open score dir: %w", err)
		}

		for ndx, score := range scores {
			scoreKey := scoreDir.Pack(tuple.Tuple{encodeSortedSetScore(score)})

			// clear the previous priority value for this item, if any
			item := &types.UIDItem{}
			exists, err := getProtoItem(tx, scoreKey.FDBKey(), item)
			if err != nil {
				return uint64(0), fmt.Errorf("failed to unmarshal uid item: %w", err)
			}
			if exists && item.Score != nil {
				oldScoreKey := scoreDir.Pack(tuple.Tuple{encodeSortedSetScore(*item.Score)})
				tx.Clear(oldScoreKey)
			}

			item = &types.UIDItem{
				Member: members[ndx],
				Uid:    uids[ndx],
				Score:  &score,
			}

			// store the sortable score -> UID mapping
			if err := setProtoItem(tx, scoreKey, item); err != nil {
				return uint64(0), err
			}
		}

		return numAdded, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to add members to set: %w", err))
	}

	added, err := cast[uint64](addedAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	metrics.SpanOK(span)
	return resp.FormatUint(added), nil
}

// Encodes a floating point sorted set score lexicographically so we can store them in sorted order
func encodeSortedSetScore(score float32) string {
	bits := math.Float32bits(score)
	if score >= 0 {
		// flip the sign bit so positive numbers are sorted after negative ones
		bits ^= 0x80000000
	} else {
		// flip all bits so more negative numbers are sorted before less negative numbers
		bits ^= 0xFFFFFFFF
	}

	// big endian ensures correct sort order
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, bits)
	return string(buf)
}

func (s *session) handleZCount(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleZCount") // nolint
	defer span.End()

	if err := validateNumArgs(args, 3); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	startStr, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse start argument: %w", err))
	}
	startRaw, err := strconv.ParseFloat(startStr, 32)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid start argument: %w", err))
	}
	start := encodeSortedSetScore(float32(startRaw))

	endStr, err := extractStringArg(args[2])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse end argument: %w", err))
	}
	endRaw, err := strconv.ParseFloat(endStr, 32)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid end argument: %w", err))
	}
	end := encodeSortedSetScore(float32(endRaw))

	countAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		dir, err := s.sortedSetScoreDir(key)
		if err != nil {
			return uint64(0), fmt.Errorf("failed to get sorted set score dir: %w", err)
		}

		beginRange, endRange := dir.FDBRangeKeys()
		rangeResult := tx.GetRange(
			fdb.KeyRange{
				Begin: beginRange,
				End:   endRange,
			},
			fdb.RangeOptions{},
		)

		count := uint64(0)
		it := rangeResult.Iterator()
		for it.Advance() {
			kv, err := it.Get()
			if err != nil {
				return uint64(0), fmt.Errorf("failed to iterate over key range: %w", err)
			}

			// convert the key tuple to a string
			tup, err := dir.Unpack(kv.Key)
			if err != nil {
				return uint64(0), fmt.Errorf("failed to unpack tuple: %w", err)
			}
			if len(tup) != 1 {
				return uint64(0), fmt.Errorf("invalid tuple length")
			}
			key, err := cast[string](tup[0])
			if err != nil {
				return uint64(0), fmt.Errorf("failed to cast tuple key: %w", err)
			}

			if key < start {
				continue // out of range but not done yet
			}
			if key > end {
				break // out of range and we're done
			}

			count += 1
		}

		return count, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get sorted set count: %w", err))
	}

	count, err := cast[uint64](countAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	metrics.SpanOK(span)
	return resp.FormatUint(count), nil
}

func (s *session) handleZRemRangeByScore(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleZRemRangeByScore") // nolint
	defer span.End()

	if err := validateNumArgs(args, 3); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	startStr, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse start argument: %w", err))
	}
	startRaw, err := strconv.ParseFloat(startStr, 32)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid start argument: %w", err))
	}
	start := encodeSortedSetScore(float32(startRaw))

	endStr, err := extractStringArg(args[2])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse end argument: %w", err))
	}
	endRaw, err := strconv.ParseFloat(endStr, 32)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid end argument: %w", err))
	}
	end := encodeSortedSetScore(float32(endRaw))

	countAny, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		dir, err := s.sortedSetScoreDir(key)
		if err != nil {
			return uint64(0), fmt.Errorf("failed to get sorted set score dir: %w", err)
		}

		beginRange, endRange := dir.FDBRangeKeys()
		rangeResult := tx.GetRange(
			fdb.KeyRange{
				Begin: beginRange,
				End:   endRange,
			},
			fdb.RangeOptions{},
		)

		uids := []uint64{}
		it := rangeResult.Iterator()
		for it.Advance() {
			kv, err := it.Get()
			if err != nil {
				return uint64(0), fmt.Errorf("failed to iterate over key range: %w", err)
			}

			// convert the key tuple to a string
			tup, err := dir.Unpack(kv.Key)
			if err != nil {
				return uint64(0), fmt.Errorf("failed to unpack tuple: %w", err)
			}
			if len(tup) != 1 {
				return uint64(0), fmt.Errorf("invalid tuple length")
			}
			key, err := cast[string](tup[0])
			if err != nil {
				return uint64(0), fmt.Errorf("failed to cast tuple key: %w", err)
			}

			if key < start {
				continue // out of range but not done yet
			}
			if key > end {
				break // out of range and we're done
			}

			// get the UID for the item, which will be deleted in batch later
			item := &types.UIDItem{}
			if err := proto.Unmarshal(kv.Value, item); err != nil {
				return uint64(0), fmt.Errorf("failed to unmarshal uid item: %w", err)
			}
			uids = append(uids, item.Uid)

			// clear the entry from the sorted score directory
			tx.Clear(kv.Key)
		}

		// remove the all items from the set
		return s.removeFromSet(ctx, tx, key, uids)
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get remove from sorted set: %w", err))
	}

	count, err := cast[uint64](countAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	metrics.SpanOK(span)
	return resp.FormatUint(count), nil
}
