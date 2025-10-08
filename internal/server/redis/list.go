package redis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/kvdb/internal/metrics"
	"github.com/bluesky-social/kvdb/internal/types"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
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

		return listMeta.NumItems, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get value: %w", err))
	}

	num, err := cast[uint64](numAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	metrics.SpanOK(span)
	return resp.FormatUint(num), nil
}

func (s *session) handleLPush(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleLPush")
	defer span.End()

	res, err := s.handlePush(ctx, args, true)
	if err != nil {
		span.RecordError(err)
		return res, err
	}

	metrics.SpanOK(span)
	return res, err
}

func (s *session) handleRPush(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRPush")
	defer span.End()

	res, err := s.handlePush(ctx, args, false)
	if err != nil {
		span.RecordError(err)
		return res, err
	}

	metrics.SpanOK(span)
	return res, err
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
		listMetaKey, objMeta, err := s.getMeta(ctx, tx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get list meta: %w", err)
		}

		now := timestamppb.Now()
		if objMeta == nil {
			// create a new list object
			objMeta = &types.ObjectMeta{
				Created: now,
				Type:    &types.ObjectMeta_List{List: &types.ListMeta{}},
			}
		}
		objMeta.Updated = now

		objMetaList, err := cast[*types.ObjectMeta_List](objMeta.Type)
		if err != nil {
			return nil, err
		}
		listMeta := objMetaList.List

		// create each item meta object and store the blob itself
		for _, member := range members {
			listMeta.NumItems += 1

			// allocated a UID for this list item
			itemIDInt, err := s.allocateNewUID(ctx, tx)
			if err != nil {
				return nil, fmt.Errorf("failed to allocate new uid: %w", err)
			}
			itemID := strconv.FormatUint(itemIDInt, 10)

			listItemMeta := &types.ListItemMeta{}
			if left {
				headKey, err := s.listItemMetaKey(key, listMeta.ItemHead)
				if err != nil {
					return nil, fmt.Errorf("failed to get list head key: %w", err)
				}

				headObj := &types.ObjectMeta{}
				exists, err := getProtoItem(tx, headKey, headObj)
				if err != nil {
					return nil, fmt.Errorf("failed to get list head: %w", err)
				}

				if !exists {
					// we're creating a new list, so assign the tail in addition to the head
					listMeta.ItemTail = itemID
				} else {
					// assign back pointers if a previous list head exists
					head, err := cast[*types.ObjectMeta_ListItem](headObj.Type)
					if err != nil {
						return nil, err
					}
					head.ListItem.Previous = itemID

					if err := setProtoItem(tx, headKey, headObj); err != nil {
						return nil, fmt.Errorf("failed to set list head: %w", err)
					}
				}

				// assign forwards pointers
				listItemMeta.Next = listMeta.ItemHead
				listMeta.ItemHead = itemID
			} else {
				tailKey, err := s.listItemMetaKey(key, listMeta.ItemTail)
				if err != nil {
					return nil, fmt.Errorf("failed to get list tail key: %w", err)
				}

				tailObj := &types.ObjectMeta{}
				exists, err := getProtoItem(tx, tailKey, tailObj)
				if err != nil {
					return nil, fmt.Errorf("failed to get list tail: %w", err)
				}

				if !exists {
					// we're creating a new list, so assign the head in addition to the tail
					listMeta.ItemHead = itemID
				} else {
					// assign forwards pointers if a previous list tail exists
					tail, err := cast[*types.ObjectMeta_ListItem](tailObj.Type)
					if err != nil {
						return nil, err
					}
					tail.ListItem.Next = itemID

					if err := setProtoItem(tx, tailKey, tailObj); err != nil {
						return nil, fmt.Errorf("failed to set list tail: %w", err)
					}
				}

				// assign back pointers
				listItemMeta.Previous = listMeta.ItemTail
				listMeta.ItemTail = itemID
			}

			// save the list item meta object
			itemMetaKey, err := s.listItemMetaKey(key, itemID)
			if err != nil {
				return nil, fmt.Errorf("failed to get list item meta key: %w", err)
			}

			listObjMeta := &types.ObjectMeta{
				Created: now,
				Updated: now,
				Type: &types.ObjectMeta_ListItem{
					ListItem: listItemMeta,
				},
			}

			if err := setProtoItem(tx, itemMetaKey, listObjMeta); err != nil {
				return nil, fmt.Errorf("failed to write list item meta: %w", err)
			}

			// save the list item object blob
			if err := s.writeObject(ctx, tx, itemID, objectKindListItem, []byte(member)); err != nil {
				return nil, fmt.Errorf("failed to create list item: %w", err)
			}
		}

		// save the full list object meta
		if err := setProtoItem(tx, listMetaKey, objMeta); err != nil {
			return nil, fmt.Errorf("failed to write list object meta: %w", err)
		}

		return nil, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get value: %w", err))
	}

	metrics.SpanOK(span)
	return resp.FormatInt(int64(len(members))), nil
}

func (s *session) handleLIndex(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleLIndex")
	defer span.End()

	if err := validateNumArgs(args, 2); err != nil {
		return "", recordErr(span, err)
	}

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse list key argument: %w", err))
	}

	indexStr, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse list index argument: %w", err))
	}
	index, err := strconv.ParseInt(indexStr, 10, 64)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse list index argument: %w", err))
	}

	bufAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		_, listMeta, err := s.getListMeta(ctx, tx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get list meta: %w", err)
		}

		if listMeta == nil {
			return nil, nil
		}

		targetNdx := index
		if targetNdx < 0 {
			// wrap around if negative (using addition because targetNdx is already negative)
			targetNdx = int64(listMeta.NumItems) + targetNdx
		}

		objKey := listMeta.ItemHead
		for targetNdx > 0 && objKey != "" {
			itemMetaKey, err := s.listItemMetaKey(key, objKey)
			if err != nil {
				return nil, fmt.Errorf("failed to get list item meta: %w", err)
			}

			objMeta := &types.ObjectMeta{}
			ok, err := getProtoItem(tx, itemMetaKey, objMeta)
			if err != nil {
				return nil, fmt.Errorf("failed to get list object meta: %w", err)
			}
			if !ok {
				return nil, fmt.Errorf("list object meta not found")
			}

			listItemMeta, err := cast[*types.ObjectMeta_ListItem](objMeta.Type)
			if err != nil {
				return nil, err
			}

			targetNdx -= 1
			objKey = listItemMeta.ListItem.Next
		}

		_, buf, err := s.getObject(ctx, tx, objectKindListItem, objKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get object payload: %w", err)
		}
		if len(buf) == 0 {
			return nil, nil
		}
		return buf, nil
	})
	if err != nil {
		return "", recordErr(span, err)
	}

	if bufAny == nil {
		metrics.SpanOK(span)
		return resp.FormatNil(), nil
	}

	buf, err := cast[[]byte](bufAny)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
	}

	metrics.SpanOK(span)
	return resp.FormatBulkString(string(buf)), nil
}
