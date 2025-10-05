package redis

// import (
// 	"context"
// 	"fmt"
// 	"strconv"

// 	"github.com/apple/foundationdb/bindings/go/src/fdb"
// 	"github.com/bluesky-social/kvdb/internal/types"
// 	"github.com/bluesky-social/kvdb/pkg/serde/resp"
// 	"go.opentelemetry.io/otel/codes"
// 	"google.golang.org/protobuf/proto"
// 	"google.golang.org/protobuf/types/known/timestamppb"
// )

// func (s *session) handleLLen(ctx context.Context, args []resp.Value) (string, error) {
// 	ctx, span := s.tracer.Start(ctx, "handleLLen")
// 	defer span.End()

// 	if err := validateNumArgs(args, 1); err != nil {
// 		return "", recordErr(span, err)
// 	}

// 	key, err := extractStringArg(args[0])
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("failed to parse list key argument: %w", err))
// 	}

// 	numAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
// 		_, listMeta, err := s.getListMeta(ctx, tx, key)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to get list meta: %w", err)
// 		}

// 		if listMeta == nil {
// 			return uint64(0), nil
// 		}

// 		// @TODO (jrc): update last_accessed out of band

// 		return listMeta.NumItems, nil
// 	})
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("failed to get value: %w", err))
// 	}

// 	num, err := cast[uint64](numAny)
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
// 	}

// 	span.SetStatus(codes.Ok, "llen handled")
// 	return resp.FormatUint(num), nil
// }

// func (s *session) handleLPush(ctx context.Context, args []resp.Value) (string, error) {
// 	ctx, span := s.tracer.Start(ctx, "handleLPush")
// 	defer span.End()

// 	span.SetStatus(codes.Ok, "lpush handled")
// 	return s.handlePush(ctx, args, true)
// }

// func (s *session) handleRPush(ctx context.Context, args []resp.Value) (string, error) {
// 	ctx, span := s.tracer.Start(ctx, "handleRPush")
// 	defer span.End()

// 	span.SetStatus(codes.Ok, "rpush handled")
// 	return s.handlePush(ctx, args, false)
// }

// func (s *session) handlePush(ctx context.Context, args []resp.Value, left bool) (string, error) {
// 	ctx, span := s.tracer.Start(ctx, "handlePush")
// 	defer span.End()

// 	if err := validateNumArgs(args, 2); err != nil {
// 		return "", recordErr(span, err)
// 	}

// 	key, err := extractStringArg(args[0])
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("failed to parse list key argument: %w", err))
// 	}

// 	members, err := parseVariadicArguments(args)
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("failed to parse list value argument(s): %w", err))
// 	}

// 	_, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
// 		metaKey, objMeta, err := s.getObjectMeta(ctx, tx, key)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to get list meta: %w", err)
// 		}

// 		now := timestamppb.Now()
// 		if objMeta == nil {
// 			// create a new list
// 			objMeta = &types.ObjectMeta{
// 				Created: now,
// 				Type: &types.ObjectMeta_List{
// 					List: &types.ListMeta{},
// 				},
// 			}
// 		}

// 		listMeta, ok := objMeta.Type.(*types.ObjectMeta_List)
// 		if !ok {
// 			return nil, fmt.Errorf("object is not a list")
// 		}

// 		// create each item meta object and store the blob itself
// 		for _, member := range members {
// 			objIDInt, err := s.allocateNewUID(ctx, tx)
// 			if err != nil {
// 				return nil, fmt.Errorf("failed to allocate new uid: %w", err)
// 			}
// 			objID := strconv.FormatUint(objIDInt, 10)

// 			listObjMeta := &types.ObjectMeta{
// 				Created: now,
// 				Updated: now,
// 				Type: &types.ObjectMeta_ListItem{
// 					ListItem: &types.ListItemMeta{},
// 				},
// 			}

// 			listMeta.List.NumItems += 1
// 			if left {
// 				// assign back pointers if a previous list head exists
// 				headKey, err := s.listObjMetaKey(key, listMeta.List.ItemHead.ItemHead)
// 				if err != nil {
// 					return nil, fmt.Errorf("failed to get list head key: %w", err)
// 				}

// 				head := &types.ListObjectMeta{}
// 				exists, err := s.getProtoItem(headKey, head)
// 				if err != nil {
// 					return nil, fmt.Errorf("failed to get list head: %w", err)
// 				}
// 				if exists {
// 					head.Previous = objID
// 					if err := s.setProtoItem(headKey, head); err != nil {
// 						return nil, fmt.Errorf("failed to set list head: %w", err)
// 					}
// 				} else {
// 					// we're creating a new list, so assign both the head and tail
// 					listMeta.ItemTail = objID
// 				}

// 				// assign forwards pointers
// 				listObjMeta.Next = listMeta.ItemHead
// 				listMeta.ItemHead = objID
// 			} else {
// 				// assign forwards pointers if a previous list tail exists
// 				tailKey, err := s.listObjMetaKey(key, listMeta.ItemTail)
// 				if err != nil {
// 					return nil, fmt.Errorf("failed to get list tail key: %w", err)
// 				}

// 				tail := &types.ListObjectMeta{}
// 				exists, err := s.getProtoItem(tailKey, tail)
// 				if err != nil {
// 					return nil, fmt.Errorf("failed to get list tail: %w", err)
// 				}
// 				if exists {
// 					tail.Next = objID
// 					if err := s.setProtoItem(tailKey, tail); err != nil {
// 						return nil, fmt.Errorf("failed to set list tail: %w", err)
// 					}
// 				} else {
// 					// we're creating a new list, so assign both the head and tail
// 					listMeta.ItemHead = objID
// 				}

// 				// assign back pointers
// 				listObjMeta.Previous = listMeta.ItemTail
// 				listMeta.ItemTail = objID
// 			}

// 			objMetaKey, err := s.listObjMetaKey(key, objID)
// 			if err != nil {
// 				return nil, fmt.Errorf("failed to get list object meta key: %w", err)
// 			}

// 			if err := s.setProtoItem(objMetaKey, listObjMeta); err != nil {
// 				return nil, fmt.Errorf("failed to write list object meta: %w", err)
// 			}

// 			if err := s.writeObject(ctx, tx, objID, []byte(member)); err != nil {
// 				return nil, fmt.Errorf("failed to create object: %w", err)
// 			}
// 		}

// 		// update and save our list meta
// 		listMeta.Updated = now
// 		listMeta.LastAccess = now

// 		metaBuf, err := proto.Marshal(listMeta)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to proto marshal list meta: %w", err)
// 		}
// 		tx.Set(metaKey, metaBuf)

// 		return nil, nil
// 	})
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("failed to get value: %w", err))
// 	}

// 	span.SetStatus(codes.Ok, "push handled")
// 	return resp.FormatInt(int64(len(members))), nil
// }

// func (s *session) handleLIndex(ctx context.Context, args []resp.Value) (string, error) {
// 	ctx, span := s.tracer.Start(ctx, "handleLIndex")
// 	defer span.End()

// 	if err := validateNumArgs(args, 2); err != nil {
// 		return "", recordErr(span, err)
// 	}

// 	key, err := extractStringArg(args[0])
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("failed to parse list key argument: %w", err))
// 	}

// 	indexStr, err := extractStringArg(args[1])
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("failed to parse list index argument: %w", err))
// 	}
// 	index, err := strconv.ParseInt(indexStr, 10, 64)
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("failed to parse list index argument: %w", err))
// 	}

// 	bufAny, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
// 		_, listMeta, err := s.getListMeta(ctx, tx, key)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to get list meta: %w", err)
// 		}

// 		if listMeta == nil {
// 			return nil, nil
// 		}

// 		// @TODO (jrc): update last_accessed out of band

// 		targetNdx := index
// 		if targetNdx < 0 {
// 			// wrap around if negative (using addition because targetNdx is already negative)
// 			targetNdx = int64(listMeta.NumItems) + targetNdx
// 		}

// 		objKey := listMeta.ItemHead
// 		for targetNdx > 0 && objKey != "" {
// 			objMetaKey, err := s.listObjMetaKey(key, objKey)
// 			if err != nil {
// 				return nil, fmt.Errorf("failed to get list object meta: %w", err)
// 			}

// 			listObjMeta := &types.ListObjectMeta{}
// 			ok, err := s.getProtoItem(objMetaKey, listObjMeta)
// 			if err != nil {
// 				return nil, fmt.Errorf("failed to get list object meta: %w", err)
// 			}
// 			if !ok {
// 				return nil, fmt.Errorf("list object meta not found")
// 			}

// 			targetNdx -= 1
// 			objKey = listObjMeta.Next
// 		}

// 		_, buf, err := s.getObject(ctx, tx, objKey)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to get object payload: %w", err)
// 		}
// 		if len(buf) == 0 {
// 			return nil, nil
// 		}
// 		return buf, nil
// 	})
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("failed to get value: %w", err))
// 	}

// 	if bufAny == nil {
// 		span.SetStatus(codes.Ok, "lindex handled")
// 		return resp.FormatNil(), nil
// 	}

// 	buf, err := cast[[]byte](bufAny)
// 	if err != nil {
// 		return "", recordErr(span, fmt.Errorf("invalid result type: %w", err))
// 	}

// 	span.SetStatus(codes.Ok, "lindex handled")
// 	return resp.FormatBulkString(string(buf)), nil
// }
