package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	roaring "github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/go-util/pkg/concurrent"
	"github.com/bluesky-social/kvdb/internal/metrics"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	uidSequencePrefix  = "uid_sequence/"
	memberToUidPrefix  = "member_to_uid/"
	uidToMemberPrefix  = "uid_to_member/"
	setPrefix          = "set/"
	maxValBytes        = 100_000  // 100k
	blobIndexSeparator = '\u21FB' // ⇻ used as a separator between the key and the chunk index
	blobSuffixStart    = "0000001"
	blobSuffixEnd      = "9999999"
)

var illegalKeyRunes = []rune{
	blobIndexSeparator,
}

var ErrInvalidKey = errors.New("invalid key: contains illegal characters")

type session struct {
	log    *slog.Logger
	tracer trace.Tracer

	fdb    fdb.Database
	conn   io.ReadWriter
	reader *bufio.Reader
}

type NewSessionArgs struct {
	FDB  fdb.Database
	Conn io.ReadWriter
}

func NewSession(args *NewSessionArgs) *session {
	return &session{
		log:    slog.Default().With(slog.String("group", "server")),
		tracer: otel.Tracer("kvdb"),
		fdb:    args.FDB,
		conn:   args.Conn,
		reader: bufio.NewReader(args.Conn),
	}
}

func (s *session) write(msg string) {
	_, err := s.conn.Write([]byte(msg))
	if err != nil {
		s.log.Warn("failed to write message to client", "err", err)
	}
}

func isValidKey(key string) bool {
	// This could get expensive if we have a lot of illegal runes, but for now it's fine
	for _, r := range illegalKeyRunes {
		if strings.ContainsRune(key, r) {
			return false
		}
	}
	return true
}

func (s *session) Serve(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "Serve")
	defer span.End()

	for {
		cmd, err := s.parseCommand(ctx, s.reader)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			s.write(resp.FormatError(fmt.Errorf("failed to parse command: %w", err)))
			continue
		}

		s.write(s.handleCommand(ctx, cmd))
	}
}

func (s *session) parseCommand(ctx context.Context, reader *bufio.Reader) (*resp.Command, error) {
	ctx, span := s.tracer.Start(ctx, "parseCommand") // nolint
	defer span.End()

	cmd, err := resp.ParseCommand(reader)
	if err != nil {
		return nil, recordErr(span, err)
	}

	return cmd, nil
}

func (s *session) handleCommand(ctx context.Context, cmd *resp.Command) string {
	ctx, span := s.tracer.Start(ctx, "handleCommand")
	defer span.End()

	cmdLower := strings.ToLower(cmd.Name)

	span.SetAttributes(
		attribute.String("cmd", cmdLower),
		attribute.Int("num_args", len(cmd.Args)),
	)

	status := metrics.StatusError
	start := time.Now()
	defer func() {
		metrics.Queries.WithLabelValues(cmd.Name, status).Inc()
		metrics.QueryDuration.WithLabelValues(cmd.Name, status).Observe(time.Since(start).Seconds())
	}()

	var res string
	var err error
	switch cmdLower {
	case "quit":
		res = resp.FormatSimpleString("OK")
	case "ping":
		res, err = s.handlePing(ctx, cmd.Args)
	case "get":
		res, err = s.handleGet(ctx, cmd.Args)
	case "exists":
		res, err = s.handleExists(ctx, cmd.Args)
	case "set":
		res, err = s.handleSet(ctx, cmd.Args)
	case "del":
		res, err = s.handleDelete(ctx, cmd.Args)
	case "incr":
		res, err = s.handleIncr(ctx, cmd.Args)
	case "incrby":
		res, err = s.handleIncrBy(ctx, cmd.Args)
	case "decr":
		res, err = s.handleDecr(ctx, cmd.Args)
	case "decrby":
		res, err = s.handleDecrBy(ctx, cmd.Args)
	case "sadd":
		res, err = s.handleSetAdd(ctx, cmd.Args)
	case "srem":
		res, err = s.handleSetRemove(ctx, cmd.Args)
	case "sismember":
		res, err = s.handleSetIsMember(ctx, cmd.Args)
	case "scard":
		res, err = s.handleSetCard(ctx, cmd.Args)
	case "smembers":
		res, err = s.handleSetMembers(ctx, cmd.Args)
	case "sinter":
		res, err = s.handleSetInter(ctx, cmd.Args)
	case "sunion":
		res, err = s.handleSetUnion(ctx, cmd.Args)
	case "sdiff":
		res, err = s.handleSetDiff(ctx, cmd.Args)
	default:
		err := fmt.Errorf("unknown command %q", cmd.Name)
		span.RecordError(err)
		return resp.FormatError(err)
	}
	if err != nil {
		span.RecordError(err)
		return resp.FormatError(err)
	}

	status = metrics.StatusOK
	return res
}

func extractKeyArg(val resp.Value) (string, error) {
	key, err := extractStringArg(val)
	if err != nil {
		return "", fmt.Errorf("failed to parse key argument: %w", err)
	}

	if !isValidKey(key) {
		return "", ErrInvalidKey
	}
	return key, nil
}

func extractStringArg(val resp.Value) (string, error) {
	switch val.Type {
	case resp.TypeSimpleString:
		v, ok := val.Value.(string)
		if !ok {
			return "", fmt.Errorf("invalid simple string type %T", val.Value)
		}
		return v, nil
	case resp.TypeBulkString:
		if val.Value == nil {
			return "", fmt.Errorf("null bulk string")
		}
		v, ok := val.Value.(string)
		if !ok {
			return "", fmt.Errorf("invalid bulk string type %T", val.Value)
		}
		return v, nil
	case resp.TypeInteger:
		v, ok := val.Value.(int64)
		if !ok {
			return "", fmt.Errorf("invalid integer type %T", val.Value)
		}
		return strconv.FormatInt(v, 10), nil
	case resp.TypeDouble:
		v, ok := val.Value.(float64)
		if !ok {
			return "", fmt.Errorf("invalid double type %T", val.Value)
		}
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case resp.TypeBoolean:
		v, ok := val.Value.(bool)
		if !ok {
			return "", fmt.Errorf("invalid bool type %T", val.Value)
		}
		if v {
			return "1", nil
		}
		return "0", nil
	default:
		return "", fmt.Errorf("unsupported type for string conversion: %v", val.Type)
	}
}

func recordErr(span trace.Span, err error) error {
	span.RecordError(err)
	return err
}

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
	key, err := extractStringArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument: %w", err)
	}

	if !isValidKey(key) {
		return nil, ErrInvalidKey
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

	if len(args) < 2 {
		return "", recordErr(span, fmt.Errorf("incorrect number of arguments for set"))
	}

	key, err := extractKeyArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	value, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse value argument: %w", err))
	}

	_, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		tx.Set(fdb.Key(key), []byte(value))
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

	key, err := extractKeyArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	res, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		return s.deleteLargeObject(tx, key)
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to delete value: %w", err))
	}

	exists, ok := res.(bool)
	if !ok {
		return "", recordErr(span, fmt.Errorf("failed to cast exists of type %T to a bool", res))
	}

	return resp.FormatBoolAsInt(exists), nil
}

func (s *session) handleIncr(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleIncr")
	defer span.End()

	res, err := s.handleIncrDecr(ctx, args, 1)
	if err != nil {
		span.RecordError(err)
		return "", err
	}

	return res, nil
}

func (s *session) handleIncrBy(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleIncrBy")
	defer span.End()

	if len(args) != 2 {
		return "", recordErr(span, fmt.Errorf("incorrect number of arguments for incrby"))
	}

	incr, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse increment argument: %w", err))
	}

	by, err := strconv.ParseInt(incr, 10, 64)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse increment argument to int: %w", err))
	}

	res, err := s.handleIncrDecr(ctx, args, by)
	if err != nil {
		span.RecordError(err)
		return "", err
	}

	return res, nil
}

func (s *session) handleDecr(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleDecr")
	defer span.End()

	res, err := s.handleIncrDecr(ctx, args, -1)
	if err != nil {
		span.RecordError(err)
		return "", err
	}

	return res, nil
}

func (s *session) handleDecrBy(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleDecrBy")
	defer span.End()

	if len(args) != 2 {
		return "", recordErr(span, fmt.Errorf("incorrect number of arguments for decrby"))
	}

	decr, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse increment argument: %w", err))
	}

	by, err := strconv.ParseInt(decr, 10, 64)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse decrement argument to int: %w", err))
	}

	// reverse sign
	by *= -1

	res, err := s.handleIncrDecr(ctx, args, by)
	if err != nil {
		span.RecordError(err)
		return "", err
	}

	return res, nil
}

func (s *session) handleIncrDecr(ctx context.Context, args []resp.Value, by int64) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleIncrDecr") // nolint
	defer span.End()

	k, err := extractKeyArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	key := fdb.Key(k)

	res, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		val, err := tx.Get(fdb.Key(key)).Get()
		if err != nil {
			return nil, err
		}

		if len(val) == 0 {
			// item does not yet exist
			val = []byte("0")
		}

		n, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse value to int: %w", err)
		}

		n += by
		tx.Set(key, []byte(strconv.FormatInt(n, 10)))
		return n, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to incr value: %w", err))
	}

	n, ok := res.(int64)
	if !ok {
		return "", recordErr(span, fmt.Errorf("failed to cast result of type %T to int64", res))
	}

	return resp.FormatInt(n), nil
}

// Large Objects are implemented by chunking the object into pieces of maxValBytes
// The root key stores the total length of the object as a string
// Each chunk is stored at key + blobIndexSeparator + chunkIndex (1-based, zero-padded to 7 digits)
// e.g. "mykey⇻0000001", "mykey⇻0000002", ...
// Large Objects are written and read in a single transaction and so transaction duration limits apply
func (s *session) readLargeObject(tx fdb.ReadTransaction, key string) ([]byte, error) {
	start := time.Now()

	// Fetch the Metadata Key with the total length of the blob
	val, err := tx.Get(fdb.Key(key)).Get()
	if err != nil {
		return nil, fmt.Errorf("failed to read large object metadata key: %w", err)
	}
	if len(val) == 0 {
		return nil, nil
	}

	// Unpack the length from a string
	totalLength, err := strconv.Atoi(string(val))
	if err != nil {
		return nil, fmt.Errorf("failed to parse total length: %w", err)
	}

	// Compute the number of chunks required to store the blob
	rangeEnd := (totalLength / maxValBytes) + 1
	if totalLength%maxValBytes == 0 {
		rangeEnd = totalLength / maxValBytes
	}

	// Read the blob chunks in parallel
	iters := make([]int, rangeEnd)
	for i := 0; i < rangeEnd; i++ {
		iters[i] = i
	}

	r := concurrent.New[int, []byte]()
	chunks, err := r.Do(context.Background(), iters, func(i int) ([]byte, error) {
		chunkKey := fmt.Sprintf("%s%c%07d", key, blobIndexSeparator, i+1)
		val, err := tx.Get(fdb.Key(chunkKey)).Get()
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk %d: %w", i+1, err)
		}
		return val, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read chunks: %w", err)
	}

	// Concatenate the chunks up to the total length
	buf := make([]byte, 0, totalLength)
	bytesRead := 0
	for _, chunk := range chunks {
		// Append the chunk to the buffer but don't exceed the total length
		end := min(bytesRead+len(chunk), totalLength)
		buf = append(buf, chunk[:end-bytesRead]...)
		bytesRead = end
	}

	s.log.Info("read large object", "key", key, "total_length", totalLength, "num_chunks", rangeEnd, "actual_length", len(buf), "duration", time.Since(start).String())

	return buf, nil
}

func (s *session) writeLargeObject(tx fdb.Transaction, key string, data []byte) (int64, error) {
	// Write the object in chunks of maxValBytes
	totalLength := len(data)
	rangeEnd := (totalLength / maxValBytes) + 1
	if totalLength%maxValBytes == 0 {
		rangeEnd = totalLength / maxValBytes
	}

	// First clear any existing chunks
	tx.ClearRange(fdb.KeyRange{
		Begin: fdb.Key(fmt.Sprintf("%s%c%s", key, blobIndexSeparator, blobSuffixStart)),
		End:   fdb.Key(fmt.Sprintf("%s%c%s", key, blobIndexSeparator, blobSuffixEnd)),
	})

	// Write the length of the object at the key without a suffix
	tx.Set(fdb.Key(key), []byte(strconv.Itoa(totalLength)))

	iters := make([]int, rangeEnd)
	for i := 0; i < rangeEnd; i++ {
		iters[i] = i
	}

	r := concurrent.New[int, any]()

	// Write all chunks concurrently since ordering does not matter
	_, _ = r.Do(context.Background(), iters, func(i int) (any, error) {
		start := i * maxValBytes
		end := min((i+1)*maxValBytes, totalLength)
		chunkKey := fmt.Sprintf("%s%c%07d", key, blobIndexSeparator, i+1)
		tx.Set(fdb.Key(chunkKey), data[start:end])
		return nil, nil
	})
	return int64(totalLength), nil
}

func (s *session) deleteLargeObject(tx fdb.Transaction, key string) (bool, error) {
	// Check if the object exists
	val, err := tx.Get(fdb.Key(key)).Get()
	if err != nil {
		return false, fmt.Errorf("failed to get large object: %w", err)
	}

	// Clear all chunks with the blob suffix range
	tx.ClearRange(fdb.KeyRange{
		Begin: fdb.Key(fmt.Sprintf("%s%c%s", key, blobIndexSeparator, blobSuffixStart)),
		End:   fdb.Key(fmt.Sprintf("%s%c%s", key, blobIndexSeparator, blobSuffixEnd)),
	})

	// Clear the main key
	tx.Clear(fdb.Key(key))

	return len(val) > 0, nil
}

// allocate a new unique 64-bit UID
// The upper 32 bits are a random sequence number
// The lower 32 bits are a sequential number within that sequence
// This allows for up to 4 billion unique IDs per sequence and very low contention when allocating new IDs
func (s *session) allocateNewUID(span trace.Span, tx fdb.Transaction) (uint64, error) {
	span.AddEvent("allocateNewUID")

	// sequenceNum is the random uint32 sequence we are using for this allocation
	var sequenceNum uint32
	var sequenceKey string

	// assignedUID is the uint32 within the sequence we will assign
	var assignedUID uint32

	// Try up to 5 times to find a sequence that is not exhausted
	for range 5 {
		// Pick a random uint32 as the sequence we will be using for this member
		sequenceNum = rand.Uint32()
		sequenceKey = fmt.Sprintf("%s%d", uidSequencePrefix, sequenceNum)

		val, err := tx.Get(fdb.Key(sequenceKey)).Get()
		if err != nil {
			return 0, recordErr(span, fmt.Errorf("failed to get last UID: %w", err))
		}
		if len(val) == 0 {
			assignedUID = 1
		} else {
			lastUID, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return 0, recordErr(span, fmt.Errorf("failed to parse last UID: %w", err))
			}

			// If we have exhausted this sequence, pick a new random sequence
			if lastUID >= 0xFFFFFFFF {
				continue
			}

			assignedUID = uint32(lastUID) + 1
		}
	}

	// If we failed to find a sequence after 5 tries, return an error
	if assignedUID == 0 {
		return 0, recordErr(span, fmt.Errorf("failed to allocate new UID after 5 attempts"))
	}

	// Assemble the 64-bit UID from the sequence and assigned UID
	newUID := (uint64(sequenceNum) << 32) | uint64(assignedUID)

	// Store the assigned UID back to the sequence key for the next allocation
	tx.Set(fdb.Key(sequenceKey), []byte(strconv.FormatUint(uint64(assignedUID), 10)))

	// Return the full 64-bit UID
	return newUID, nil
}

// getUID returns the UID for the given member string, creating a new one if it does not exist
// If peek is true, it will only look up the UID without creating a new one
// If peeking and the member does not exist, it returns 0 without an error
// UIDs start at 1, so 0 is never a valid UID
func (s *session) getUID(ctx context.Context, member string, peek bool) (uint64, error) {
	ctx, span := s.tracer.Start(ctx, "getUID") // nolint
	defer span.End()
	var memberToUIDKey = memberToUidPrefix + member

	var res any
	var err error

	if peek {
		// just look up the UID without creating a new one
		res, err = s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			val, err := tx.Get(fdb.Key(memberToUIDKey)).Get()
			if err != nil {
				return nil, fmt.Errorf("failed to get member to UID mapping: %w", err)
			}
			if len(val) == 0 {
				return []byte("0"), nil
			}
			return val, nil
		})
	} else {
		res, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
			// Check if we've already assigned a UID to this member
			val, err := tx.Get(fdb.Key(memberToUIDKey)).Get()
			if err != nil {
				return nil, fmt.Errorf("failed to get member to UID mapping: %w", err)
			}

			if len(val) == 0 {
				// Allocate a new UID for this member string
				uid, err := s.allocateNewUID(span, tx)
				if err != nil {
					return nil, fmt.Errorf("failed to allocate new UID: %w", err)
				}

				uidStr := strconv.FormatUint(uid, 10)
				var uidToMemberKey = uidToMemberPrefix + uidStr

				// Store the bi-directional mapping
				tx.Set(fdb.Key(memberToUIDKey), []byte(uidStr))
				tx.Set(fdb.Key(uidToMemberKey), []byte(member))

				return []byte(uidStr), nil
			}
			return val, nil
		})
	}
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to get or create UID: %w", err))
	}

	uidStr, ok := res.([]byte)
	if !ok {
		return 0, recordErr(span, fmt.Errorf("invalid UID type: %T", res))
	}

	uid, err := strconv.ParseUint(string(uidStr), 10, 64)
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to parse UID: %w", err))
	}

	return uid, nil
}

func (s *session) uidToMember(ctx context.Context, uid uint64) (string, error) {
	ctx, span := s.tracer.Start(ctx, "uidToMember") // nolint
	defer span.End()

	uidStr := strconv.FormatUint(uid, 10)
	var uidToMemberKey = uidToMemberPrefix + uidStr

	var member string
	res, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		val, err := tx.Get(fdb.Key(uidToMemberKey)).Get()
		if err != nil {
			return nil, fmt.Errorf("failed to get UID to member mapping: %w", err)
		}
		if len(val) == 0 {
			return nil, fmt.Errorf("UID %d not found", uid)
		}
		member = string(val)
		return member, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get member for UID %d: %w", uid, err))
	}

	member, ok := res.(string)
	if !ok {
		return "", recordErr(span, fmt.Errorf("invalid member type: %T", res))
	}

	return member, nil
}

func (s *session) handleSetAdd(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetAdd") // nolint
	defer span.End()

	if len(args) < 2 {
		return "", recordErr(span, fmt.Errorf("SADD requires at least 2 arguments"))
	}

	setKey, err := extractKeyArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	var members []string
	for _, arg := range args[1:] {
		member, err := extractStringArg(arg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse member argument: %w", err))
		}
		members = append(members, member)
	}

	added, err := s.redisSetAdd(ctx, setKey, members)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to add members to set: %w", err))
	}

	return resp.FormatInt(added), nil
}

func (s *session) redisSetAdd(ctx context.Context, setKey string, members []string) (int64, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetAdd") // nolint
	defer span.End()

	if len(members) == 0 {
		return 0, nil
	}

	// get or create UIDs for all members concurrently
	r := concurrent.New[string, uint64]()

	uids, err := r.Do(ctx, members, func(member string) (uint64, error) {
		return s.getUID(ctx, member, false)
	})
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to get UIDs for members: %w", err))
	}

	start := time.Now()
	res, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		// Get the Bitmap if it exists
		bitmapKey := setPrefix + setKey
		blob, err := s.readLargeObject(tx, bitmapKey)
		if err != nil {
			return int64(0), fmt.Errorf("failed to read large object: %w", err)
		}

		// If the key doesn't exist, create a new bitmap
		var bitmap *roaring.Bitmap
		if len(blob) == 0 {
			bitmap = roaring.New()
		} else if len(blob) > 0 {
			bitmap = roaring.New()
			if err := bitmap.UnmarshalBinary(blob); err != nil {
				return int64(0), fmt.Errorf("failed to unmarshal bitmap: %w", err)
			}
		}

		// Add the new UIDs to the bitmap
		bitmap.AddMany(uids)

		// serialize and store the updated bitmap as a large object
		data, err := bitmap.MarshalBinary()
		if err != nil {
			return int64(0), fmt.Errorf("failed to marshal bitmap: %w", err)
		}
		bytesWritten, err := s.writeLargeObject(tx, bitmapKey, data)
		if err != nil {
			return int64(0), fmt.Errorf("failed to write large object: %w", err)
		}

		return bytesWritten, nil
	})
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to add members to set: %w", err))
	}

	bytesWritten, ok := res.(int64)
	if !ok {
		return 0, recordErr(span, fmt.Errorf("invalid bytesWritten type: %T", res))
	}

	s.log.Info("updated bitmap", "size", bytesWritten, "duration", time.Since(start).String())

	return int64(len(uids)), nil
}

func (s *session) handleSetRemove(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetRemove") // nolint
	defer span.End()

	if len(args) < 2 {
		return "", recordErr(span, fmt.Errorf("SREM requires at least 2 arguments"))
	}

	setKey, err := extractKeyArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	var members []string
	for _, arg := range args[1:] {
		member, err := extractStringArg(arg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse member argument: %w", err))
		}
		members = append(members, member)
	}

	removed, err := s.redisSetRemove(ctx, setKey, members)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to remove members from set: %w", err))
	}

	return resp.FormatInt(removed), nil
}

func (s *session) redisSetRemove(ctx context.Context, setKey string, members []string) (int64, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetRemove") // nolint
	defer span.End()

	if len(members) == 0 {
		return 0, nil
	}

	// get or create UIDs for all members concurrently
	r := concurrent.New[string, uint64]()

	uids, err := r.Do(ctx, members, func(member string) (uint64, error) {
		// Peek since we don't want to create new UIDs when removing
		return s.getUID(ctx, member, true)
	})
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to get UIDs for members: %w", err))
	}

	res, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		// Get the Bitmap if it exists
		bitmapKey := setPrefix + setKey
		blob, err := s.readLargeObject(tx, bitmapKey)
		if err != nil {
			return int64(0), fmt.Errorf("failed to read large object: %w", err)
		}

		if len(blob) == 0 {
			// set does not exist, nothing to remove
			return int64(0), nil
		}

		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(blob); err != nil {
			return int64(0), fmt.Errorf("failed to unmarshal bitmap: %w", err)
		}

		removed := int64(0)
		for _, uid := range uids {
			if bitmap.Contains(uid) {
				bitmap.Remove(uid)
				removed++
			}
		}

		// If the set is now empty, delete the large object
		if bitmap.IsEmpty() {
			if _, err := s.deleteLargeObject(tx, bitmapKey); err != nil {
				return int64(0), fmt.Errorf("failed to delete large object: %w", err)
			}
			return removed, nil
		}

		// serialize and store the updated bitmap
		data, err := bitmap.MarshalBinary()
		if err != nil {
			return int64(0), fmt.Errorf("failed to marshal bitmap: %w", err)
		}
		_, err = s.writeLargeObject(tx, bitmapKey, data)
		if err != nil {
			return int64(0), fmt.Errorf("failed to write large object: %w", err)
		}

		return removed, nil
	})
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to remove members from set: %w", err))
	}

	removed, ok := res.(int64)
	if !ok {
		return 0, recordErr(span, fmt.Errorf("invalid removed type: %T", res))
	}

	return removed, nil
}

func (s *session) handleSetIsMember(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetIsMember") // nolint
	defer span.End()

	if len(args) != 2 {
		return "", recordErr(span, fmt.Errorf("SISMEMBER requires exactly 2 arguments"))
	}

	setKey, err := extractKeyArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	member, err := extractStringArg(args[1])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse member argument: %w", err))
	}

	isMember, err := s.redisSetIsMember(ctx, setKey, member)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to check membership: %w", err))
	}

	return resp.FormatBoolAsInt(isMember), nil
}

func (s *session) redisSetIsMember(ctx context.Context, setKey string, member string) (bool, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetIsMember") // nolint
	defer span.End()

	// Peek since we don't want to create new UIDs when checking membership
	uid, err := s.getUID(ctx, member, true)
	if err != nil {
		return false, recordErr(span, fmt.Errorf("failed to get UID for member: %w", err))
	}

	res, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		// Get the Bitmap if it exists
		bitmapKey := setPrefix + setKey
		blob, err := s.readLargeObject(tx, bitmapKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read large object: %w", err)
		}
		return blob, nil
	})
	if err != nil {
		return false, recordErr(span, fmt.Errorf("failed to check membership: %w", err))
	}

	blob, ok := res.([]byte)
	if !ok {
		return false, recordErr(span, fmt.Errorf("invalid blob type: %T", res))
	}

	if len(blob) == 0 {
		// set does not exist, member cannot be present
		return false, nil
	}

	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(blob); err != nil {
		return false, fmt.Errorf("failed to unmarshal bitmap: %w", err)
	}

	return bitmap.Contains(uid), nil
}

func (s *session) handleSetCard(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetCard") // nolint
	defer span.End()

	if len(args) != 1 {
		return "", recordErr(span, fmt.Errorf("SCARD requires exactly 1 argument"))
	}

	setKey, err := extractKeyArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	card, err := s.redisSetCard(ctx, setKey)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get set cardinality: %w", err))
	}

	return resp.FormatInt(card), nil
}

func (s *session) redisSetCard(ctx context.Context, setKey string) (int64, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetCard") // nolint
	defer span.End()

	res, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		// Get the Bitmap's Meta Key if it exists
		bitmapKey := setPrefix + setKey
		blob, err := s.readLargeObject(tx, bitmapKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read large object: %w", err)
		}
		return blob, nil
	})
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to get set cardinality: %w", err))
	}

	blob, ok := res.([]byte)
	if !ok {
		return 0, recordErr(span, fmt.Errorf("invalid blob type: %T", res))
	}

	if len(blob) == 0 {
		// set does not exist, cardinality is 0
		return int64(0), nil
	}

	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(blob); err != nil {
		return int64(0), fmt.Errorf("failed to unmarshal bitmap: %w", err)
	}

	// uint64 to int64 conversion, beware of overflow though incredibly unlikely
	return int64(bitmap.GetCardinality()), nil
}

func (s *session) handleSetMembers(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetMembers") // nolint
	defer span.End()

	if len(args) != 1 {
		return "", recordErr(span, fmt.Errorf("SMEMBERS requires exactly 1 argument"))
	}

	setKey, err := extractKeyArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	members, err := s.redisSetMembers(ctx, setKey)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get set members: %w", err))
	}

	return resp.FormatArrayOfBulkStrings(members), nil
}

func (s *session) redisSetMembers(ctx context.Context, setKey string) ([]string, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetMembers") // nolint
	defer span.End()

	res, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		// Get the Bitmap if it exists
		bitmapKey := setPrefix + setKey
		blob, err := s.readLargeObject(tx, bitmapKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read large object: %w", err)
		}
		return blob, nil
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to get set members: %w", err))
	}

	blob, ok := res.([]byte)
	if !ok {
		return nil, recordErr(span, fmt.Errorf("invalid blob type: %T", res))
	}

	if len(blob) == 0 {
		// set does not exist, no members
		return []string{}, nil
	}

	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(blob); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
	}

	if bitmap == nil || bitmap.IsEmpty() {
		return []string{}, nil
	}

	// convert UIDs back to members
	uids := bitmap.ToArray()
	r := concurrent.New[uint64, string]()

	members, err := r.Do(ctx, uids, func(u uint64) (string, error) {
		return s.uidToMember(ctx, u)
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to get members for UIDs: %w", err))
	}

	return members, nil
}

func (s *session) handleSetInter(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetInter") // nolint
	defer span.End()

	if len(args) < 1 {
		return "", recordErr(span, fmt.Errorf("SINTER requires at least 1 argument"))
	}

	var setKeys []string
	for _, arg := range args {
		setKey, err := extractKeyArg(arg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
		}
		setKeys = append(setKeys, setKey)
	}

	members, err := s.redisSetInter(ctx, setKeys)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to compute set intersection: %w", err))
	}

	return resp.FormatArrayOfBulkStrings(members), nil
}

func (s *session) redisSetInter(ctx context.Context, setKeys []string) ([]string, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetInter") // nolint
	defer span.End()

	if len(setKeys) == 0 {
		return []string{}, nil
	}

	res, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		r := concurrent.New[string, []byte]()
		blobs, err := r.Do(ctx, setKeys, func(setKey string) ([]byte, error) {
			bitmapKey := setPrefix + setKey
			blob, err := s.readLargeObject(tx, bitmapKey)
			if err != nil {
				return nil, fmt.Errorf("failed to read large object for set %q: %w", setKey, err)
			}
			return blob, nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to read bitmaps for sets: %w", err)
		}
		return blobs, nil
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to compute set intersection: %w", err))
	}

	blobs, ok := res.([][]byte)
	if !ok {
		return nil, recordErr(span, fmt.Errorf("invalid blobs type: %T", res))
	}

	// Unmarshal and intersect all bitmaps
	r := concurrent.New[[]byte, *roaring.Bitmap]()
	bitmaps, err := r.Do(ctx, blobs, func(blob []byte) (*roaring.Bitmap, error) {
		if len(blob) == 0 {
			// empty bitmap
			return roaring.New(), nil
		}
		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(blob); err != nil {
			return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
		}
		return bitmap, nil
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to unmarshal bitmaps: %w", err))
	}

	// Intersect all bitmaps
	resultBitmap := bitmaps[0]
	for _, bitmap := range bitmaps[1:] {
		if bitmap == nil || bitmap.IsEmpty() {
			// intersection with empty set is empty
			return []string{}, nil
		}
		resultBitmap.And(bitmap)
	}

	if resultBitmap == nil || resultBitmap.IsEmpty() {
		return []string{}, nil
	}

	// convert UIDs back to members
	uids := resultBitmap.ToArray()
	r2 := concurrent.New[uint64, string]()
	members, err := r2.Do(ctx, uids, func(u uint64) (string, error) {
		return s.uidToMember(ctx, u)
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to get members for UIDs: %w", err))
	}

	return members, nil
}

func (s *session) handleSetUnion(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetUnion") // nolint
	defer span.End()

	if len(args) < 1 {
		return "", recordErr(span, fmt.Errorf("SUNION requires at least 1 argument"))
	}

	var setKeys []string
	for _, arg := range args {
		setKey, err := extractKeyArg(arg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
		}
		setKeys = append(setKeys, setKey)
	}

	members, err := s.redisSetUnion(ctx, setKeys)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to compute set union: %w", err))
	}

	return resp.FormatArrayOfBulkStrings(members), nil
}

func (s *session) redisSetUnion(ctx context.Context, setKeys []string) ([]string, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetUnion")
	defer span.End()

	if len(setKeys) == 0 {
		return []string{}, nil
	}

	res, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		r := concurrent.New[string, []byte]()
		blobs, err := r.Do(ctx, setKeys, func(setKey string) ([]byte, error) {
			bitmapKey := setPrefix + setKey
			blob, err := s.readLargeObject(tx, bitmapKey)
			if err != nil {
				return nil, fmt.Errorf("failed to read large object for set %q: %w", setKey, err)
			}
			return blob, nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to read bitmaps for sets: %w", err)
		}
		return blobs, nil
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to compute set union: %w", err))
	}

	blobs, ok := res.([][]byte)
	if !ok {
		return nil, recordErr(span, fmt.Errorf("invalid blobs type: %T", res))
	}

	// Unmarshal and union all bitmaps
	r := concurrent.New[[]byte, *roaring.Bitmap]()
	bitmaps, err := r.Do(ctx, blobs, func(blob []byte) (*roaring.Bitmap, error) {
		if len(blob) == 0 {
			// empty bitmap
			return roaring.New(), nil
		}
		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(blob); err != nil {
			return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
		}
		return bitmap, nil
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to unmarshal bitmaps: %w", err))
	}

	// Union all bitmaps
	resultBitmap := roaring.New()
	for _, bitmap := range bitmaps {
		if bitmap == nil || bitmap.IsEmpty() {
			// union with empty set is no-op
			continue
		}
		resultBitmap.Or(bitmap)
	}

	if resultBitmap == nil || resultBitmap.IsEmpty() {
		return []string{}, nil
	}

	// convert UIDs back to members
	uids := resultBitmap.ToArray()
	r2 := concurrent.New[uint64, string]()
	members, err := r2.Do(ctx, uids, func(u uint64) (string, error) {
		return s.uidToMember(ctx, u)
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to get members for UIDs: %w", err))
	}

	return members, nil
}

func (s *session) handleSetDiff(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleSetDiff") // nolint
	defer span.End()

	if len(args) < 1 {
		return "", recordErr(span, fmt.Errorf("SDIFF requires at least 1 argument"))
	}

	var setKeys []string
	for _, arg := range args {
		setKey, err := extractKeyArg(arg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
		}
		setKeys = append(setKeys, setKey)
	}

	members, err := s.redisSetDiff(ctx, setKeys)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to compute set difference: %w", err))
	}

	return resp.FormatArrayOfBulkStrings(members), nil
}

func (s *session) redisSetDiff(ctx context.Context, setKeys []string) ([]string, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetDiff") // nolint
	defer span.End()

	if len(setKeys) == 0 {
		return []string{}, nil
	}

	res, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		r := concurrent.New[string, []byte]()
		blobs, err := r.Do(ctx, setKeys, func(setKey string) ([]byte, error) {
			bitmapKey := setPrefix + setKey
			blob, err := s.readLargeObject(tx, bitmapKey)
			if err != nil {
				return nil, fmt.Errorf("failed to read large object for set %q: %w", setKey, err)
			}
			return blob, nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to read bitmaps for sets: %w", err)
		}
		return blobs, nil
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to compute set difference: %w", err))
	}

	blobs, ok := res.([][]byte)
	if !ok {
		return nil, recordErr(span, fmt.Errorf("invalid blobs type: %T", res))
	}

	// Unmarshal and difference all bitmaps
	r := concurrent.New[[]byte, *roaring.Bitmap]()
	bitmaps, err := r.Do(ctx, blobs, func(blob []byte) (*roaring.Bitmap, error) {
		if len(blob) == 0 {
			// empty bitmap
			return roaring.New(), nil
		}
		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(blob); err != nil {
			return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
		}
		return bitmap, nil
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to unmarshal bitmaps: %w", err))
	}

	// Difference all bitmaps
	resultBitmap := bitmaps[0]
	for _, bitmap := range bitmaps[1:] {
		if bitmap == nil || bitmap.IsEmpty() {
			// difference with empty set is no-op
			continue
		}
		resultBitmap.AndNot(bitmap)
	}

	if resultBitmap == nil || resultBitmap.IsEmpty() {
		return []string{}, nil
	}

	// convert UIDs back to members
	uids := resultBitmap.ToArray()
	r2 := concurrent.New[uint64, string]()
	members, err := r2.Do(ctx, uids, func(u uint64) (string, error) {
		return s.uidToMember(ctx, u)
	})
	if err != nil {
		return nil, recordErr(span, fmt.Errorf("failed to get members for UIDs: %w", err))
	}

	return members, nil
}
