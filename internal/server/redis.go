package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	roaring "github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/go-util/pkg/concurrent"
	"github.com/bluesky-social/kvdb/internal/metrics"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *server) serveRedis(wg *sync.WaitGroup, done <-chan any, args *Args) {
	defer wg.Done()

	l, err := net.Listen("tcp", args.RedisAddr)
	if err != nil {
		s.log.Error("failed to initialize redis listener", "err", err)
		os.Exit(1)
	}

	s.log.Info("redis server listening", "addr", args.RedisAddr)

	go func() {
		// wait until the user requests that the server is shut down, then close the listener
		<-done
		if err := l.Close(); err != nil {
			s.log.Error("failed to close redis server", "err", err)
			os.Exit(1)
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-done:
				s.log.Info("redis server stopped")
				return
			default:
			}

			s.log.Warn("failed to accept client connection", "err", err)
			continue
		}

		go s.handleRedisConn(context.Background(), &redisSession{
			conn:   conn,
			reader: bufio.NewReader(conn),
		})
	}
}

type redisSession struct {
	conn   net.Conn
	reader *bufio.Reader
}

func (sess *redisSession) write(s *server, msg string) {
	_, err := sess.conn.Write([]byte(msg))
	if err != nil {
		s.log.Warn("failed to write message to client", "err", err)
	}
}

func formatSimpleString(str string) string {
	return fmt.Sprintf("+%s\r\n", str)
}

func formatBulkString(str string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
}

func formatArrayOfBulkStrings(strs []string) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("*%d\r\n", len(strs)))
	for _, str := range strs {
		b.WriteString(formatBulkString(str))
	}
	return b.String()
}

func formatNil() string {
	return "_\r\n"
}

func formatBoolAsInt(val bool) string {
	n := int64(0)
	if val {
		n = 1
	}
	return formatInt(n)
}

func formatInt(n int64) string {
	return fmt.Sprintf(":%d\r\n", n)
}

func formatError(err error) string {
	return fmt.Sprintf("-ERR %s\r\n", err)
}

func (s *server) handleRedisConn(ctx context.Context, sess *redisSession) {
	defer func() {
		if err := sess.conn.Close(); err != nil {
			s.log.Warn("failed to close client connection", "err", err)
		}
	}()

	ctx, span := s.tracer.Start(ctx, "handleRedisConn")
	defer span.End()

	for {
		cmd, err := s.parseRedisCommand(ctx, sess.reader)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			sess.write(s, formatError(fmt.Errorf("failed to parse command: %w", err)))
			continue
		}

		resp := s.handleRedisCommand(ctx, cmd)
		sess.write(s, resp)
	}
}

func (s *server) parseRedisCommand(ctx context.Context, reader *bufio.Reader) (*resp.Command, error) {
	ctx, span := s.tracer.Start(ctx, "parseRedisCommand") // nolint
	defer span.End()

	cmd, err := resp.ParseCommand(reader)
	if err != nil {
		return nil, recordErr(span, err)
	}

	return cmd, nil
}

func (s *server) handleRedisCommand(ctx context.Context, cmd *resp.Command) string {
	ctx, span := s.tracer.Start(ctx, "handleRedisCommand") // nolint
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
	case "ping":
		res, err = s.handleRedisPing(ctx, cmd.Args)
	case "get":
		res, err = s.handleRedisGet(ctx, cmd.Args)
	case "exists":
		res, err = s.handleRedisExists(ctx, cmd.Args)
	case "set":
		res, err = s.handleRedisSet(ctx, cmd.Args)
	case "del":
		res, err = s.handleRedisDelete(ctx, cmd.Args)
	case "sadd":
		res, err = s.handleRedisSetAdd(ctx, cmd.Args)
	case "srem":
		res, err = s.handleRedisSetRemove(ctx, cmd.Args)
	case "sismember":
		res, err = s.handleRedisSetIsMember(ctx, cmd.Args)
	case "scard":
		res, err = s.handleRedisSetCard(ctx, cmd.Args)
	case "smembers":
		res, err = s.handleRedisSetMembers(ctx, cmd.Args)
	case "sinter":
		res, err = s.handleRedisSetInter(ctx, cmd.Args)
	case "incr":
		res, err = s.handleRedisIncr(ctx, cmd.Args)
	case "decr":
		res, err = s.handleRedisDecr(ctx, cmd.Args)
	case "quit":
		res = "+OK\r\n"
	default:
		err := fmt.Errorf("unknown command %q", cmd.Name)
		span.RecordError(err)
		return formatError(err)
	}
	if err != nil {
		span.RecordError(err)
		return formatError(err)
	}

	status = metrics.StatusOK
	return res
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

func (s *server) handleRedisPing(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisPing") // nolint
	defer span.End()

	const resp = "+PONG\r\n"
	if len(args) == 0 {
		return resp, nil
	}

	// echo back the first argument
	if arg, err := extractStringArg(args[0]); err == nil {
		return formatBulkString(arg), nil
	}

	return resp, nil
}

func (s *server) handleRedisGet(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisGet") // nolint
	defer span.End()

	val, err := s.redisGet(args)
	if err != nil {
		return "", recordErr(span, err)
	}

	if len(val) == 0 {
		// not found
		return formatNil(), nil
	}

	return formatBulkString(string(val)), nil
}

func (s *server) handleRedisExists(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisExists") // nolint
	defer span.End()

	val, err := s.redisGet(args)
	if err != nil {
		return "", recordErr(span, err)
	}

	return formatBoolAsInt(len(val) > 0), nil
}

func (s *server) redisGet(args []resp.Value) ([]byte, error) {
	arg, err := extractStringArg(args[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument: %w", err)
	}

	res, err := s.fdb.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(fdb.Key(arg)).Get()
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

func (s *server) handleRedisSet(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisSet") // nolint
	defer span.End()

	key, err := extractStringArg(args[0])
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

	return formatSimpleString("OK"), nil
}

func (s *server) handleRedisDelete(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisDelete") // nolint
	defer span.End()

	key, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse key argument: %w", err))
	}

	exists := false
	_, err = s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		val, err := tx.Get(fdb.Key(key)).Get()
		if err != nil {
			return nil, err
		}
		exists = len(val) > 0

		tx.Clear(fdb.Key(key))
		return nil, nil
	})
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to delete value: %w", err))
	}

	return formatBoolAsInt(exists), nil
}

func (s *server) handleRedisIncr(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisIncr")
	defer span.End()

	res, err := s.handleRedisIncrDecr(ctx, args, true)
	if err != nil {
		span.RecordError(err)
		return "", err
	}

	return res, nil
}

func (s *server) handleRedisDecr(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisDecr")
	defer span.End()

	res, err := s.handleRedisIncrDecr(ctx, args, false)
	if err != nil {
		span.RecordError(err)
		return "", err
	}

	return res, nil
}

func (s *server) handleRedisIncrDecr(ctx context.Context, args []resp.Value, incr bool) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisIncrDecr") // nolint
	defer span.End()

	k, err := extractStringArg(args[0])
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
			return nil, fmt.Errorf("failed to parse value to int")
		}

		if incr {
			n += 1
		} else {
			n -= 1
		}

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

	return formatInt(n), nil
}

// implement sets as serialized Roaring Bitmaps with interned keys we store in FDB

const (
	memberToUidPrefix = "member_to_uid/"
	uidToMemberPrefix = "uid_to_member/"
	setPrefix         = "set/"
	maxValBytes       = 100_000 // 100k
	blobSuffixStart   = "-0000001"
	blobSuffixEnd     = "-9999999"
)

func (s *server) readLargeObject(tx fdb.ReadTransaction, key string) ([]byte, error) {
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
		chunkKey := fmt.Sprintf("%s-%07d", key, i+1)
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

func (s *server) writeLargeObject(tx fdb.Transaction, key string, data []byte) error {
	start := time.Now()

	// Write the object in chunks of maxValBytes
	totalLength := len(data)
	rangeEnd := (totalLength / maxValBytes) + 1
	if totalLength%maxValBytes == 0 {
		rangeEnd = totalLength / maxValBytes
	}

	// First clear any existing chunks
	tx.ClearRange(fdb.KeyRange{
		Begin: fdb.Key(fmt.Sprintf("%s-%s", key, blobSuffixStart)),
		End:   fdb.Key(fmt.Sprintf("%s-%s", key, blobSuffixEnd)),
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
		chunkKey := fmt.Sprintf("%s-%07d", key, i+1)
		tx.Set(fdb.Key(chunkKey), data[start:end])
		return nil, nil
	})

	s.log.Info("wrote large object", "key", key, "total_length", totalLength, "num_chunks", rangeEnd, "duration", time.Since(start).String())

	return nil
}

func (s *server) getUID(ctx context.Context, member string) (uint64, error) {
	ctx, span := s.tracer.Start(ctx, "getUID") // nolint
	defer span.End()
	var memberToUIDKey = memberToUidPrefix + member

	memberHash := xxhash.Sum64([]byte(member))

	res, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		val, err := tx.Get(fdb.Key(memberToUIDKey)).Get()
		if err != nil {
			return nil, fmt.Errorf("failed to get member to UID mapping: %w", err)
		}

		if len(val) == 0 {
			// key does not exist yet, allocate a new UID
			uidStr := strconv.FormatUint(memberHash, 10)
			tx.Set(fdb.Key(memberToUIDKey), []byte(uidStr))

			var uidToMemberKey = uidToMemberPrefix + uidStr
			tx.Set(fdb.Key(uidToMemberKey), []byte(member))

			return memberHash, nil
		}

		// parse existing UID
		uid, err := strconv.ParseUint(string(val), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse existing UID: %w", err)
		}
		return uid, nil
	})
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to get or create UID: %w", err))
	}

	uid, ok := res.(uint64)
	if !ok {
		return 0, recordErr(span, fmt.Errorf("invalid UID type: %T", res))
	}

	return uid, nil
}

func (s *server) uidToMember(ctx context.Context, uid uint64) (string, error) {
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

func (s *server) handleRedisSetAdd(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisSetAdd") // nolint
	defer span.End()

	if len(args) < 2 {
		return "", recordErr(span, fmt.Errorf("SADD requires at least 2 arguments"))
	}

	setKey, err := extractStringArg(args[0])
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

	return formatInt(added), nil
}

func (s *server) redisSetAdd(ctx context.Context, setKey string, members []string) (int64, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetAdd") // nolint
	defer span.End()

	if len(members) == 0 {
		return 0, nil
	}

	// get or create UIDs for all members concurrently
	r := concurrent.New[string, uint64]()

	uids, err := r.Do(ctx, members, func(member string) (uint64, error) {
		return s.getUID(ctx, member)
	})
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to get UIDs for members: %w", err))
	}

	res, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		// Get the Bitmap if it exists
		bitmapKey := setPrefix + setKey
		blob, err := s.readLargeObject(tx, bitmapKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read large object: %w", err)
		}

		// If the key doesn't exist, create a new bitmap
		var bitmap *roaring.Bitmap
		if len(blob) == 0 {
			bitmap = roaring.New()
		} else if len(blob) > 0 {
			bitmap = roaring.New()
			if err := bitmap.UnmarshalBinary(blob); err != nil {
				return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
			}
		}

		// Add the new UIDs to the bitmap
		bitmap.AddMany(uids)

		// serialize and store the updated bitmap as a large object
		data, err := bitmap.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal bitmap: %w", err)
		}
		if err := s.writeLargeObject(tx, bitmapKey, data); err != nil {
			return nil, fmt.Errorf("failed to write large object: %w", err)
		}

		return int64(len(uids)), nil
	})
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to add members to set: %w", err))
	}

	added, ok := res.(int64)
	if !ok {
		return 0, recordErr(span, fmt.Errorf("invalid added type: %T", res))
	}

	return added, nil
}

func (s *server) handleRedisSetRemove(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisSetRemove") // nolint
	defer span.End()

	if len(args) < 2 {
		return "", recordErr(span, fmt.Errorf("SREM requires at least 2 arguments"))
	}

	setKey, err := extractStringArg(args[0])
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

	return formatInt(removed), nil
}

func (s *server) redisSetRemove(ctx context.Context, setKey string, members []string) (int64, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetRemove") // nolint
	defer span.End()

	if len(members) == 0 {
		return 0, nil
	}

	// get or create UIDs for all members concurrently
	r := concurrent.New[string, uint64]()

	uids, err := r.Do(ctx, members, func(member string) (uint64, error) {
		return s.getUID(ctx, member)
	})
	if err != nil {
		return 0, recordErr(span, fmt.Errorf("failed to get UIDs for members: %w", err))
	}

	res, err := s.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		// Get the Bitmap if it exists
		bitmapKey := setPrefix + setKey
		blob, err := s.readLargeObject(tx, bitmapKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read large object: %w", err)
		}

		if len(blob) == 0 {
			// set does not exist, nothing to remove
			return nil, nil
		}

		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(blob); err != nil {
			return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
		}

		removed := int64(0)
		for _, uid := range uids {
			if bitmap.Contains(uid) {
				bitmap.Remove(uid)
				removed++
			}
		}

		// serialize and store the updated bitmap
		data, err := bitmap.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal bitmap: %w", err)
		}
		if err := s.writeLargeObject(tx, bitmapKey, data); err != nil {
			return nil, fmt.Errorf("failed to write large object: %w", err)
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

func (s *server) handleRedisSetIsMember(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisSetIsMember") // nolint
	defer span.End()

	if len(args) != 2 {
		return "", recordErr(span, fmt.Errorf("SISMEMBER requires exactly 2 arguments"))
	}

	setKey, err := extractStringArg(args[0])
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

	return formatBoolAsInt(isMember), nil
}

func (s *server) redisSetIsMember(ctx context.Context, setKey string, member string) (bool, error) {
	ctx, span := s.tracer.Start(ctx, "redisSetIsMember") // nolint
	defer span.End()

	uid, err := s.getUID(ctx, member)
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

func (s *server) handleRedisSetCard(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisSetCard") // nolint
	defer span.End()

	if len(args) != 1 {
		return "", recordErr(span, fmt.Errorf("SCARD requires exactly 1 argument"))
	}

	setKey, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	card, err := s.redisSetCard(ctx, setKey)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get set cardinality: %w", err))
	}

	return formatInt(card), nil
}

func (s *server) redisSetCard(ctx context.Context, setKey string) (int64, error) {
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

func (s *server) handleRedisSetMembers(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisSetMembers") // nolint
	defer span.End()

	if len(args) != 1 {
		return "", recordErr(span, fmt.Errorf("SMEMBERS requires exactly 1 argument"))
	}

	setKey, err := extractStringArg(args[0])
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
	}

	members, err := s.redisSetMembers(ctx, setKey)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to get set members: %w", err))
	}

	return formatArrayOfBulkStrings(members), nil
}

func (s *server) redisSetMembers(ctx context.Context, setKey string) ([]string, error) {
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

func (s *server) handleRedisSetInter(ctx context.Context, args []resp.Value) (string, error) {
	ctx, span := s.tracer.Start(ctx, "handleRedisSetInter") // nolint
	defer span.End()

	if len(args) < 1 {
		return "", recordErr(span, fmt.Errorf("SINTER requires at least 1 argument"))
	}

	var setKeys []string
	for _, arg := range args {
		setKey, err := extractStringArg(arg)
		if err != nil {
			return "", recordErr(span, fmt.Errorf("failed to parse set key argument: %w", err))
		}
		setKeys = append(setKeys, setKey)
	}

	members, err := s.redisSetInter(ctx, setKeys)
	if err != nil {
		return "", recordErr(span, fmt.Errorf("failed to compute set intersection: %w", err))
	}

	return formatArrayOfBulkStrings(members), nil
}

func (s *server) redisSetInter(ctx context.Context, setKeys []string) ([]string, error) {
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
