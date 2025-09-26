package redis

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/kvdb/internal/testutil"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"github.com/stretchr/testify/require"
)

func testSession(t *testing.T) *session {
	err := fdb.APIVersion(730)
	require.NoError(t, err)

	db, err := fdb.OpenDatabase("../../../foundation.cluster")
	require.NoError(t, err)
	require.NotNil(t, db)

	return NewSession(&NewSessionArgs{
		FDB:  db,
		Conn: &bytes.Buffer{},
	})
}

func requireRESPError(t *testing.T, str string) {
	require.True(t, strings.HasPrefix(str, "-"))
}

func requireNoRESPError(t *testing.T, str string) {
	require.False(t, strings.HasPrefix(str, "-"))
}

func TestPing(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSession(t)

	res := sess.handleCommand(ctx, &resp.Command{
		Name: "PING",
	})
	require.Equal(resp.FormatSimpleString("PONG"), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "PING",
		Args: []resp.Value{resp.SimpleStringValue("hello")},
	})
	require.Equal(resp.FormatSimpleString("hello"), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "PING",
		Args: []resp.Value{
			resp.SimpleStringValue("hello"),
			resp.SimpleStringValue("world"),
		},
	})
	requireRESPError(t, res)
	require.True(strings.Contains(res, "incorrect number of arguments"))

}

func TestBasicCRUD(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSession(t)

	key := testutil.RandString(24)
	val := testutil.RandString(24)

	{
		// test operations on a key that does not yet exist
		res := sess.handleCommand(ctx, &resp.Command{
			Name: "GET",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		require.Equal(resp.FormatNil(), res)

		res = sess.handleCommand(ctx, &resp.Command{
			Name: "EXISTS",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		require.Equal(resp.FormatBoolAsInt(false), res)

		res = sess.handleCommand(ctx, &resp.Command{
			Name: "DEL",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		require.Equal(resp.FormatBoolAsInt(false), res)
	}

	checkValid := func() {
		// test entry retreival
		res := sess.handleCommand(ctx, &resp.Command{
			Name: "GET",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		require.Equal(resp.FormatBulkString(val), res)

		res = sess.handleCommand(ctx, &resp.Command{
			Name: "EXISTS",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		require.Equal(resp.FormatBoolAsInt(true), res)
	}

	{
		// test entry creation
		res := sess.handleCommand(ctx, &resp.Command{
			Name: "SET",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		requireRESPError(t, res)

		res = sess.handleCommand(ctx, &resp.Command{
			Name: "SET",
			Args: []resp.Value{
				resp.SimpleStringValue(key),
				resp.SimpleStringValue(val),
			},
		})
		require.Equal(resp.FormatSimpleString("OK"), res)

		checkValid()
	}

	{
		// test updating an existing item and check again
		res := sess.handleCommand(ctx, &resp.Command{
			Name: "SET",
			Args: []resp.Value{
				resp.SimpleStringValue(key),
				resp.SimpleStringValue(val),
			},
		})
		require.Equal(resp.FormatSimpleString("OK"), res)

		checkValid()
	}

	{
		// delete the item and ensure it's gone
		res := sess.handleCommand(ctx, &resp.Command{
			Name: "DEL",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		require.Equal(resp.FormatBoolAsInt(true), res)

		res = sess.handleCommand(ctx, &resp.Command{
			Name: "GET",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		require.Equal(resp.FormatNil(), res)

		res = sess.handleCommand(ctx, &resp.Command{
			Name: "EXISTS",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		require.Equal(resp.FormatBoolAsInt(false), res)

		// another deletion should inform the client that the item did not exist
		res = sess.handleCommand(ctx, &resp.Command{
			Name: "DEL",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		require.Equal(resp.FormatBoolAsInt(false), res)
	}
}

func TestIncrDecr(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSession(t)

	key := testutil.RandString(24)

	res := sess.handleCommand(ctx, &resp.Command{
		Name: "INCR",
		Args: []resp.Value{resp.SimpleStringValue(key)},
	})
	require.Equal(resp.FormatInt(1), res)

	for range 4 {
		res := sess.handleCommand(ctx, &resp.Command{
			Name: "INCR",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		requireNoRESPError(t, res)
	}

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "GET",
		Args: []resp.Value{resp.SimpleStringValue(key)},
	})
	require.Equal(resp.FormatBulkString("5"), res)

	for range 10 {
		res := sess.handleCommand(ctx, &resp.Command{
			Name: "DECR",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		requireNoRESPError(t, res)
	}

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "GET",
		Args: []resp.Value{resp.SimpleStringValue(key)},
	})
	require.Equal(resp.FormatBulkString("-5"), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "INCRBY",
		Args: []resp.Value{
			resp.SimpleStringValue(key),
			resp.BulkStringValue("10"),
		},
	})
	require.Equal(resp.FormatInt(5), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "DECRBY",
		Args: []resp.Value{
			resp.SimpleStringValue(key),
			resp.BulkStringValue("10"),
		},
	})
	require.Equal(resp.FormatInt(-5), res)
}
