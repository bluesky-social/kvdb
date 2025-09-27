package redis

import (
	"bufio"
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
	t.SkipNow()

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

// order-independant array comparison
func requireArraysEqual(t *testing.T, expected []string, actualResp string) {
	val, err := resp.ParseRESP3Value(bufio.NewReader(strings.NewReader(actualResp)))
	require.NoError(t, err)
	require.Equal(t, resp.TypeArray, val.Type)

	valArr, ok := val.Value.([]resp.Value)
	require.True(t, ok)

	actual := map[string]any{}
	for _, v := range valArr {
		str, ok := v.Value.(string)
		require.True(t, ok)
		actual[str] = struct{}{}
	}

	require.Equal(t, len(expected), len(actual))

	// check that ever member exists in the array
	for _, exp := range expected {
		require.Contains(t, actual, exp)
	}
}

func TestSets(t *testing.T) {
	t.SkipNow()

	require := require.New(t)
	ctx := t.Context()
	sess := testSession(t)

	set1 := testutil.RandString(24)
	val1 := testutil.RandString(24)
	val2 := testutil.RandString(24)

	{
		// invalid arguments
		res := sess.handleCommand(ctx, &resp.Command{
			Name: "SADD",
			Args: []resp.Value{resp.SimpleStringValue(set1)},
		})
		requireRESPError(t, res)

		res = sess.handleCommand(ctx, &resp.Command{
			Name: "SADD",
			Args: []resp.Value{},
		})
		requireRESPError(t, res)
	}

	// add val1 to the set
	res := sess.handleCommand(ctx, &resp.Command{
		Name: "SADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue(val1),
		},
	})
	require.Equal(resp.FormatInt(1), res)

	// invalid arguments
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SISMEMBER",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	requireRESPError(t, res)

	// check that val1 is in the set and val2 is not
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SISMEMBER",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(val1),
		},
	})
	require.Equal(resp.FormatBoolAsInt(true), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SISMEMBER",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(val2),
		},
	})
	require.Equal(resp.FormatBoolAsInt(false), res)

	// check if an item is in a set that does not exist
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SISMEMBER",
		Args: []resp.Value{
			resp.SimpleStringValue("invalid"),
			resp.SimpleStringValue(val2),
		},
	})
	require.Equal(resp.FormatBoolAsInt(false), res)

	// check on the size of the set
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SCARD",
		Args: []resp.Value{resp.SimpleStringValue("invalid")},
	})
	require.Equal(resp.FormatInt(0), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SCARD",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	require.Equal(resp.FormatInt(1), res)

	// check member lists
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SMEMBERS",
		Args: []resp.Value{resp.SimpleStringValue("invalid")},
	})
	requireArraysEqual(t, []string{}, res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SMEMBERS",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	requireArraysEqual(t, []string{val1}, res)

	// add a second member to the set
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue(val2),
		},
	})
	require.Equal(resp.FormatInt(1), res)

	// check its members again
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SMEMBERS",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	requireArraysEqual(t, []string{val1, val2}, res)

	// invalid arguments
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SINTER",
		Args: []resp.Value{},
	})
	requireRESPError(t, res)

	// intersect a set with nothing
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SINTER",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	requireArraysEqual(t, []string{val1, val2}, res)

	// intersect a set with itself
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SINTER",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(set1),
		},
	})
	requireArraysEqual(t, []string{val1, val2}, res)

	// intersect against a set that does not exist
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SINTER",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue("invalid"),
		},
	})
	requireArraysEqual(t, []string{}, res)

	set2 := testutil.RandString(24)
	val3 := testutil.RandString(24)
	val4 := testutil.RandString(24)

	// create a second set with multiple keys at once
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set2),
			resp.BulkStringValue(val3),
			resp.BulkStringValue(val4),
		},
	})
	require.Equal(resp.FormatInt(2), res)

	// intersection between set1 and set2 should be zero
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SINTER",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(set2),
		},
	})
	requireArraysEqual(t, []string{}, res)

	// add some overlap between the sets
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set2),
			resp.BulkStringValue(val1),
		},
	})
	require.Equal(resp.FormatInt(1), res)

	// intersection should now contain one value
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SINTER",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(set2),
		},
	})
	requireArraysEqual(t, []string{val1}, res)

	// @TODO: fix unions
	// // union should be all values
	// res = sess.handleCommand(ctx, &resp.Command{
	// 	Name: "SUNION",
	// 	Args: []resp.Value{
	// 		resp.SimpleStringValue(set1),
	// 		resp.SimpleStringValue(set2),
	// 	},
	// })
	// requireArraysEqual(t, []string{val1, val2, val3, val4}, res)

	// // invalid arguments
	// res = sess.handleCommand(ctx, &resp.Command{
	// 	Name: "SUNION",
	// 	Args: []resp.Value{},
	// })
	// requireRESPError(t, res)

	// // union of one set is just the set
	// res = sess.handleCommand(ctx, &resp.Command{
	// 	Name: "SUNION",
	// 	Args: []resp.Value{
	// 		resp.SimpleStringValue(set2),
	// 	},
	// })
	// requireArraysEqual(t, []string{val3, val4}, res)

	// remove an item from the set and check that it no longer exists
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SREM",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(val1),
		},
	})
	require.Equal(resp.FormatBoolAsInt(true), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SISMEMBER",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(val1),
		},
	})
	require.Equal(resp.FormatBoolAsInt(false), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SMEMBERS",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	requireArraysEqual(t, []string{val2}, res)

	// invalid arguments
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SDIFF",
		Args: []resp.Value{},
	})
	requireRESPError(t, res)

	// diff with one item is just the set
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SDIFF",
		Args: []resp.Value{resp.SimpleStringValue(set2)},
	})
	requireArraysEqual(t, []string{val1, val3, val4}, res)

	// diff in one direction
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SDIFF",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(set2),
		},
	})
	requireArraysEqual(t, []string{val2}, res)

	// diff in the other direction
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SDIFF",
		Args: []resp.Value{
			resp.SimpleStringValue(set2),
			resp.SimpleStringValue(set1),
		},
	})
	requireArraysEqual(t, []string{val1, val3, val4}, res)

	// delete the whole set and check that it's gone
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "DEL",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	require.Equal(resp.FormatBoolAsInt(true), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SMEMBERS",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	requireArraysEqual(t, []string{}, res)
}
