package redis

import (
	"bufio"
	"bytes"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/kvdb/internal/testutil"
	"github.com/bluesky-social/kvdb/internal/types"
	"github.com/bluesky-social/kvdb/pkg/serde/resp"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func init() {
	// speed up tests
	bcryptCost = bcrypt.MinCost
}

func testSession(t *testing.T) *session {
	err := fdb.APIVersion(730)
	require.NoError(t, err)

	db, err := fdb.OpenDatabase("../../../foundation.cluster")
	require.NoError(t, err)

	dirs, err := InitDirectories(db)
	require.NoError(t, err)

	err = InitAdminUser(db, dirs, "admin", "admin")
	require.NoError(t, err)

	return NewSession(&NewSessionArgs{
		Conn: &bytes.Buffer{},
		FDB:  db,
		Dirs: dirs,
	})
}

func testSessionWithAuth(t *testing.T) *session {
	sess := testSession(t)

	// log the user in as admin
	res := sess.handleCommand(t.Context(), &resp.Command{
		Name: "AUTH",
		Args: []resp.Value{
			resp.SimpleStringValue("admin"),
			resp.SimpleStringValue("admin"),
		},
	})
	requireNoRESPError(t, res)

	return sess
}

func requireRESPError(t *testing.T, str string) {
	require.True(t, strings.HasPrefix(str, "-"), "a RESP error was expected, but got %q", str)
}

func requireNoRESPError(t *testing.T, str string) {
	require.False(t, strings.HasPrefix(str, "-"), "no RESP errors were expected, but got %q", str)
}

func TestPing(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSessionWithAuth(t)

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
}

func TestAuthentication(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSession(t) // create a session with no credentials set

	username := testutil.RandString(24)
	password := testutil.RandString(24)
	gtPass := ">" + password

	// spot check that a few commands require authentication
	for _, cmd := range []string{"GET", "SET", "DEL", "ACL"} {
		res := sess.handleCommand(ctx, &resp.Command{
			Name: cmd,
			Args: []resp.Value{},
		})
		requireRESPError(t, res)
		require.Contains(res, "auth")
	}

	requireUserNil := func() {
		sess.userMu.RLock()
		defer sess.userMu.RUnlock()
		require.Nil(sess.user)
	}

	res := sess.handleCommand(ctx, &resp.Command{
		Name: "AUTH",
		Args: []resp.Value{},
	})
	requireRESPError(t, res)
	requireUserNil()

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "AUTH",
		Args: []resp.Value{resp.SimpleStringValue(username)},
	})
	requireRESPError(t, res)
	requireUserNil()

	// invalid credentials
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "AUTH",
		Args: []resp.Value{
			resp.SimpleStringValue(username),
			resp.SimpleStringValue(password),
		},
	})
	requireRESPError(t, res)
	requireUserNil()

	// invalid because the user has not yet been created
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "AUTH",
		Args: []resp.Value{
			resp.SimpleStringValue(username),
			resp.SimpleStringValue(password),
		},
	})
	requireRESPError(t, res)
	requireUserNil()

	{
		adminSess := testSessionWithAuth(t)

		// check lots of invalid create user command arguments
		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{},
		})
		requireRESPError(t, res)

		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{
				resp.SimpleStringValue("invalid"),
				resp.SimpleStringValue(username),
				resp.SimpleStringValue("on"),
				resp.SimpleStringValue(gtPass),
			},
		})
		requireRESPError(t, res)

		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{
				resp.SimpleStringValue("SETUSER"),
				resp.SimpleStringValue(""),
				resp.SimpleStringValue("on"),
				resp.SimpleStringValue(gtPass),
			},
		})
		requireRESPError(t, res)

		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{
				resp.SimpleStringValue("SETUSER"),
				resp.SimpleStringValue("_invalid"),
				resp.SimpleStringValue("on"),
				resp.SimpleStringValue(gtPass),
			},
		})
		requireRESPError(t, res)

		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{
				resp.SimpleStringValue("SETUSER"),
				resp.SimpleStringValue("invalid username"),
				resp.SimpleStringValue("on"),
				resp.SimpleStringValue(gtPass),
			},
		})
		requireRESPError(t, res)

		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{
				resp.SimpleStringValue("SETUSER"),
				resp.SimpleStringValue(username),
				resp.SimpleStringValue("invalid"),
				resp.SimpleStringValue(gtPass),
			},
		})
		requireRESPError(t, res)

		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{
				resp.SimpleStringValue("SETUSER"),
				resp.SimpleStringValue(username),
				resp.SimpleStringValue("on"),
				resp.SimpleStringValue(""),
			},
		})
		requireRESPError(t, res)

		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{
				resp.SimpleStringValue("SETUSER"),
				resp.SimpleStringValue(username),
				resp.SimpleStringValue("on"),
				resp.SimpleStringValue("invalid password"),
			},
		})
		requireRESPError(t, res)

		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{
				resp.SimpleStringValue("SETUSER"),
				resp.SimpleStringValue(username),
				resp.SimpleStringValue("on"),
				resp.SimpleStringValue(password), // must start with ">"
			},
		})
		requireRESPError(t, res)

		// success!
		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{
				resp.SimpleStringValue("SETUSER"),
				resp.SimpleStringValue(username),
				resp.SimpleStringValue("on"),
				resp.SimpleStringValue(gtPass),
			},
		})
		requireNoRESPError(t, res)

		// attempting to create again should fail because the user already exists
		res = adminSess.handleCommand(ctx, &resp.Command{
			Name: "ACL",
			Args: []resp.Value{
				resp.SimpleStringValue("SETUSER"),
				resp.SimpleStringValue(username),
				resp.SimpleStringValue("on"),
				resp.SimpleStringValue(gtPass),
			},
		})
		requireRESPError(t, res)
	}

	// log in as the new user
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "AUTH",
		Args: []resp.Value{
			resp.SimpleStringValue(username),
			resp.SimpleStringValue(password),
		},
	})
	requireNoRESPError(t, res)
	func() {
		sess.userMu.RLock()
		defer sess.userMu.RUnlock()
		require.NotNil(sess.user)
	}()

	// now we should be able to run any of the commands in the DMZ
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "GET",
		Args: []resp.Value{resp.SimpleStringValue(testutil.RandString(24))},
	})
	require.Equal(resp.FormatNil(), res)

	// a non-admin user should not be able to create other users
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ACL",
		Args: []resp.Value{
			resp.SimpleStringValue("SETUSER"),
			resp.SimpleStringValue(username + "2"),
			resp.SimpleStringValue("on"),
			resp.SimpleStringValue(gtPass),
		},
	})
	requireRESPError(t, res)
}

func TestBasicCRUD(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSessionWithAuth(t)

	key := testutil.RandString(24)
	val := "val"

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

func TestGetAndSetLargeObjects(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSessionWithAuth(t)

	key := testutil.RandString(24)
	const size = 550_000
	payload := make([]byte, size)
	for ndx := range size {
		payload[ndx] = byte(rand.IntN(256))
	}

	res := sess.handleCommand(ctx, &resp.Command{
		Name: "SET",
		Args: []resp.Value{
			resp.SimpleStringValue(key),
			resp.BulkStringValue(string(payload)),
		},
	})
	require.Equal(resp.FormatSimpleString("OK"), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "GET",
		Args: []resp.Value{resp.SimpleStringValue(key)},
	})
	requireNoRESPError(t, res)

	val, err := resp.ParseRESP3Value(bufio.NewReader(strings.NewReader(res)))
	require.NoError(err)

	valStr, ok := val.Value.(string)
	require.True(ok)

	require.Equal(len(payload), len(valStr))
	require.Equal(string(payload), valStr)
}

func TestIncrDecr(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSessionWithAuth(t)

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

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "INCRBYFLOAT",
		Args: []resp.Value{
			resp.SimpleStringValue(key),
			resp.BulkStringValue("10.5"),
		},
	})
	require.Equal(resp.FormatBulkString("5.5"), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "INCRBYFLOAT",
		Args: []resp.Value{
			resp.SimpleStringValue(key),
			resp.BulkStringValue("5.0e3"),
		},
	})
	require.Equal(resp.FormatBulkString("5005.5"), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "INCRBYFLOAT",
		Args: []resp.Value{
			resp.SimpleStringValue(key),
			resp.BulkStringValue("-10.0e3"),
		},
	})
	require.Equal(resp.FormatBulkString("-4994.5"), res)
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

	// check that every member exists in the array
	for _, exp := range expected {
		require.Contains(t, actual, exp)
	}
}

func TestSets(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSessionWithAuth(t)

	set1 := testutil.RandString(24)
	val1 := "val1"
	val2 := "val2"

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

	// add val1 to the set
	res = sess.handleCommand(ctx, &resp.Command{
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

	// ensure that we can't ZADD to a non-ordered set
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue("100"),
			resp.BulkStringValue(val1),
		},
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
	requireNoRESPError(t, res)
	requireArraysEqual(t, []string{}, res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SMEMBERS",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	requireNoRESPError(t, res)
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
	requireNoRESPError(t, res)
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
	requireNoRESPError(t, res)
	requireArraysEqual(t, []string{val1, val2}, res)

	// intersect a set with itself
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SINTER",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(set1),
		},
	})
	requireNoRESPError(t, res)
	requireArraysEqual(t, []string{val1, val2}, res)

	// intersect against a set that does not exist
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SINTER",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue("invalid"),
		},
	})
	requireNoRESPError(t, res)
	requireArraysEqual(t, []string{}, res)

	set2 := testutil.RandString(24)
	val3 := "val3"
	val4 := "val4"

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
	requireNoRESPError(t, res)
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
	requireNoRESPError(t, res)
	requireArraysEqual(t, []string{val1}, res)

	// union should be all values
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SUNION",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(set2),
		},
	})
	requireNoRESPError(t, res)
	requireArraysEqual(t, []string{val1, val2, val3, val4}, res)

	// invalid arguments
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SUNION",
		Args: []resp.Value{},
	})
	requireRESPError(t, res)

	// union of one set is just the set
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SUNION",
		Args: []resp.Value{
			resp.SimpleStringValue(set2),
		},
	})
	requireNoRESPError(t, res)
	requireArraysEqual(t, []string{val1, val3, val4}, res)

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
	requireNoRESPError(t, res)
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
	requireNoRESPError(t, res)
	requireArraysEqual(t, []string{val1, val3, val4}, res)

	// diff in one direction
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SDIFF",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(set2),
		},
	})
	requireNoRESPError(t, res)
	requireArraysEqual(t, []string{val2}, res)

	// diff in the other direction
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SDIFF",
		Args: []resp.Value{
			resp.SimpleStringValue(set2),
			resp.SimpleStringValue(set1),
		},
	})
	requireNoRESPError(t, res)
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
	requireNoRESPError(t, res)
	requireArraysEqual(t, []string{}, res)

	// create a new set with one member, then delete that member,
	// which should blow away the whole set
	set3 := testutil.RandString(24)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set3),
			resp.BulkStringValue(val1),
		},
	})
	require.Equal(resp.FormatInt(1), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SREM",
		Args: []resp.Value{
			resp.SimpleStringValue(set3),
			resp.BulkStringValue(val1),
		},
	})
	require.Equal(resp.FormatInt(1), res)
}

func TestOrderedSets(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSessionWithAuth(t)

	set1 := testutil.RandString(24)
	val1 := testutil.RandString(24)
	score1 := "10"
	val2 := testutil.RandString(24)
	score2 := "20"
	val3 := testutil.RandString(24)

	// invalid arguments
	res := sess.handleCommand(ctx, &resp.Command{
		Name: "ZADD",
		Args: []resp.Value{},
	})
	requireRESPError(t, res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZADD",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	requireRESPError(t, res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(score1),
		},
	})
	requireRESPError(t, res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.SimpleStringValue(score1),
			resp.SimpleStringValue(val1),
			resp.SimpleStringValue(score2),
		},
	})
	requireRESPError(t, res)

	// add two vals to the set
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue(score1),
			resp.BulkStringValue(val1),
			resp.BulkStringValue(score2),
			resp.BulkStringValue(val2),
		},
	})
	require.Equal(resp.FormatInt(2), res)

	// should have two items since we're scanning the range inclusively
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue(score1),
			resp.BulkStringValue(score2),
		},
	})
	require.Equal(resp.FormatInt(2), res)

	// ensure that we can't SADD to an ordered set
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "SADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue(val1),
		},
	})
	requireRESPError(t, res)

	// should have two items and parse infinity values correctly
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("-inf"),
			resp.BulkStringValue("+inf"),
		},
	})
	require.Equal(resp.FormatInt(2), res)

	// should have one item
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("5"),
			resp.BulkStringValue("10"),
		},
	})
	require.Equal(resp.FormatInt(1), res)

	// should have zero items since they're out of range
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("5"),
			resp.BulkStringValue("9"),
		},
	})
	require.Equal(resp.FormatInt(0), res)

	// start range less than end range should have no results
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("5"),
			resp.BulkStringValue("0"),
		},
	})
	require.Equal(resp.FormatInt(0), res)

	// adding again should be a noop, then check the count
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue(score1),
			resp.BulkStringValue(val1),
			resp.BulkStringValue(score2),
			resp.BulkStringValue(val2),
		},
	})
	require.Equal(resp.FormatInt(0), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("-inf"),
			resp.BulkStringValue("+inf"),
		},
	})
	require.Equal(resp.FormatInt(2), res)

	// remove one member, then check the count
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZREMRANGEBYSCORE",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("0"),
			resp.BulkStringValue("19"),
		},
	})
	require.Equal(resp.FormatInt(1), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("-inf"),
			resp.BulkStringValue("+inf"),
		},
	})
	require.Equal(resp.FormatInt(1), res)

	// adding again should add one, then check the count
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue(score1),
			resp.BulkStringValue(val1),
			resp.BulkStringValue(score2),
			resp.BulkStringValue(val2),
		},
	})
	require.Equal(resp.FormatInt(1), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("-inf"),
			resp.BulkStringValue("+inf"),
		},
	})
	require.Equal(resp.FormatInt(2), res)

	// remove them all
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZREMRANGEBYSCORE",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("-inf"),
			resp.BulkStringValue("+inf"),
		},
	})
	require.Equal(resp.FormatInt(2), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("-inf"),
			resp.BulkStringValue("+inf"),
		},
	})
	require.Equal(resp.FormatInt(0), res)

	// add two items with the same score
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZADD",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue(score1),
			resp.BulkStringValue(val1),
			resp.BulkStringValue(score2),
			resp.BulkStringValue(val2),
			resp.BulkStringValue(score2),
			resp.BulkStringValue(val3),
		},
	})
	require.Equal(resp.FormatInt(3), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("-inf"),
			resp.BulkStringValue("+inf"),
		},
	})
	require.Equal(resp.FormatInt(3), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZREMRANGEBYSCORE",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("19"),
			resp.BulkStringValue("20"),
		},
	})
	require.Equal(resp.FormatInt(2), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("-inf"),
			resp.BulkStringValue("+inf"),
		},
	})
	require.Equal(resp.FormatInt(1), res)

	// delete should work
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "DEL",
		Args: []resp.Value{resp.SimpleStringValue(set1)},
	})
	require.Equal(resp.FormatInt(1), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "ZCOUNT",
		Args: []resp.Value{
			resp.SimpleStringValue(set1),
			resp.BulkStringValue("-inf"),
			resp.BulkStringValue("+inf"),
		},
	})
	require.Equal(resp.FormatInt(0), res)
}

func TestLists(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSessionWithAuth(t)

	list := testutil.RandString(24)
	val0 := "val0"
	val1 := "val1"
	val2 := "val2"
	val3 := "val3"
	val4 := "val4"

	// invalid args
	res := sess.handleCommand(ctx, &resp.Command{
		Name: "LLEN",
	})
	requireRESPError(t, res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LLEN",
		Args: []resp.Value{resp.SimpleStringValue(list)},
	})
	require.Equal(resp.FormatInt(0), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LPUSH",
		Args: []resp.Value{resp.SimpleStringValue(list)},
	})
	requireRESPError(t, res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LPUSH",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue(val2),
		},
	})
	require.Equal(resp.FormatInt(1), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LPUSH",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue(val1),
			resp.SimpleStringValue(val0),
		},
	})
	require.Equal(resp.FormatInt(2), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LLEN",
		Args: []resp.Value{resp.SimpleStringValue(list)},
	})
	require.Equal(resp.FormatInt(3), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "RPUSH",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue(val3),
			resp.SimpleStringValue(val4),
		},
	})
	require.Equal(resp.FormatInt(2), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LLEN",
		Args: []resp.Value{resp.SimpleStringValue(list)},
	})
	require.Equal(resp.FormatInt(5), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LINDEX",
		Args: []resp.Value{resp.SimpleStringValue(list)},
	})
	requireRESPError(t, res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LINDEX",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue("0"),
		},
	})
	require.Equal(resp.FormatBulkString(val0), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LINDEX",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue("1"),
		},
	})
	require.Equal(resp.FormatBulkString(val1), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LINDEX",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue("2"),
		},
	})
	require.Equal(resp.FormatBulkString(val2), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LINDEX",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue("3"),
		},
	})
	require.Equal(resp.FormatBulkString(val3), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LINDEX",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue("4"),
		},
	})
	require.Equal(resp.FormatBulkString(val4), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LINDEX",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue("5"),
		},
	})
	require.Equal(resp.FormatNil(), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LINDEX",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue("-1"),
		},
	})
	require.Equal(resp.FormatBulkString(val4), res)

	res = sess.handleCommand(ctx, &resp.Command{
		Name: "LINDEX",
		Args: []resp.Value{
			resp.SimpleStringValue(list),
			resp.SimpleStringValue("-2"),
		},
	})
	require.Equal(resp.FormatBulkString(val3), res)
}

func TestExpire(t *testing.T) {
	require := require.New(t)
	ctx := t.Context()
	sess := testSessionWithAuth(t)

	key := testutil.RandString(24)
	val := "test val"

	// create an entry
	res := sess.handleCommand(ctx, &resp.Command{
		Name: "SET",
		Args: []resp.Value{
			resp.SimpleStringValue(key),
			resp.SimpleStringValue(val),
		},
	})
	require.Equal(resp.FormatSimpleString("OK"), res)

	check := func(exists bool) {
		res = sess.handleCommand(ctx, &resp.Command{
			Name: "GET",
			Args: []resp.Value{resp.SimpleStringValue(key)},
		})
		if exists {
			require.Equal(resp.FormatBulkString(val), res)
		} else {
			require.Equal(resp.FormatNil(), res)
		}
	}

	// check that is was created successfully
	check(true)

	// set an expiration of that should not expire the
	// object for a long time (one million seconds)
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "EXPIRE",
		Args: []resp.Value{
			resp.SimpleStringValue(key),
			resp.SimpleStringValue("1000000"),
		},
	})
	require.Equal(resp.FormatInt(1), res)
	check(true)

	// set an expiration of -1, which should immediately expire the object
	res = sess.handleCommand(ctx, &resp.Command{
		Name: "EXPIRE",
		Args: []resp.Value{
			resp.SimpleStringValue(key),
			resp.SimpleStringValue("-1"),
		},
	})
	require.Equal(resp.FormatInt(1), res)
	check(false)

	// ensure the underlying object is actually deleted
	var (
		meta *types.ObjectMeta
		buf  []byte
	)
	_, err := sess.fdb.Transact(func(tx fdb.Transaction) (any, error) {
		var err error
		meta, buf, err = sess.getObject(ctx, tx, objectKindBasic, key)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	require.NoError(err)
	require.Nil(meta)
	require.Empty(buf)
}
