# KVDB

[![CI](https://github.com/bluesky-social/kvdb/actions/workflows/ci.yml/badge.svg)](https://github.com/bluesky-social/kvdb/actions/workflows/ci.yml)

**NOTE:** This repository is under heavy development and is not suitable for general production use yet. Do not use!

An implementation of a subset of the redis command set backed persistently by FoundationDB.

Note that the name KVDB is temporary.

## Local Development

Ensure you have [go](https://go.dev), [just](https://github.com/casey/just), and the [foundation clients](https://github.com/apple/foundationdb/releases/tag/7.3.63) installed (you must use fdb version 7.3.63).

Then, run `just install-tools` once to ensure you have the local go tooling you need.

You may then stand up and tear down the local dev dependencies with:

```bash
# runs local dependencies in docker
just up

# cleans up when you're done developing
just down
```

You can then run any of the following:

```bash
# show all supported commands
just --list

# run the linter and all tests
just

# run the database (you can also launch it in the debugger in vs code)
just run kvdb run

# open a local shell to the fdb database
just fdbcli
```

For instance, in one shell, you could run `just run kvdb run`, then in another, you could do:

```
redis-cli
127.0.0.1:6379> auth admin admin
OK
127.0.0.1:6379> set hello world
OK
127.0.0.1:6379> get hello
"world"
127.0.0.1:6379> ACL SETUSER newusername on >password123
OK
```

## Keyspace Layout

FoundationDB provides strictly serializable transactions atop an ordered key-value store. The design of our key directory layout for redis is the following:

|Path|Description|
|-|-|
|`redis_v0/*`|All data related to the redis implementation atop foundationdb (if we implement other wire protocols in the future, we will also give them their own directory)|
|`redis_v0/_admin_user_initialized`|Whether or not an admin user has been created on the cluster via the CLI and env vars|
|`redis_v0/_user/<username>`|The directory of user protobuf objects, stored by username|
|`redis_v0/<user_id>/*`|Data for an individual redis user|
|`redis_v0/<user_id>/meta/<obj_id>`|Metadata protobuf object for bookkeeping of stored object blobs|
|`redis_v0/<user_id>/obj/<obj_id>`|Storage of data objects|
