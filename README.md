# KVDB

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

For instance, in one shell, you could run `just run kvdb run`, then in another, you could do `redis-cli set hello world && redis-cli get hello`.
