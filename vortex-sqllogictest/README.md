# vortex-sqllogictest

This crate tests correctness of our DuckDB and DataFusion integrations. It runs
[.slt](https://github.com/risinglightdb/sqllogictest-rs) tests with our queries.

## Running tests

```sh
cargo test -p vortex-sqllogictest --test sqllogictests

# You can run only DuckDB tests...
cargo nextest run -p vortex-sqllogictest -E 'test(/slt::duckdb::/)'
# ... or a single test on both DuckDB and DataFusion
cargo nextest run -p vortex-sqllogictest -E 'test(strings)'

# You can also list all tests
cargo nextest list -p vortex-sqllogictest
```

## Writing tests

To run a test only for DuckDB or DataFusion, place test in `slt/duckdb` or
`slt/datafusion`. To run a query inside test only for DuckDB or DataFusion,
place `onlyif duckdb` or `onlyif datafusion` before query.

To match output by regex in DuckDB, use `<REGEX>:pattern` or `<!REGEX>:pattern`
in query result.

To regenerate .slt file to match current output, use `--complete`.

```sh
cargo test -p vortex-sqllogictest --test sqllogictests -- --complete
# Regenerate output for a specific test
cargo test -p vortex-sqllogictest --test sqllogictests -- --complete strings
```
