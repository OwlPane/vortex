# Install

## `vx` CLI

The `vx` command-line tool lets you convert, inspect, browse, and query Vortex files from
the terminal. You have the following options:

1. Binstall (recommended) downloads a pre-built binary. Requires
[cargo-binstall](https://github.com/cargo-bins/cargo-binstall):

```bash
cargo binstall vortex-tui
```

2. pip also installs the Python library; see the [Python quickstart](python.rst) for library usage:

```bash
pip install vortex-data
```

3. uvx runs the CLI without installing. Requires [uv](https://docs.astral.sh/uv/):

```bash
uvx --from vortex-data vx --help
```

4. Cargo builds from source, which can be slow due to the large dependency tree:

```bash
cargo install vortex-tui
```

Verify the installation:

```bash
vx --help
```

## Using Vortex as a library

The `vx` CLI is the quickest way to get started, but Vortex can also be used as a library
for reading, writing, and manipulating compressed arrays programmatically. See the language
quickstarts for [Python](python.rst), [Rust](rust.rst), and [Java](java.md).

## Sample data

This quickstart uses a 1,000-row sample of the NYC Yellow Taxi trip dataset, stored as an Arrow
IPC file. More data is available from the
[NYC TLC trip record data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

```bash
curl -O https://docs.vortex.dev/_static/example.arrow
```
