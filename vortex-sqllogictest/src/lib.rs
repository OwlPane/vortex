// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Support library for the `sqllogictests` binary, which replays `.slt` files
//! against both DataFusion and DuckDB configured to read Vortex files.
//!
//! The binary itself lives in `bin/sqllogictests-runner.rs`; this crate exposes
//! the DataFusion and DuckDB [`sqllogictest::AsyncDB`] adapters
//! ([`datafusion_engine::DataFusion`], [`duckdb::DuckDB`]) and file discovery
//! [`utils`].

pub mod datafusion_engine;
pub mod duckdb;
pub mod normalize;
pub mod scratch;
pub mod utils;
