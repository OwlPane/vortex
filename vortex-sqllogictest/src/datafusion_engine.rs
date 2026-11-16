// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Apache Software Foundation (ASF)

//! A [`sqllogictest::AsyncDB`] driver for DataFusion.
//!
//! This is an adapted, trimmed copy of the DataFusion engine from the `datafusion-sqllogictest`
//! crate. That crate enables every default DataFusion feature, so depending on it would pull
//! file formats into the build that this workspace does not use. Only the pieces the runner needs
//! are kept: query execution, result normalization, and the value validator. Cell formatting
//! matches upstream so `.slt` files stay comparable between engines.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Decimal32Type;
use arrow_array::types::Decimal64Type;
use arrow_array::types::Decimal128Type;
use arrow_array::types::Decimal256Type;
use arrow_array::types::DecimalType;
use arrow_array::types::Float16Type;
use arrow_array::types::Float32Type;
use arrow_array::types::Float64Type;
use arrow_cast::display::ArrayFormatter;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Fields;
use arrow_schema::Schema;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use half::f16;
use itertools::Either;
use itertools::Itertools;
use sqllogictest::ColumnType;
use sqllogictest::DBOutput;
use sqllogictest::Normalizer;

/// The string sqllogictest uses for a NULL value.
const NULL_STR: &str = "NULL";

/// Column types reported to the sqllogictest runner.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DFColumnType {
    Boolean,
    DateTime,
    Integer,
    Float,
    Text,
    Timestamp,
    Another,
}

impl ColumnType for DFColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Boolean),
            'D' => Some(Self::DateTime),
            'I' => Some(Self::Integer),
            'P' => Some(Self::Timestamp),
            'R' => Some(Self::Float),
            'T' => Some(Self::Text),
            _ => Some(Self::Another),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Boolean => 'B',
            Self::DateTime => 'D',
            Self::Integer => 'I',
            Self::Timestamp => 'P',
            Self::Float => 'R',
            Self::Text => 'T',
            Self::Another => '?',
        }
    }
}

/// The output of one statement or query.
pub type DFOutput = DBOutput<DFColumnType>;

/// Errors raised while running a statement against DataFusion.
#[derive(Debug, thiserror::Error)]
pub enum DFSqlLogicTestError {
    /// Error from DataFusion.
    #[error("DataFusion error: {}", .0.strip_backtrace())]
    DataFusion(#[from] DataFusionError),
    /// Error from arrow-rs.
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
}

type Result<T, E = DFSqlLogicTestError> = std::result::Result<T, E>;

/// Runs `.slt` statements against a DataFusion [`SessionContext`].
pub struct DataFusion {
    ctx: SessionContext,
}

impl DataFusion {
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusion {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DFOutput> {
        run_query(&self.ctx, sql).await
    }

    fn engine_name(&self) -> &str {
        "DataFusion"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    async fn shutdown(&mut self) {}
}

async fn run_query(ctx: &SessionContext, sql: &str) -> Result<DFOutput> {
    let df = ctx.sql(sql).await?;
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;
    let schema = plan.schema();

    let stream = execute_stream(plan, task_ctx)?;
    let types = convert_schema_to_types(stream.schema().fields());
    let results: Vec<RecordBatch> = collect(stream).await?;
    let rows = convert_batches(&schema, results)?;

    if rows.is_empty() && types.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        Ok(DBOutput::Rows { types, rows })
    }
}

/// Converts `batches` to the rows sqllogictest compares.
fn convert_batches(schema: &Schema, batches: Vec<RecordBatch>) -> Result<Vec<Vec<String>>> {
    let mut rows = vec![];
    for batch in batches {
        if !schema.contains(&batch.schema()) {
            return Err(DataFusionError::Internal(format!(
                "Schema mismatch. Previously had\n{schema:#?}\n\nGot:\n{:#?}",
                batch.schema()
            ))
            .into());
        }

        let new_rows = (0..batch.num_rows())
            .map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|col| cell_to_string(col, row))
                    .collect::<Result<Vec<String>>>()
            })
            .collect::<Result<Vec<Vec<String>>>>()?
            .into_iter()
            .flat_map(expand_row);
        rows.extend(new_rows);
    }
    Ok(rows)
}

/// Splits a last cell that holds newlines (such as an `EXPLAIN` plan) into one row per line.
///
/// Leading spaces become `-`, since sqllogictest ignores whitespace differences, and each line
/// is numbered so plan changes are easy to review.
fn expand_row(mut row: Vec<String>) -> impl Iterator<Item = Vec<String>> {
    let Some(cell) = row.pop() else {
        return Either::Left(std::iter::once(row));
    };
    let lines: Vec<_> = cell.split('\n').collect();
    if lines.len() < 2 {
        row.push(cell);
        return Either::Left(std::iter::once(row));
    }

    let new_lines: Vec<_> = lines
        .into_iter()
        .enumerate()
        .map(|(idx, line)| {
            let content = line.trim_start();
            let new_prefix = "-".repeat(line.len() - content.len());
            let line_num = idx + 1;
            vec![format!("{line_num:02}){new_prefix}{content}")]
        })
        .collect();

    Either::Right(std::iter::once(row).chain(new_lines))
}

/// Normalizes a single cell prior to comparison.
///
/// NULLs print as `NULL`, empty strings as `(empty)`, and floats are rounded to have a consistent
/// representation across engines.
fn cell_to_string(col: &ArrayRef, row: usize) -> Result<String> {
    if col.is_null(row) {
        return Ok(NULL_STR.to_string());
    }
    Ok(match col.data_type() {
        DataType::Null => NULL_STR.to_string(),
        DataType::Boolean => col.as_boolean().value(row).to_string(),
        DataType::Float16 => f16_to_str(col.as_primitive::<Float16Type>().value(row)),
        DataType::Float32 => f32_to_str(col.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => f64_to_str(col.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal32(_, scale) => {
            decimal_to_str::<Decimal32Type>(col.as_primitive::<Decimal32Type>().value(row), *scale)
        }
        DataType::Decimal64(_, scale) => {
            decimal_to_str::<Decimal64Type>(col.as_primitive::<Decimal64Type>().value(row), *scale)
        }
        DataType::Decimal128(_, scale) => decimal_to_str::<Decimal128Type>(
            col.as_primitive::<Decimal128Type>().value(row),
            *scale,
        ),
        DataType::Decimal256(_, scale) => decimal_to_str::<Decimal256Type>(
            col.as_primitive::<Decimal256Type>().value(row),
            *scale,
        ),
        DataType::LargeUtf8 => varchar_to_str(col.as_string::<i64>().value(row)),
        DataType::Utf8 => varchar_to_str(col.as_string::<i32>().value(row)),
        DataType::Utf8View => varchar_to_str(col.as_string_view().value(row)),
        DataType::Dictionary(..) => {
            let dict = col.as_any_dictionary();
            let key = dict.normalized_keys()[row];
            cell_to_string(dict.values(), key)?
        }
        _ => {
            let format_options = datafusion::config::FormatOptions {
                null: NULL_STR.to_string(),
                ..Default::default()
            };
            let arrow_format_options: arrow_cast::display::FormatOptions =
                (&format_options).try_into()?;
            ArrayFormatter::try_new(col.as_ref(), &arrow_format_options)?
                .value(row)
                .to_string()
        }
    })
}

/// Maps result columns to the column types sqllogictest reports.
fn convert_schema_to_types(columns: &Fields) -> Vec<DFColumnType> {
    columns
        .iter()
        .map(|f| match f.data_type() {
            DataType::Boolean => DFColumnType::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DFColumnType::Integer,
            DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..) => DFColumnType::Float,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DFColumnType::Text,
            DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) => {
                DFColumnType::DateTime
            }
            DataType::Timestamp(..) => DFColumnType::Timestamp,
            DataType::Dictionary(key_type, value_type) if key_type.is_integer() => {
                match value_type.as_ref() {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DFColumnType::Text,
                    _ => DFColumnType::Another,
                }
            }
            _ => DFColumnType::Another,
        })
        .collect()
}

fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        // Escape nulls so that they render in review tools.
        value.trim_end_matches('\n').replace('\u{0000}', "\\0")
    }
}

/// Formats a float through [`BigDecimal`], printing NaN without its platform-dependent sign.
fn float_to_str(value: impl ToString, is_nan: bool, infinite: Option<bool>) -> String {
    if is_nan {
        return "NaN".to_string();
    }
    match infinite {
        Some(true) => "Infinity".to_string(),
        Some(false) => "-Infinity".to_string(),
        None => {
            let text = value.to_string();
            BigDecimal::from_str(&text).map_or(text, |value| big_decimal_to_str(&value, None))
        }
    }
}

fn infinity_sign(is_infinite: bool, is_sign_positive: bool) -> Option<bool> {
    is_infinite.then_some(is_sign_positive)
}

fn f16_to_str(value: f16) -> String {
    float_to_str(
        value,
        value.is_nan(),
        infinity_sign(value.is_infinite(), value.is_sign_positive()),
    )
}

fn f32_to_str(value: f32) -> String {
    float_to_str(
        value,
        value.is_nan(),
        infinity_sign(value.is_infinite(), value.is_sign_positive()),
    )
}

fn f64_to_str(value: f64) -> String {
    float_to_str(
        value,
        value.is_nan(),
        infinity_sign(value.is_infinite(), value.is_sign_positive()),
    )
}

fn decimal_to_str<T: DecimalType>(value: T::Native, scale: i8) -> String {
    // The precision does not affect the formatted digits.
    let text = T::format_decimal(value, u8::MAX, scale);
    BigDecimal::from_str(&text).map_or(text, |value| {
        big_decimal_to_str(&value, Some(i64::from(scale)))
    })
}

/// Formats a [`BigDecimal`] as a plain string, rounded to `round_digits` (default 12) places.
fn big_decimal_to_str(value: &BigDecimal, round_digits: Option<i64>) -> String {
    value
        .round(round_digits.unwrap_or(12))
        .normalized()
        .to_plain_string()
}

/// Trims trailing whitespace from an expected value.
///
/// Trailing whitespace is usually stripped from `.slt` lines, so a test that covers it should
/// project another column to its right.
#[expect(
    clippy::ptr_arg,
    reason = "matches sqllogictest's normalizer signature"
)]
pub fn value_normalizer(s: &String) -> String {
    s.trim_end().to_string()
}

/// Validates actual rows against expected lines, honoring the `<slt:ignore>` marker.
pub fn df_value_validator(
    normalizer: Normalizer,
    actual: &[Vec<String>],
    expected: &[String],
) -> bool {
    const IGNORE_MARKER: &str = "<slt:ignore>";

    let normalized_expected = expected.iter().map(normalizer).collect_vec();
    let normalized_actual = actual
        .iter()
        .map(|strs| strs.iter().join(" ").trim_end().to_string())
        .collect_vec();

    if !expected.iter().any(|line| line.contains(IGNORE_MARKER)) {
        return normalized_actual == normalized_expected;
    }

    // Fragment-based matching on the full snapshot.
    let expected_snapshot = normalized_expected.join("\n");
    let actual_snapshot = normalized_actual.join("\n");
    let mut pos = 0;
    for (i, fragment) in expected_snapshot.split(IGNORE_MARKER).enumerate() {
        if fragment.is_empty() {
            continue;
        }
        let Some(idx) = actual_snapshot[pos..].find(fragment) else {
            return false;
        };
        // Text before the first fragment must not be skipped.
        if i == 0 && idx != 0 {
            return false;
        }
        pos += idx + fragment.len();
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignore_marker_does_not_skip_leading_text() {
        let actual = vec![vec!["foo bar baz".to_string()]];
        let expected = vec!["bar <slt:ignore>".to_string()];
        assert!(!df_value_validator(value_normalizer, &actual, &expected));
    }

    #[test]
    fn floats_round_to_twelve_places() {
        assert_eq!(f64_to_str(1.0 / 3.0), "0.333333333333");
        assert_eq!(f64_to_str(f64::NAN), "NaN");
        assert_eq!(f64_to_str(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(f32_to_str(2.5), "2.5");
    }

    #[test]
    fn decimals_keep_their_scale() {
        assert_eq!(decimal_to_str::<Decimal128Type>(12345, 2), "123.45");
        assert_eq!(decimal_to_str::<Decimal128Type>(1200, 2), "12");
    }
}
