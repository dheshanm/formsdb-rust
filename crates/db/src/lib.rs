use std::io::Cursor;
use std::num::NonZero;
use std::time::Duration;
use std::usize;

use futures_util::TryStreamExt;

use polars::prelude::*;
use sqlx::AssertSqlSafe;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tracing::{debug, error, info};

/// Default connection timeout in seconds.
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 30;

/// Maximum number of DataFrame rows serialized into one JSONB insert parameter.
///
/// Keeping this bounded prevents multi-million-row DataFrames from creating a
/// single multi-gigabyte PostgreSQL message that can exhaust a proxy or cause
/// the database connection to be closed.
const INSERT_BATCH_ROWS: usize = 10_000;

/// Controls how [`df_to_table`] handles a destination table that already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IfExists {
    /// Return an error if the destination table already exists.
    Fail,
    /// Keep the existing table and append the DataFrame rows.
    Append,
    /// Keep the existing table definition, remove all rows, and insert the DataFrame rows.
    ///
    /// Use this when database views or other objects depend on the table.
    Truncate,
    /// Drop the existing table, create it from the DataFrame schema, and insert rows.
    Replace,
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn postgres_type(dtype: &DataType) -> &'static str {
    match dtype {
        DataType::Boolean => "BOOLEAN",
        DataType::UInt8 | DataType::Int8 | DataType::UInt16 | DataType::Int16 => "SMALLINT",
        DataType::UInt32 | DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        // PostgreSQL has no unsigned integer types, and `NUMERIC` safely holds
        // every value in these Polars integer types.
        DataType::UInt64 | DataType::UInt128 | DataType::Int128 => "NUMERIC",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "DOUBLE PRECISION",
        DataType::String => "TEXT",
        DataType::Date => "DATE",
        DataType::Datetime(_, Some(_)) => "TIMESTAMPTZ",
        DataType::Datetime(_, None) => "TIMESTAMP",
        DataType::Time => "TIME",
        // JSON is a lossless representation for nested Polars values and for
        // data types without a direct PostgreSQL equivalent.
        _ => "JSONB",
    }
}

/// Create a database connection pool with custom max connections.
pub async fn create_pool_with_options(
    uri: &str,
    max_connections: u32,
) -> Result<PgPool, sqlx::Error> {
    let connection_string = uri;

    debug!("Creating connection pool to {}", connection_string);

    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
        .connect(&connection_string)
        .await
        .map_err(|e| {
            error!("Failed to create connection pool: {}", e);
            e
        })?;

    info!(
        "Created connection pool with {} max connections",
        max_connections
    );

    Ok(pool)
}

/// Execute multiple SQL queries in a single transaction.
pub async fn execute_queries_in_transaction(
    pool: &PgPool,
    queries: &[String],
) -> Result<(), sqlx::Error> {
    info!("Executing {} queries in transaction...", queries.len());

    let mut tx = pool.begin().await.map_err(|e| {
        error!("Failed to begin transaction: {}", e);
        e
    })?;

    for (i, query) in queries.iter().enumerate() {
        debug!("Executing query {}/{} in transaction", i + 1, queries.len());
        sqlx::query(AssertSqlSafe(query.as_str()))
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                error!("Failed to execute query: {}", e);
                e
            })?;
    }

    tx.commit().await.map_err(|e| {
        error!("Failed to commit transaction: {}", e);
        e
    })?;

    info!(
        "Successfully committed transaction with {} queries",
        queries.len()
    );
    Ok(())
}

/// Run a trusted SELECT query and return its results as a Polars DataFrame.
///
/// The supplied SQL must:
/// - be a SELECT-like query that can be used inside `FROM (<query>) AS q`
/// - not end in a semicolon
/// - be trusted application SQL, not user-provided SQL
///
/// JSONB columns become Polars Struct columns where possible.
pub async fn get_df(pool: &PgPool, sql: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // PostgreSQL serializes every selected row as a JSON object.
    //
    // Example input:
    //   SELECT subject_id, form_name, form_data FROM forms.forms
    //
    // Becomes:
    //   {"subject_id":"123","form_name":"...", "form_data":{...}}
    let wrapped_sql = format!(
        r#"
        SELECT row_to_json(q)::text AS _row_json
        FROM (
            {sql}
        ) AS q
        "#
    );

    let mut rows = sqlx::query(AssertSqlSafe(wrapped_sql.as_str())).fetch(pool);
    let mut ndjson = Vec::<u8>::new();

    while let Some(row) = rows.try_next().await? {
        let row_json: String = row.try_get("_row_json")?;

        ndjson.extend_from_slice(row_json.as_bytes());
        ndjson.push(b'\n');
    }

    // Polars cannot infer a schema from zero JSON rows. An empty result
    // therefore has no columns unless you provide a schema separately.
    if ndjson.is_empty() {
        return Ok(DataFrame::empty());
    }

    let df = JsonReader::new(Cursor::new(ndjson))
        .with_json_format(JsonFormat::JsonLines)
        // Important for arbitrary JSON keys. Use a smaller value if the
        // result sets are very large and JSON shape is stable.
        .infer_schema_len(Some(NonZero::new(usize::MAX).unwrap()))
        .finish()?;

    Ok(df)
}

/// Write a Polars DataFrame to a PostgreSQL table.
///
/// This is the Rust equivalent of the Python helper:
///
/// ```ignore
/// df_to_table(&pool, &df, "forms", "rpms_entry_status", IfExists::Replace).await?;
/// ```
///
/// The schema and table names are quoted as PostgreSQL identifiers. The target
/// table is created from the DataFrame schema for [`IfExists::Fail`] and
/// [`IfExists::Replace`]. For [`IfExists::Append`] and [`IfExists::Truncate`],
/// it must already exist and have columns compatible with the DataFrame.
/// `Truncate` preserves dependent views and other database objects while
/// replacing the table's contents. The operation runs in one transaction, and
/// rows are loaded through PostgreSQL's `jsonb_to_recordset`.
/// Nested and unsupported Polars types are stored as `JSONB`.
pub async fn df_to_table(
    pool: &PgPool,
    df: &DataFrame,
    schema: &str,
    table_name: &str,
    if_exists: IfExists,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if df.width() == 0 {
        return Err("cannot write a DataFrame with no columns".into());
    }

    let qualified_table = format!(
        "{}.{}",
        quote_identifier(schema),
        quote_identifier(table_name)
    );
    let mut tx = pool.begin().await?;

    // All interpolated fragments below are generated exclusively by
    // `quote_identifier` and `postgres_type`, rather than raw SQL input.
    sqlx::query(AssertSqlSafe(format!(
        "CREATE SCHEMA IF NOT EXISTS {}",
        quote_identifier(schema)
    )))
    .execute(&mut *tx)
    .await?;

    match if_exists {
        IfExists::Fail => {
            let exists: bool =
                sqlx::query_scalar("SELECT to_regclass(format('%I.%I', $1, $2)) IS NOT NULL")
                    .bind(schema)
                    .bind(table_name)
                    .fetch_one(&mut *tx)
                    .await?;
            if exists {
                return Err(format!("table {schema}.{table_name} already exists").into());
            }
        }
        IfExists::Append => {}
        IfExists::Truncate => {
            sqlx::query(AssertSqlSafe(format!("TRUNCATE TABLE {qualified_table}")))
                .execute(&mut *tx)
                .await?;
        }
        IfExists::Replace => {
            sqlx::query(AssertSqlSafe(format!(
                "DROP TABLE IF EXISTS {qualified_table}"
            )))
            .execute(&mut *tx)
            .await?;
        }
    }

    if !matches!(if_exists, IfExists::Append | IfExists::Truncate) {
        let columns = df
            .columns()
            .iter()
            .map(|series| {
                format!(
                    "{} {}",
                    quote_identifier(series.name()),
                    postgres_type(series.dtype())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE {qualified_table} ({columns})"
        )))
        .execute(&mut *tx)
        .await?;
    }

    if df.height() > 0 {
        let record_definition = df
            .columns()
            .iter()
            .map(|series| {
                format!(
                    "{} {}",
                    quote_identifier(series.name()),
                    postgres_type(series.dtype())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let column_names = df
            .get_column_names()
            .iter()
            .map(|name| quote_identifier(name.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {qualified_table} ({column_names}) SELECT {column_names} FROM jsonb_to_recordset($1::jsonb) AS rows({record_definition})"
        );
        let batch_count = df.height().div_ceil(INSERT_BATCH_ROWS);
        for batch_index in 0..batch_count {
            let offset = batch_index * INSERT_BATCH_ROWS;
            let length = (df.height() - offset).min(INSERT_BATCH_ROWS);
            let mut json = Vec::new();
            let mut batch = df.slice(offset as i64, length);
            JsonWriter::new(&mut json)
                .with_json_format(JsonFormat::Json)
                .finish(&mut batch)?;

            sqlx::query(AssertSqlSafe(sql.clone()))
                .bind(String::from_utf8(json)?)
                .execute(&mut *tx)
                .await?;
            debug!(
                batch = batch_index + 1,
                batches = batch_count,
                rows = length,
                "Inserted DataFrame batch"
            );
        }
    }

    tx.commit().await?;
    info!("Wrote {} rows to {}.{}", df.height(), schema, table_name);
    Ok(())
}
