use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use clap::Parser;
use futures_util::{StreamExt, TryStreamExt, stream};
use indicatif::ProgressStyle;
use serde::Deserialize;
use serde_json::Value;
use sqlx::PgPool;
use tracing::{info, warn};
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Import Mindlamp sensor JSON files into `mindlamp.gps_observations`.
///
/// Only `lamp.gps` sensor entries are ingested. Required environment
/// variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Sensor JSON files to ingest. Glob patterns are supported
    /// (for example, '/data/.../raw/*/phone/*.json').
    #[arg(required = true)]
    files: Vec<String>,

    /// Number of files ingested concurrently.
    #[arg(short, long, default_value_t = 8)]
    jobs: usize,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long, default_value_t = 8)]
    max_connections: u32,
}

#[derive(Debug, Deserialize)]
struct SensorEntry {
    data: Value,
    sensor: String,
    timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct GpsData {
    #[serde(default)]
    accuracy: Option<f64>,
    #[serde(default)]
    altitude: Option<f64>,
    latitude: f64,
    longitude: f64,
}

/// Extract (site_id, subject_id) from a PHOENIX path like
/// .../PROTECTED/PronetYA/raw/YA24706/phone/U7328719234_..._sensor_....json
fn site_subject_from_path(path: &Path) -> ImportResult<(String, String)> {
    let components: Vec<&str> = path
        .components()
        .map(|c| c.as_os_str().to_str().unwrap_or_default())
        .collect();

    let raw_pos = components
        .iter()
        .rposition(|c| *c == "raw")
        .ok_or_else(|| {
            format!(
                "could not find 'raw' directory component in path: {}",
                path.display()
            )
        })?;

    let site_id = components
        .get(raw_pos - 1)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("missing site directory before 'raw': {}", path.display()))?;
    let subject_id = components
        .get(raw_pos + 1)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("missing subject directory after 'raw': {}", path.display()))?;

    Ok((site_id.to_string(), subject_id.to_string()))
}

/// Expand each CLI argument into concrete file paths, expanding glob patterns.
fn expand_inputs(patterns: &[String]) -> ImportResult<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for pattern in patterns {
        let matched: Vec<PathBuf> = glob::glob(pattern)
            .map_err(|e| format!("invalid glob pattern '{pattern}': {e}"))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("glob error for pattern '{pattern}': {e}"))?;

        if matched.is_empty() {
            return Err(format!("no files matched pattern: {pattern}").into());
        }
        paths.extend(matched);
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

/// Parse one sensor JSON file and return its GPS rows as NDJSON for
/// `jsonb_to_recordset`.
fn gps_rows_from_file(path: &Path) -> ImportResult<(String, String, String)> {
    let (site_id, subject_id) = site_subject_from_path(path)?;

    let contents = fs::read_to_string(path)?;
    let entries: Vec<SensorEntry> = serde_json::from_str(&contents)
        .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;

    let mut rows = Vec::new();
    for entry in entries.into_iter().filter(|e| e.sensor == "lamp.gps") {
        let data: GpsData = serde_json::from_value(entry.data)
            .map_err(|e| format!("invalid lamp.gps data in {}: {e}", path.display()))?;
        rows.push(serde_json::json!({
            "timestamp_ms": entry.timestamp,
            "accuracy_m": data.accuracy,
            "altitude_m": data.altitude,
            "latitude": data.latitude,
            "longitude": data.longitude,
        }));
    }

    Ok((site_id, subject_id, serde_json::to_string(&rows)?))
}

async fn ingest_file(pool: &PgPool, path: &Path) -> ImportResult<u64> {
    let (site_id, subject_id, rows_json) = gps_rows_from_file(path)?;

    if rows_json == "[]" {
        warn!(file = %path.display(), "no GPS entries found");
        return Ok(0);
    }

    // All interpolated identifiers come from the parsed path components and
    // are bound as parameters below.
    let result = sqlx::query(
        r#"
        INSERT INTO mindlamp.gps_observations
            (site_id, subject_id, observed_at, accuracy_m, altitude_m, latitude, longitude, geom)
        SELECT
            $1,
            $2,
            to_timestamp(r.timestamp_ms / 1000.0),
            r.accuracy_m,
            r.altitude_m,
            r.latitude,
            r.longitude,
            ST_SetSRID(ST_MakePoint(r.longitude, r.latitude), 4326)
        FROM jsonb_to_recordset($3::jsonb)
            AS r(timestamp_ms bigint, accuracy_m double precision,
                 altitude_m double precision, latitude double precision,
                 longitude double precision)
        ON CONFLICT (site_id, subject_id, observed_at) DO NOTHING
        "#,
    )
    .bind(&site_id)
    .bind(&subject_id)
    .bind(&rows_json)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

#[tokio::main]
async fn main() -> ImportResult<()> {
    let indicatif_layer = IndicatifLayer::new();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
        .with(indicatif_layer)
        .init();

    let cli = Cli::parse();

    let discovery_span = tracing::info_span!("expanding_input_file_paths");
    discovery_span.pb_set_style(
        &ProgressStyle::with_template("{spinner:.green} {msg}").expect("valid template"),
    );
    discovery_span.pb_set_message("Expanding input file paths");
    let _discovery_enter = discovery_span.enter();
    let paths = expand_inputs(&cli.files);
    drop(_discovery_enter);
    drop(discovery_span);
    let paths = paths?;
    info!("Found {} files to ingest", paths.len());

    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections).await?;

    let progress_style = ProgressStyle::default_bar()
        .template("{span_child_prefix}{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
        .expect("valid template")
        .progress_chars("#>-");

    let ingest_span = tracing::info_span!("ingesting_gps_files");
    ingest_span.pb_set_style(&progress_style);
    ingest_span.pb_set_length(paths.len() as u64);
    ingest_span.pb_set_message("Ingesting GPS files");

    let total_rows = stream::iter(paths)
        .map(|path| {
            let pool = pool.clone();
            let ingest_span = ingest_span.clone();
            async move {
                let rows = ingest_file(&pool, &path).await?;
                info!(file = %path.display(), rows, "Ingested GPS observations");
                ingest_span.pb_inc(1);
                Ok::<u64, Box<dyn Error + Send + Sync>>(rows)
            }
        })
        .buffer_unordered(cli.jobs)
        .try_fold(0u64, |acc, rows| async move { Ok(acc + rows) })
        .await?;
    drop(ingest_span);

    info!("Ingestion complete: {} GPS rows inserted", total_rows);

    pool.close().await;
    Ok(())
}
