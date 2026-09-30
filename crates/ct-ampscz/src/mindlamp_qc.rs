use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use clap::Parser;
use futures_util::{StreamExt, stream};
use indicatif::ProgressStyle;
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use sqlx::{PgPool, types::Json};
use tracing::{debug, info, warn};
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const SENSOR_PREFIX: &str = "lamp.";
const UNKNOWN_SENSOR: &str = "unknown";

/// Compute per-file QC metrics for Mindlamp phone JSON files and upsert them
/// into `mindlamp.file_qc`.
///
/// File names must look like `<mindlamp_id>_<subject_id>_<activity|sensor>_YYYY_MM_DD.json`.
/// Files whose size and modification time match the stored QC row are skipped
/// without reading their contents (only a `stat` is issued), which keeps
/// re-runs cheap on network / object-storage mounts.
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Mindlamp JSON files to process. Glob patterns are supported
    /// (for example, '/data/.../raw/YA11006/phone/*.json').
    #[arg(required = true)]
    files: Vec<String>,

    /// Number of files processed concurrently.
    #[arg(short, long, default_value_t = 8)]
    jobs: usize,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long, default_value_t = 8)]
    max_connections: u32,

    /// Re-read and recompute every file, even if its size and modification
    /// time match the stored QC row.
    #[arg(long)]
    force: bool,
}

const INIT_QUERIES: &[&str] = &[
    r#"CREATE SCHEMA IF NOT EXISTS mindlamp;"#,
    r#"CREATE TABLE IF NOT EXISTS mindlamp.file_qc (
        mindlamp_id TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        data_date DATE NOT NULL,
        data_type TEXT NOT NULL CHECK (data_type IN ('activity', 'sensor')),
        qc_metrics JSONB NOT NULL,
        source_file_path TEXT NOT NULL,
        PRIMARY KEY (subject_id, mindlamp_id, data_type, data_date),
        FOREIGN KEY (subject_id) REFERENCES public.subjects(subject_id) ON DELETE CASCADE
    );"#,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DataType {
    Activity,
    Sensor,
}

impl DataType {
    fn as_str(self) -> &'static str {
        match self {
            DataType::Activity => "activity",
            DataType::Sensor => "sensor",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "activity" => Some(DataType::Activity),
            "sensor" => Some(DataType::Sensor),
            _ => None,
        }
    }
}

/// Identity of a Mindlamp file, parsed from its name. Mirrors the primary key
/// of `mindlamp.file_qc`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileKey {
    mindlamp_id: String,
    subject_id: String,
    data_type: DataType,
    /// UTC day covered by the file.
    data_date: NaiveDate,
}

/// Filesystem attributes used both as QC metrics and for change detection.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileState {
    file_size_bytes: u64,
    /// RFC 3339, UTC, second precision (as stored in `qc_metrics`).
    file_modified_at: String,
}

/// What is already recorded in the database for a file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredRow {
    state: Option<FileState>,
    source_file_path: String,
}

#[derive(Debug)]
struct FileQc {
    key: FileKey,
    qc_metrics: Value,
    source_file_path: String,
}

/// Result of checking a file against its stored row.
#[derive(Debug, PartialEq, Eq)]
enum Decision {
    /// Unchanged and recorded at the same path.
    Skip,
    /// New, changed, or found at a different path; contents must be read and
    /// QC recomputed.
    Ingest,
}

/// Work produced for one file after the (blocking) filesystem step.
#[derive(Debug)]
enum Outcome {
    Skipped,
    Ingested(FileQc),
}

/// Parse `U8475627989_YA11006_activity_2026_02_24.json` into its parts.
fn parse_file_name(path: &Path) -> ImportResult<FileKey> {
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("invalid file name: {}", path.display()))?;
    let stem = file_name
        .strip_suffix(".json")
        .ok_or_else(|| format!("expected a .json file: {file_name}"))?;

    let parts: Vec<&str> = stem.split('_').collect();
    let [mindlamp_id, subject_id, data_type, year, month, day] = parts[..] else {
        return Err(format!(
            "unexpected file name '{file_name}' \
             (expected <mindlamp_id>_<subject_id>_<activity|sensor>_YYYY_MM_DD.json)"
        )
        .into());
    };

    if mindlamp_id.is_empty() || subject_id.is_empty() {
        return Err(format!("missing mindlamp_id or subject_id in file name: {file_name}").into());
    }

    let data_type = DataType::parse(data_type).ok_or_else(|| {
        format!(
            "unknown data type '{data_type}' in file name '{file_name}' (expected activity or sensor)"
        )
    })?;

    let date_raw = format!("{year}_{month}_{day}");
    let data_date = NaiveDate::parse_from_str(&date_raw, "%Y_%m_%d")
        .map_err(|e| format!("invalid date '{date_raw}' in file name '{file_name}': {e}"))?;

    Ok(FileKey {
        mindlamp_id: mindlamp_id.to_owned(),
        subject_id: subject_id.to_owned(),
        data_type,
        data_date,
    })
}

fn format_modified_at(modified: DateTime<Utc>) -> String {
    modified.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// `stat` the file; does not read its contents.
fn file_state(path: &Path) -> ImportResult<FileState> {
    let metadata = fs::metadata(path)?;
    let modified: SystemTime = metadata.modified()?;
    Ok(FileState {
        file_size_bytes: metadata.len(),
        file_modified_at: format_modified_at(modified.into()),
    })
}

fn decide(
    stored: Option<&StoredRow>,
    current: &FileState,
    source_file_path: &str,
    force: bool,
) -> Decision {
    match stored {
        _ if force => Decision::Ingest,
        Some(StoredRow {
            state: Some(state),
            source_file_path: stored_path,
        }) if state == current && stored_path == source_file_path => Decision::Skip,
        _ => Decision::Ingest,
    }
}

/// Number of events in an activity file (a top-level JSON array).
fn count_activity_events(contents: &[u8]) -> ImportResult<u64> {
    let events: Vec<IgnoredAny> = serde_json::from_slice(contents)?;
    Ok(events.len() as u64)
}

/// Only the `sensor` field of a sensor entry is needed; `data` and
/// `timestamp` are skipped without being materialized.
#[derive(Deserialize)]
struct SensorKey {
    #[serde(default)]
    sensor: Option<String>,
}

/// Per-sensor entry counts, built while streaming over the top-level array so
/// large sensor files never become a `Vec` of entries.
struct SensorCounts(BTreeMap<String, u64>);

impl<'de> Deserialize<'de> for SensorCounts {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CountingVisitor;

        impl<'de> Visitor<'de> for CountingVisitor {
            type Value = SensorCounts;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an array of sensor entries")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut counts = BTreeMap::new();
                while let Some(entry) = seq.next_element::<SensorKey>()? {
                    let name = match entry.sensor {
                        Some(sensor) => sensor
                            .strip_prefix(SENSOR_PREFIX)
                            .map(str::to_owned)
                            .unwrap_or(sensor),
                        None => UNKNOWN_SENSOR.to_owned(),
                    };
                    *counts.entry(name).or_insert(0) += 1;
                }
                Ok(SensorCounts(counts))
            }
        }

        deserializer.deserialize_seq(CountingVisitor)
    }
}

/// Entry counts per sensor (with the `lamp.` prefix stripped).
fn count_sensor_events(contents: &[u8]) -> ImportResult<BTreeMap<String, u64>> {
    let SensorCounts(counts) = serde_json::from_slice(contents)?;
    Ok(counts)
}

fn build_qc_metrics(
    data_type: DataType,
    contents: &[u8],
    state: &FileState,
) -> ImportResult<Value> {
    let metrics = match data_type {
        DataType::Activity => json!({
            "num_events": count_activity_events(contents)?,
            "file_size_bytes": state.file_size_bytes,
            "file_modified_at": state.file_modified_at,
        }),
        DataType::Sensor => {
            let sensor_counts = count_sensor_events(contents)?;
            json!({
                "num_events": sensor_counts.values().sum::<u64>(),
                "sensor_counts": sensor_counts,
                "file_size_bytes": state.file_size_bytes,
                "file_modified_at": state.file_modified_at,
            })
        }
    };
    Ok(metrics)
}

/// Filesystem step for one file: `stat` it, compare with the stored row, and
/// only read + parse the contents when it is new or changed. CPU/IO bound;
/// run it on a blocking thread.
fn evaluate_file(
    path: &Path,
    key: FileKey,
    stored: Option<&StoredRow>,
    force: bool,
) -> ImportResult<Outcome> {
    let state = file_state(path)?;
    let source_file_path = std::path::absolute(path)?.display().to_string();

    match decide(stored, &state, &source_file_path, force) {
        Decision::Skip => Ok(Outcome::Skipped),
        Decision::Ingest => {
            let contents = fs::read(path)?;
            let qc_metrics = build_qc_metrics(key.data_type, &contents, &state)
                .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;
            Ok(Outcome::Ingested(FileQc {
                key,
                qc_metrics,
                source_file_path,
            }))
        }
    }
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

async fn init_db(pool: &PgPool) -> ImportResult<()> {
    info!("Initializing database with mindlamp schema and file QC table");
    let queries: Vec<String> = INIT_QUERIES.iter().map(|&q| q.to_string()).collect();
    db::execute_queries_in_transaction(pool, &queries).await?;
    Ok(())
}

/// (mindlamp_id, subject_id, data_type, data_date, file_size_bytes,
/// file_modified_at, source_file_path)
type StoredRowTuple = (
    String,
    String,
    String,
    NaiveDate,
    Option<i64>,
    Option<String>,
    String,
);

/// Load stored size / mtime / path for the given subjects, keyed by file.
async fn load_stored_rows(
    pool: &PgPool,
    subject_ids: &[String],
) -> ImportResult<HashMap<FileKey, StoredRow>> {
    let rows: Vec<StoredRowTuple> = sqlx::query_as(
        r#"
SELECT
    mindlamp_id,
    subject_id,
    data_type,
    data_date,
    (qc_metrics->>'file_size_bytes')::bigint,
    qc_metrics->>'file_modified_at',
    source_file_path
FROM mindlamp.file_qc
WHERE subject_id = ANY($1);
            "#,
    )
    .bind(subject_ids)
    .fetch_all(pool)
    .await?;

    let mut stored = HashMap::with_capacity(rows.len());
    for (mindlamp_id, subject_id, data_type, data_date, size, modified_at, source_file_path) in rows
    {
        let Some(data_type) = DataType::parse(&data_type) else {
            continue;
        };
        let state = match (size, modified_at) {
            (Some(size), Some(file_modified_at)) if size >= 0 => Some(FileState {
                file_size_bytes: size as u64,
                file_modified_at,
            }),
            _ => None,
        };
        stored.insert(
            FileKey {
                mindlamp_id,
                subject_id,
                data_type,
                data_date,
            },
            StoredRow {
                state,
                source_file_path,
            },
        );
    }
    Ok(stored)
}

async fn upsert_qc(pool: &PgPool, qc: &FileQc) -> ImportResult<()> {
    let query = r#"
INSERT INTO mindlamp.file_qc (
    mindlamp_id,
    subject_id,
    data_date,
    data_type,
    qc_metrics,
    source_file_path
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (subject_id, mindlamp_id, data_type, data_date) DO UPDATE SET
    qc_metrics = EXCLUDED.qc_metrics,
    source_file_path = EXCLUDED.source_file_path;
    "#;

    sqlx::query(query)
        .bind(&qc.key.mindlamp_id)
        .bind(&qc.key.subject_id)
        .bind(qc.key.data_date)
        .bind(qc.key.data_type.as_str())
        .bind(Json(&qc.qc_metrics))
        .bind(&qc.source_file_path)
        .execute(pool)
        .await?;

    Ok(())
}

async fn process_file(
    pool: &PgPool,
    path: PathBuf,
    key: FileKey,
    stored: Arc<HashMap<FileKey, StoredRow>>,
    force: bool,
) -> ImportResult<Outcome> {
    let outcome = tokio::task::spawn_blocking(move || {
        let stored_row = stored.get(&key).cloned();
        evaluate_file(&path, key, stored_row.as_ref(), force)
    })
    .await??;

    if let Outcome::Ingested(qc) = &outcome {
        upsert_qc(pool, qc).await?;
    }
    Ok(outcome)
}

#[derive(Debug, Default)]
struct Summary {
    ingested: usize,
    skipped: usize,
    failed: usize,
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

    let paths = expand_inputs(&cli.files)?;
    info!("Found {} files to process", paths.len());

    // File names are parsed up front (no IO) so stored rows can be loaded for
    // just the subjects being processed.
    let mut summary = Summary::default();
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        match parse_file_name(&path) {
            Ok(key) => files.push((path, key)),
            Err(e) => {
                warn!(file = %path.display(), error = %e, "Skipping file with unexpected name");
                summary.failed += 1;
            }
        }
    }
    let mut subject_ids: Vec<String> = files.iter().map(|(_, k)| k.subject_id.clone()).collect();
    subject_ids.sort();
    subject_ids.dedup();

    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;

    init_db(&pool).await?;

    let stored = if cli.force {
        HashMap::new()
    } else {
        load_stored_rows(&pool, &subject_ids).await?
    };
    info!(
        subjects = subject_ids.len(),
        stored_rows = stored.len(),
        force = cli.force,
        "Loaded existing file QC rows"
    );
    let stored = Arc::new(stored);

    let progress_style = ProgressStyle::default_bar()
        .template("{span_child_prefix}{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
        .expect("valid template")
        .progress_chars("#>-");

    let qc_span = tracing::info_span!("computing_mindlamp_qc");
    qc_span.pb_set_style(&progress_style);
    qc_span.pb_set_length(files.len() as u64);
    qc_span.pb_set_message("Computing Mindlamp file QC");

    let summary = stream::iter(files)
        .map(|(path, key)| {
            let pool = pool.clone();
            let stored = Arc::clone(&stored);
            let qc_span = qc_span.clone();
            async move {
                let display_path = path.display().to_string();
                let result = process_file(&pool, path, key, stored, cli.force).await;
                match &result {
                    Ok(Outcome::Skipped) => debug!(file = %display_path, "Unchanged; skipped"),
                    Ok(Outcome::Ingested(qc)) => info!(
                        file = %display_path,
                        data_type = qc.key.data_type.as_str(),
                        num_events = %qc.qc_metrics["num_events"],
                        "Recorded file QC"
                    ),
                    Err(e) => warn!(file = %display_path, error = %e, "Failed to record file QC"),
                }
                qc_span.pb_inc(1);
                result
            }
        })
        .buffer_unordered(cli.jobs.max(1))
        .fold(summary, |mut summary, result| async move {
            match result {
                Ok(Outcome::Skipped) => summary.skipped += 1,
                Ok(Outcome::Ingested(_)) => summary.ingested += 1,
                Err(_) => summary.failed += 1,
            }
            summary
        })
        .await;
    drop(qc_span);

    info!(
        ingested = summary.ingested,
        skipped_unchanged = summary.skipped,
        failed = summary.failed,
        "Mindlamp file QC complete"
    );

    pool.close().await;

    if summary.failed > 0 {
        return Err(format!(
            "{} file(s) failed QC ingestion; see warnings above",
            summary.failed
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(size: u64, modified: &str) -> FileState {
        FileState {
            file_size_bytes: size,
            file_modified_at: modified.to_owned(),
        }
    }

    fn stored(size: u64, modified: &str, path: &str) -> StoredRow {
        StoredRow {
            state: Some(state(size, modified)),
            source_file_path: path.to_owned(),
        }
    }

    #[test]
    fn test_parse_file_name_activity() {
        let key = parse_file_name(Path::new(
            "/data/raw/YA11006/phone/U8475627989_YA11006_activity_2026_02_24.json",
        ))
        .unwrap();
        assert_eq!(
            key,
            FileKey {
                mindlamp_id: "U8475627989".to_owned(),
                subject_id: "YA11006".to_owned(),
                data_type: DataType::Activity,
                data_date: NaiveDate::from_ymd_opt(2026, 2, 24).unwrap(),
            }
        );
    }

    #[test]
    fn test_parse_file_name_sensor() {
        let key = parse_file_name(Path::new("U8475627989_YA11006_sensor_2026_03_12.json")).unwrap();
        assert_eq!(key.data_type, DataType::Sensor);
        assert_eq!(key.data_date, NaiveDate::from_ymd_opt(2026, 3, 12).unwrap());
    }

    #[test]
    fn test_parse_file_name_errors() {
        // Unknown data type
        assert!(parse_file_name(Path::new("U1_YA11006_survey_2026_03_12.json")).is_err());
        // Wrong number of parts
        assert!(parse_file_name(Path::new("U1_YA11006_sensor_2026_03.json")).is_err());
        assert!(parse_file_name(Path::new("U1_YA11006_x_sensor_2026_03_12.json")).is_err());
        // Invalid date
        assert!(parse_file_name(Path::new("U1_YA11006_sensor_2026_02_30.json")).is_err());
        // Not a JSON file
        assert!(parse_file_name(Path::new("U1_YA11006_sensor_2026_03_12.csv")).is_err());
        // Empty ids
        assert!(parse_file_name(Path::new("_YA11006_sensor_2026_03_12.json")).is_err());
    }

    #[test]
    fn test_decide() {
        let t = "2026-03-12T05:00:00Z";
        let current = state(100, t);

        // New file
        assert_eq!(decide(None, &current, "/a/f.json", false), Decision::Ingest);
        // Unchanged, same path
        let row = stored(100, t, "/a/f.json");
        assert_eq!(
            decide(Some(&row), &current, "/a/f.json", false),
            Decision::Skip
        );
        // Unchanged, but moved (e.g. new mount point): re-ingested so the
        // stored source path is refreshed
        assert_eq!(
            decide(Some(&row), &current, "/mnt/blob/f.json", false),
            Decision::Ingest
        );
        // Force overrides everything
        assert_eq!(
            decide(Some(&row), &current, "/a/f.json", true),
            Decision::Ingest
        );
        // Size changed
        let row = stored(99, t, "/a/f.json");
        assert_eq!(
            decide(Some(&row), &current, "/a/f.json", false),
            Decision::Ingest
        );
        // Modification time changed
        let row = stored(100, "2026-03-12T05:00:01Z", "/a/f.json");
        assert_eq!(
            decide(Some(&row), &current, "/a/f.json", false),
            Decision::Ingest
        );
        // Stored row without usable size/mtime
        let row = StoredRow {
            state: None,
            source_file_path: "/a/f.json".to_owned(),
        };
        assert_eq!(
            decide(Some(&row), &current, "/a/f.json", false),
            Decision::Ingest
        );
    }

    #[test]
    fn test_count_activity_events() {
        let contents = br#"[
            {"activity": "a", "timestamp": 1, "temporal_slices": [{"item": "x"}]},
            {"activity": "b", "timestamp": 2, "temporal_slices": []}
        ]"#;
        assert_eq!(count_activity_events(contents).unwrap(), 2);
        assert_eq!(count_activity_events(b"[]").unwrap(), 0);
        assert!(count_activity_events(b"{}").is_err());
    }

    #[test]
    fn test_count_sensor_events() {
        let contents = br#"[
            {"data": {"x": 1.0, "y": 2.0, "z": 3.0}, "sensor": "lamp.accelerometer", "timestamp": 1},
            {"data": {"x": 1.0, "y": 2.0, "z": 3.0}, "sensor": "lamp.accelerometer", "timestamp": 2},
            {"data": {"latitude": 1.0, "longitude": 2.0}, "sensor": "lamp.gps", "timestamp": 3},
            {"data": {}, "sensor": "phone_state", "timestamp": 4},
            {"data": {}, "timestamp": 5}
        ]"#;
        let counts = count_sensor_events(contents).unwrap();
        let expected: BTreeMap<String, u64> = [
            ("accelerometer".to_owned(), 2),
            ("gps".to_owned(), 1),
            ("phone_state".to_owned(), 1),
            (UNKNOWN_SENSOR.to_owned(), 1),
        ]
        .into_iter()
        .collect();
        assert_eq!(counts, expected);
        assert!(count_sensor_events(b"[]").unwrap().is_empty());
        assert!(count_sensor_events(b"not json").is_err());
    }

    #[test]
    fn test_build_qc_metrics_sensor() {
        let contents = br#"[{"data": {}, "sensor": "lamp.gps", "timestamp": 1}, {"data": {}, "sensor": "lamp.gps", "timestamp": 2}]"#;
        let metrics = build_qc_metrics(
            DataType::Sensor,
            contents,
            &state(123, "2026-03-12T05:00:00Z"),
        )
        .unwrap();
        assert_eq!(
            metrics,
            json!({
                "num_events": 2,
                "sensor_counts": {"gps": 2},
                "file_size_bytes": 123,
                "file_modified_at": "2026-03-12T05:00:00Z",
            })
        );
    }

    #[test]
    fn test_evaluate_file_from_disk() {
        let dir = std::env::temp_dir().join(format!("mindlamp_qc_test_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("U8475627989_YA11006_activity_2026_02_24.json");
        let contents = br#"[{"activity": "a", "timestamp": 1}, {"activity": "b", "timestamp": 2}, {"activity": "c", "timestamp": 3}]"#;
        fs::write(&path, contents).unwrap();
        let key = parse_file_name(&path).unwrap();

        // First run: no stored row, so the file is read and QC computed.
        let Outcome::Ingested(qc) = evaluate_file(&path, key.clone(), None, false).unwrap() else {
            panic!("expected Ingested");
        };
        assert_eq!(qc.key.subject_id, "YA11006");
        assert_eq!(qc.key.data_type, DataType::Activity);
        assert_eq!(qc.qc_metrics["num_events"], 3);
        assert_eq!(qc.qc_metrics["file_size_bytes"], contents.len() as u64);
        assert!(qc.qc_metrics["file_modified_at"].is_string());
        assert!(qc.qc_metrics.get("sensor_counts").is_none());
        assert_eq!(qc.source_file_path, path.display().to_string());

        // Second run with the row that would have been stored: skipped.
        let row = StoredRow {
            state: Some(FileState {
                file_size_bytes: qc.qc_metrics["file_size_bytes"].as_u64().unwrap(),
                file_modified_at: qc.qc_metrics["file_modified_at"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            }),
            source_file_path: qc.source_file_path.clone(),
        };
        let outcome = evaluate_file(&path, key.clone(), Some(&row), false).unwrap();
        assert!(matches!(outcome, Outcome::Skipped));

        // File grows: re-ingested.
        fs::write(&path, br#"[{"activity": "a", "timestamp": 1}]"#).unwrap();
        let outcome = evaluate_file(&path, key, Some(&row), false).unwrap();
        fs::remove_dir_all(&dir).unwrap();
        let Outcome::Ingested(qc) = outcome else {
            panic!("expected Ingested after change");
        };
        assert_eq!(qc.qc_metrics["num_events"], 1);
    }
}
