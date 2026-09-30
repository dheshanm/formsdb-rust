use std::{
    collections::BTreeMap,
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
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
use tracing::{info, warn};
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const SENSOR_PREFIX: &str = "lamp.";
const UNKNOWN_SENSOR: &str = "unknown";

/// Compute per-file QC metrics for Mindlamp phone JSON files and upsert them
/// into `mindlamp.file_qc`.
///
/// File names must look like `<mindlamp_id>_<subject_id>_<activity|sensor>_YYYY_MM_DD.json`.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

/// Identity of a Mindlamp file, parsed from its name.
#[derive(Debug, PartialEq, Eq)]
struct FileKey {
    mindlamp_id: String,
    subject_id: String,
    data_type: DataType,
    /// UTC day covered by the file.
    data_date: NaiveDate,
}

#[derive(Debug)]
struct FileQc {
    key: FileKey,
    qc_metrics: Value,
    source_file_path: String,
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

    let data_type = match data_type {
        "activity" => DataType::Activity,
        "sensor" => DataType::Sensor,
        other => {
            return Err(format!(
                "unknown data type '{other}' in file name '{file_name}' (expected activity or sensor)"
            )
            .into());
        }
    };

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
    file_size_bytes: u64,
    file_modified_at: DateTime<Utc>,
) -> ImportResult<Value> {
    let file_modified_at = file_modified_at.to_rfc3339_opts(SecondsFormat::Secs, true);
    let metrics = match data_type {
        DataType::Activity => json!({
            "num_events": count_activity_events(contents)?,
            "file_size_bytes": file_size_bytes,
            "file_modified_at": file_modified_at,
        }),
        DataType::Sensor => {
            let sensor_counts = count_sensor_events(contents)?;
            json!({
                "num_events": sensor_counts.values().sum::<u64>(),
                "sensor_counts": sensor_counts,
                "file_size_bytes": file_size_bytes,
                "file_modified_at": file_modified_at,
            })
        }
    };
    Ok(metrics)
}

/// Read one file from disk and compute its QC row. CPU/IO bound; run it on a
/// blocking thread.
fn build_file_qc(path: &Path) -> ImportResult<FileQc> {
    let key = parse_file_name(path)?;

    let metadata = fs::metadata(path)?;
    let modified: SystemTime = metadata.modified()?;
    let contents = fs::read(path)?;

    let qc_metrics = build_qc_metrics(key.data_type, &contents, metadata.len(), modified.into())
        .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;

    let source_file_path = std::path::absolute(path)?.display().to_string();

    Ok(FileQc {
        key,
        qc_metrics,
        source_file_path,
    })
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

async fn process_file(pool: &PgPool, path: PathBuf) -> ImportResult<FileQc> {
    let qc = tokio::task::spawn_blocking(move || build_file_qc(&path)).await??;
    upsert_qc(pool, &qc).await?;
    Ok(qc)
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

    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;

    init_db(&pool).await?;

    let progress_style = ProgressStyle::default_bar()
        .template("{span_child_prefix}{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
        .expect("valid template")
        .progress_chars("#>-");

    let qc_span = tracing::info_span!("computing_mindlamp_qc");
    qc_span.pb_set_style(&progress_style);
    qc_span.pb_set_length(paths.len() as u64);
    qc_span.pb_set_message("Computing Mindlamp file QC");

    let (ok, failed) = stream::iter(paths)
        .map(|path| {
            let pool = pool.clone();
            let qc_span = qc_span.clone();
            async move {
                let display_path = path.display().to_string();
                let result = process_file(&pool, path).await;
                match &result {
                    Ok(qc) => info!(
                        file = %display_path,
                        data_type = qc.key.data_type.as_str(),
                        num_events = %qc.qc_metrics["num_events"],
                        "Recorded file QC"
                    ),
                    Err(e) => warn!(file = %display_path, error = %e, "Failed to record file QC"),
                }
                qc_span.pb_inc(1);
                result.is_ok()
            }
        })
        .buffer_unordered(cli.jobs.max(1))
        .fold((0usize, 0usize), |(ok, failed), success| async move {
            if success {
                (ok + 1, failed)
            } else {
                (ok, failed + 1)
            }
        })
        .await;
    drop(qc_span);

    info!(ok, failed, "Mindlamp file QC complete");

    pool.close().await;

    if failed > 0 {
        return Err(format!("{failed} file(s) failed QC ingestion; see warnings above").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let contents =
            br#"[{"data": {}, "sensor": "lamp.gps", "timestamp": 1}, {"data": {}, "sensor": "lamp.gps", "timestamp": 2}]"#;
        let modified = DateTime::parse_from_rfc3339("2026-03-12T05:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let metrics = build_qc_metrics(DataType::Sensor, contents, 123, modified).unwrap();
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
    fn test_build_file_qc_from_disk() {
        let dir = std::env::temp_dir().join(format!("mindlamp_qc_test_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("U8475627989_YA11006_activity_2026_02_24.json");
        let contents = br#"[{"activity": "a", "timestamp": 1}, {"activity": "b", "timestamp": 2}, {"activity": "c", "timestamp": 3}]"#;
        fs::write(&path, contents).unwrap();

        let qc = build_file_qc(&path).unwrap();
        fs::remove_dir_all(&dir).unwrap();

        assert_eq!(qc.key.subject_id, "YA11006");
        assert_eq!(qc.key.data_type, DataType::Activity);
        assert_eq!(qc.qc_metrics["num_events"], 3);
        assert_eq!(qc.qc_metrics["file_size_bytes"], contents.len() as u64);
        assert!(qc.qc_metrics["file_modified_at"].is_string());
        assert!(qc.qc_metrics.get("sensor_counts").is_none());
        assert_eq!(qc.source_file_path, path.display().to_string());
    }
}
