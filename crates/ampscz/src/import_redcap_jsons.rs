use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use ampscz::get_data_dictionary;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use clap::Parser;
use futures_util::{StreamExt, TryStreamExt, stream};
use indicatif::ProgressStyle;
use polars::prelude::DataFrame;
use serde_json::{Map, Number, Value};
use sqlx::{PgPool, Row};
use tracing::info;
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type FormEvents = BTreeMap<String, BTreeMap<String, Map<String, Value>>>;
const DEFAULT_LOG_FREQ: usize = 100;

/// Import REDCap JSON survey exports into `forms.forms`.
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// PHOENIX directory containing PROTECTED (for example, /data/.../Prescient/PHOENIX).
    #[arg(short, long)]
    data_root: PathBuf,

    /// REDCap network suffix in survey JSON file names.
    #[arg(long, default_value = "Prescient")]
    network: String,

    /// Number of subjects parsed concurrently.
    #[arg(short, long, default_value_t = 8)]
    jobs: usize,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long)]
    max_connections: Option<u32>,

    /// Reimport every discovered JSON file, ignoring stored source timestamps.
    #[arg(long)]
    force: bool,
}

#[derive(Debug)]
struct FormInsert {
    form_name: String,
    event_name: String,
    form_data: Value,
    variables_with_data: i32,
    variables_without_data: Option<i32>,
    total_variables: Option<i32>,
    percent_complete: Option<f64>,
}

#[derive(Debug)]
struct SubjectImport {
    subject_id: String,
    source_mdate: NaiveDateTime,
    forms: Vec<FormInsert>,
}

fn subject_id_from_path(path: &Path) -> ImportResult<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.split('.').next())
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("invalid subject JSON filename: {}", path.display()).into())
}

/// Return the source modification time at PostgreSQL's microsecond precision.
///
/// PostgreSQL `timestamp` values retain microseconds, whereas filesystem mtimes
/// may retain nanoseconds. Normalizing before comparison prevents an unchanged
/// file from being continually reimported due only to precision loss.
fn source_mdate(path: &Path) -> ImportResult<NaiveDateTime> {
    let modified = DateTime::<Utc>::from(
        fs::metadata(path)?
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH),
    );
    DateTime::from_timestamp_micros(modified.timestamp_micros())
        .map(|timestamp| timestamp.naive_utc())
        .ok_or_else(|| format!("invalid source modification time: {}", path.display()).into())
}

fn is_up_to_date(
    path: &Path,
    imported_mdates: &BTreeMap<String, NaiveDateTime>,
) -> ImportResult<bool> {
    let subject_id = subject_id_from_path(path)?;
    Ok(imported_mdates.get(&subject_id) == Some(&source_mdate(path)?))
}

/// Get the latest recorded source timestamp for each previously imported subject.
async fn imported_source_mdates(
    pool: &PgPool,
) -> Result<BTreeMap<String, NaiveDateTime>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT subject_id, MAX(source_mdate) AS source_mdate FROM forms.forms GROUP BY subject_id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let subject_id: String = row.try_get("subject_id").ok()?;
            let source_mdate: Option<NaiveDateTime> = row.try_get("source_mdate").ok()?;
            let source_mdate = source_mdate?;
            Some((subject_id, source_mdate))
        })
        .collect())
}

fn paths_to_import(
    paths: Vec<PathBuf>,
    imported_mdates: &BTreeMap<String, NaiveDateTime>,
    force: bool,
) -> ImportResult<Vec<PathBuf>> {
    paths
        .into_iter()
        .filter_map(|path| {
            if force {
                return Some(Ok(path));
            }
            match is_up_to_date(&path, imported_mdates) {
                Ok(true) => None,
                Ok(false) => Some(Ok(path)),
                Err(error) => Some(Err(error)),
            }
        })
        .collect()
}

fn subject_json_paths(
    data_root: &Path,
    network: &str,
    discovery_span: &tracing::Span,
) -> ImportResult<Vec<PathBuf>> {
    let protected = data_root.join("PROTECTED");
    let suffix = format!(".{network}.json");
    let mut paths = Vec::new();

    for site in fs::read_dir(&protected)? {
        discovery_span.pb_inc(1);
        let raw = site?.path().join("raw");
        if !raw.is_dir() {
            continue;
        }
        for subject in fs::read_dir(raw)? {
            discovery_span.pb_inc(1);
            let surveys = subject?.path().join("surveys");
            if !surveys.is_dir() {
                continue;
            }
            for file in fs::read_dir(surveys)? {
                let file = file?;
                if file.file_type()?.is_file()
                    && file.file_name().to_string_lossy().ends_with(&suffix)
                {
                    paths.push(file.path());
                }
            }
        }
    }
    paths.sort();
    Ok(paths)
}

fn dictionary_forms(data_dictionary: &DataFrame) -> ImportResult<BTreeMap<String, String>> {
    let fields = data_dictionary.column("field_name")?.str()?;
    let forms = data_dictionary.column("form_name")?.str()?;
    let mut dictionary = BTreeMap::new();

    for index in 0..data_dictionary.height() {
        if let (Some(field), Some(form)) = (fields.get(index), forms.get(index)) {
            dictionary.insert(field.to_owned(), form.to_owned());
        }
    }
    Ok(dictionary)
}

fn json_value(value: &str) -> Value {
    let value = value.trim();
    if let Ok(value) = value.parse::<i64>() {
        return Value::Number(value.into());
    }
    if let Ok(value) = value.parse::<f64>()
        && let Some(value) = Number::from_f64(value)
    {
        return Value::Number(value);
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Value::String(
            date.and_hms_opt(0, 0, 0)
                .unwrap()
                .to_string()
                .replace(' ', "T"),
        );
    }
    if let Ok(time) = NaiveTime::parse_from_str(value, "%H:%M") {
        return Value::String(
            NaiveDate::from_ymd_opt(1900, 1, 1)
                .unwrap()
                .and_time(time)
                .to_string()
                .replace(' ', "T"),
        );
    }
    if let Ok(datetime) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M") {
        return Value::String(datetime.to_string().replace(' ', "T"));
    }
    Value::String(value.to_owned())
}

fn json_columns_from_value(value: Value) -> ImportResult<BTreeMap<String, Vec<Value>>> {
    if let Some(rows) = value.as_array() {
        let mut names = BTreeSet::new();
        for row in rows {
            let row = row
                .as_object()
                .ok_or("a REDCap JSON array must contain only JSON objects")?;
            names.extend(row.keys().cloned());
        }

        return Ok(names
            .into_iter()
            .map(|name| {
                let values = rows
                    .iter()
                    .map(|row| {
                        row.as_object()
                            .and_then(|row| row.get(&name))
                            .cloned()
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                (name, values)
            })
            .collect());
    }

    let object = value
        .as_object()
        .ok_or("a REDCap JSON export must contain an object or an array of objects")?;
    let mut columns = BTreeMap::new();
    for (name, values) in object {
        let values = values
            .as_array()
            .ok_or_else(|| format!("column {name} must contain an array"))?;
        columns.insert(name.clone(), values.clone());
    }
    Ok(columns)
}

fn json_columns(path: &Path) -> ImportResult<BTreeMap<String, Vec<Value>>> {
    json_columns_from_value(serde_json::from_reader(fs::File::open(path)?)?)
        .map_err(|error| format!("{}: {error}", path.display()).into())
}

fn process_subject(
    path: PathBuf,
    dictionary: &BTreeMap<String, String>,
) -> ImportResult<SubjectImport> {
    let subject_id = subject_id_from_path(&path)?;
    let source_mdate = source_mdate(&path)?;
    let columns = json_columns(&path)?;
    let events = columns
        .get("redcap_event_name")
        .ok_or_else(|| format!("{} has no redcap_event_name column", path.display()))?;
    let form_variables =
        dictionary
            .iter()
            .fold(BTreeMap::<String, i32>::new(), |mut counts, (_, form)| {
                *counts.entry(form.clone()).or_default() += 1;
                counts
            });
    let mut forms: FormEvents = BTreeMap::new();

    for (variable, values) in &columns {
        if variable == "redcap_event_name" {
            continue;
        }
        let form_name = dictionary
            .get(variable)
            .cloned()
            .unwrap_or_else(|| "uncategorized".to_owned());
        for (index, value) in values.iter().enumerate() {
            let Some(value) = value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let Some(event) = events
                .get(index)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|event| !event.is_empty())
            else {
                continue;
            };
            forms
                .entry(form_name.clone())
                .or_default()
                .entry(event.to_owned())
                .or_default()
                .insert(variable.clone(), json_value(value));
        }
    }

    let mut inserts = Vec::new();
    for (form_name, events) in forms {
        for (event_name, form_data) in events {
            let variables_with_data = form_data.len() as i32;
            let (variables_without_data, total_variables, percent_complete) =
                if form_name == "uncategorized" {
                    (None, None, None)
                } else {
                    let total = *form_variables.get(&form_name).unwrap_or(&0);
                    let without_data = total - variables_with_data;
                    let percent = if total == 0 {
                        0.0
                    } else {
                        f64::from(variables_with_data) / f64::from(total) * 100.0
                    };
                    (Some(without_data), Some(total), Some(percent))
                };
            inserts.push(FormInsert {
                form_name: form_name.clone(),
                event_name,
                form_data: Value::Object(form_data),
                variables_with_data,
                variables_without_data,
                total_variables,
                percent_complete,
            });
        }
    }

    Ok(SubjectImport {
        subject_id,
        source_mdate,
        forms: inserts,
    })
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn subject_queries(subject: SubjectImport) -> ImportResult<Vec<String>> {
    let mut queries = Vec::with_capacity(subject.forms.len() + 2);
    let subject_id = sql_literal(&subject.subject_id);
    let site_id = sql_literal(&subject.subject_id[..2.min(subject.subject_id.len())]);
    queries.push(format!(
        "DELETE FROM forms.forms WHERE subject_id = {subject_id};"
    ));
    queries.push(format!(
        "INSERT INTO subjects (id, site_id) VALUES ({subject_id}, {site_id}) ON CONFLICT DO NOTHING;"
    ));

    let count = subject.forms.len();
    for form in subject.forms {
        let form_data = serde_json::to_string(&form.form_data)?;
        let variables_without_data = form
            .variables_without_data
            .map_or_else(|| "NULL".to_owned(), |value| value.to_string());
        let total_variables = form
            .total_variables
            .map_or_else(|| "NULL".to_owned(), |value| value.to_string());
        let percent_complete = form
            .percent_complete
            .map_or_else(|| "NULL".to_owned(), |value| value.to_string());
        queries.push(format!(
            "INSERT INTO forms.forms (subject_id, form_name, event_name, form_data, source_mdate, variables_with_data, variables_without_data, total_variables, percent_complete) VALUES ({subject_id}, {}, {}, {}::jsonb, {}, {}, {variables_without_data}, {total_variables}, {percent_complete});",
            sql_literal(&form.form_name),
            sql_literal(&form.event_name),
            sql_literal(&form_data),
            sql_literal(&subject.source_mdate.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
            form.variables_with_data,
        ));
    }
    debug_assert_eq!(queries.len(), count + 2);
    Ok(queries)
}

fn parse_log_frequency(raw: Option<&str>) -> ImportResult<usize> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_LOG_FREQ);
    };
    let frequency = raw
        .parse::<usize>()
        .map_err(|_| format!("LOG_FREQ must be a positive integer, got {raw:?}"))?;
    if frequency == 0 {
        return Err("LOG_FREQ must be greater than zero".into());
    }
    Ok(frequency)
}

fn log_frequency() -> ImportResult<usize> {
    match std::env::var("LOG_FREQ") {
        Ok(raw) => parse_log_frequency(Some(&raw)),
        Err(std::env::VarError::NotPresent) => parse_log_frequency(None),
        Err(error) => Err(format!("failed to read LOG_FREQ: {error}").into()),
    }
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
    let discovery_span = tracing::info_span!("discovering_redcap_jsons");
    discovery_span.pb_set_style(
        &ProgressStyle::with_template("{spinner:.green} {msg}").expect("valid template"),
    );
    discovery_span.pb_set_message("Discovering REDCap JSON files");
    let _discovery_enter = discovery_span.enter();
    let paths = subject_json_paths(&cli.data_root, &cli.network, &discovery_span);
    drop(_discovery_enter);
    drop(discovery_span);
    let paths = paths?;
    if paths.is_empty() {
        return Err(format!(
            "no .{}.json files found below {}",
            cli.network,
            cli.data_root.display()
        )
        .into());
    }

    let jobs = cli.jobs.max(1);
    let log_freq = log_frequency()?;
    info!(log_freq, "Logging REDCap JSON progress");
    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool =
        db::create_pool_with_options(&db_uri, cli.max_connections.unwrap_or(jobs as u32)).await?;
    let data_dictionary = get_data_dictionary(&pool)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let dictionary = dictionary_forms(&data_dictionary)?;
    if dictionary.is_empty() {
        return Err("forms.data_dictionary is empty".into());
    }
    let dictionary = std::sync::Arc::new(dictionary);

    let imported_mdates = if cli.force {
        BTreeMap::new()
    } else {
        imported_source_mdates(&pool).await?
    };
    let total_discovered = paths.len();
    let paths = paths_to_import(paths, &imported_mdates, cli.force)?;
    let skipped = total_discovered - paths.len();
    if skipped > 0 {
        info!(skipped, "Skipping unchanged REDCap JSON subjects");
    }
    if paths.is_empty() {
        info!("All discovered REDCap JSON subjects are up to date");
        pool.close().await;
        return Ok(());
    }

    let style = ProgressStyle::default_bar().template("{span_child_prefix}{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}").expect("valid template").progress_chars("#>-");
    let span = tracing::info_span!("importing_redcap_jsons");
    let _enter = span.enter();
    span.pb_set_style(&style);
    let total_to_import = paths.len();
    span.pb_set_length(total_to_import as u64);
    let mut subject_stream = stream::iter(paths)
        .map(|path| {
            let dictionary = std::sync::Arc::clone(&dictionary);
            async move {
                tokio::task::spawn_blocking(move || process_subject(path, &dictionary))
                    .await
                    .map_err(|error| -> Box<dyn Error + Send + Sync> { Box::new(error) })?
            }
        })
        .buffer_unordered(jobs);
    let mut subjects = Vec::with_capacity(total_to_import);
    while let Some(subject) = subject_stream.try_next().await? {
        subjects.push(subject);
        span.pb_inc(1);
        let processed = subjects.len();
        if processed % log_freq == 0 || processed == total_to_import {
            info!(processed, total = total_to_import, "Processed REDCap JSON subjects");
        }
    }
    let subject_count = subjects.len();
    let form_rows = subjects.iter().map(|subject| subject.forms.len()).sum::<usize>();
    let queries = subjects
        .into_iter()
        .map(subject_queries)
        .collect::<ImportResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    span.pb_set_message(&format!("Executing {} SQL queries", queries.len()));
    db::execute_queries_in_transaction(&pool, &queries).await?;
    span.pb_set_position(subject_count as u64);
    drop(_enter);
    drop(span);
    info!(subjects = subject_count, form_rows, queries = queries.len(), "Imported REDCap JSON forms");
    pool.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_redcap_values_like_the_python_importer() {
        assert_eq!(json_value(" 42 "), serde_json::json!(42));
        assert_eq!(json_value("-1.5"), serde_json::json!(-1.5));
        assert_eq!(
            json_value("2024-02-03"),
            serde_json::json!("2024-02-03T00:00:00")
        );
        assert_eq!(
            json_value("13:45"),
            serde_json::json!("1900-01-01T13:45:00")
        );
        assert_eq!(json_value("text"), serde_json::json!("text"));
    }

    #[test]
    fn accepts_redcap_record_arrays() {
        let columns = json_columns_from_value(serde_json::json!([
            {"redcap_event_name": "screening_arm_1", "bprs": "2"},
            {"redcap_event_name": "baseline_arm_1", "other": "value"}
        ]))
        .unwrap();

        assert_eq!(columns["redcap_event_name"].len(), 2);
        assert_eq!(columns["bprs"][0], serde_json::json!("2"));
        assert_eq!(columns["bprs"][1], Value::Null);
    }

    #[test]
    fn generates_queries_for_a_subject_as_one_ordered_sequence() {
        let queries = subject_queries(SubjectImport {
            subject_id: "GA76723".to_owned(),
            source_mdate: NaiveDate::from_ymd_opt(2026, 8, 19)
                .unwrap()
                .and_hms_micro_opt(12, 0, 0, 123_456)
                .unwrap(),
            forms: vec![FormInsert {
                form_name: "example's form".to_owned(),
                event_name: "baseline_arm_1".to_owned(),
                form_data: serde_json::json!({"text": "O'Brien"}),
                variables_with_data: 1,
                variables_without_data: Some(2),
                total_variables: Some(3),
                percent_complete: Some(33.333_333),
            }],
        })
        .unwrap();

        assert_eq!(queries.len(), 3);
        assert!(queries[0].starts_with("DELETE FROM forms.forms"));
        assert!(queries[1].starts_with("INSERT INTO subjects"));
        assert!(queries[2].contains("example''s form"));
        assert!(queries[2].contains("O''Brien"));
        assert!(queries[2].contains("2026-08-19 12:00:00.123456"));
    }

    #[test]
    fn filters_only_subjects_with_changed_source_timestamps() {
        let directory = std::env::temp_dir().join(format!(
            "import_redcap_jsons_test_{}",
            std::process::id()
        ));
        fs::create_dir_all(&directory).unwrap();
        let unchanged = directory.join("AA00001.Prescient.json");
        let changed = directory.join("AA00002.Prescient.json");
        fs::write(&unchanged, "{}").unwrap();
        fs::write(&changed, "{}").unwrap();
        let mut imported_mdates = BTreeMap::new();
        imported_mdates.insert("AA00001".to_owned(), source_mdate(&unchanged).unwrap());

        let paths = paths_to_import(
            vec![unchanged.clone(), changed.clone()],
            &imported_mdates,
            false,
        )
        .unwrap();

        assert_eq!(paths, vec![changed]);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn log_frequency_uses_default_when_unset() {
        assert_eq!(parse_log_frequency(None).unwrap(), DEFAULT_LOG_FREQ);
    }

    #[test]
    fn log_frequency_reads_env_value() {
        assert_eq!(parse_log_frequency(Some("25")).unwrap(), 25);
    }

    #[test]
    fn log_frequency_rejects_zero() {
        let error = parse_log_frequency(Some("0")).unwrap_err().to_string();
        assert!(error.contains("greater than zero"));
    }

    #[test]
    fn log_frequency_rejects_non_numeric_values() {
        let error = parse_log_frequency(Some("abc")).unwrap_err().to_string();
        assert!(error.contains("positive integer"));
    }
}
