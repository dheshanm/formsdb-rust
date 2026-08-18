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
use sqlx::{PgPool, Postgres, Transaction};
use tracing::{info, warn};
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type FormEvents = BTreeMap<String, BTreeMap<String, Map<String, Value>>>;

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

    /// Number of subjects parsed and written concurrently.
    #[arg(short, long, default_value_t = 8)]
    jobs: usize,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long)]
    max_connections: Option<u32>,
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
    source_mdate: DateTime<Utc>,
    forms: Vec<FormInsert>,
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
    let subject_id = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.split('.').next())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| format!("invalid subject JSON filename: {}", path.display()))?
        .to_owned();
    let source_mdate = DateTime::<Utc>::from(
        fs::metadata(&path)?
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH),
    );
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

async fn write_subject(pool: &PgPool, subject: SubjectImport) -> Result<usize, sqlx::Error> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;
    sqlx::query("DELETE FROM forms.forms WHERE subject_id = $1")
        .bind(&subject.subject_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO subjects (id, site_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
        .bind(&subject.subject_id)
        .bind(&subject.subject_id[..2.min(subject.subject_id.len())])
        .execute(&mut *tx)
        .await?;
    let count = subject.forms.len();
    for form in subject.forms {
        sqlx::query("INSERT INTO forms.forms (subject_id, form_name, event_name, form_data, source_mdate, variables_with_data, variables_without_data, total_variables, percent_complete) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
			.bind(&subject.subject_id).bind(form.form_name).bind(form.event_name).bind(form.form_data)
			.bind(subject.source_mdate).bind(form.variables_with_data).bind(form.variables_without_data)
			.bind(form.total_variables).bind(form.percent_complete).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(count)
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

    let style = ProgressStyle::default_bar().template("{span_child_prefix}{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}").expect("valid template").progress_chars("#>-");
    let span = tracing::info_span!("importing_redcap_jsons");
    let _enter = span.enter();
    span.pb_set_style(&style);
    span.pb_set_length(paths.len() as u64);
    let (subjects, form_rows) = stream::iter(paths)
        .map(|path| {
            let dictionary = std::sync::Arc::clone(&dictionary);
            async move {
                tokio::task::spawn_blocking(move || process_subject(path, &dictionary))
                    .await
                    .map_err(|error| -> Box<dyn Error + Send + Sync> { Box::new(error) })?
            }
        })
        .buffer_unordered(jobs)
        // Database work is asynchronous and I/O-bound, so it belongs on
        // Tokio rather than Rayon. `buffer_unordered` bounds simultaneous
        // transactions to `jobs` (and the pool independently caps them at
        // `max_connections`) while allowing completed parses to be written
        // without waiting for every subject to finish parsing.
        .map(|subject| {
            let pool = pool.clone();
            let span = span.clone();
            async move {
                let subject = subject?;
                span.pb_set_message(&format!("Writing subject {}", subject.subject_id));
                let count = match write_subject(&pool, subject).await {
                    Ok(count) => count,
                    Err(error) => {
                        warn!(%error, "failed to write subject");
                        0
                    }
                };
                span.pb_inc(1);
                Ok::<_, Box<dyn Error + Send + Sync>>(count)
            }
        })
        .buffer_unordered(jobs)
        .try_fold(
            (0usize, 0usize),
            |(subjects, form_rows), count| async move {
                Ok::<_, Box<dyn Error + Send + Sync>>((subjects + 1, form_rows + count))
            },
        )
        .await?;
    drop(_enter);
    drop(span);
    info!(subjects, form_rows, "Imported REDCap JSON forms");
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
}
