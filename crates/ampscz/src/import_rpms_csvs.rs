use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::SystemTime,
};

use ampscz::constants::{form_name_to_rpms_suffix, rpms_to_redcap_event, FORM_NAME_RPMS_SUFFIXES};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use clap::Parser;
use futures_util::{stream, StreamExt, TryStreamExt};
use serde_json::{Map, Number, Value};
use sqlx::{PgPool, Postgres, Transaction};
use tracing::{info, warn};

const IGNORED_METADATA_COLUMNS: &[&str] = &[
    "LastModifiedDate",
    "subjectkey",
    "interview_date",
    "interview_age",
    "gender",
    "visit",
];

/// Import all RPMS CSVs into the database.
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Data root containing PROTECTED.
    #[arg(short, long)]
    data_root: PathBuf,

    /// Number of subjects parsed and written concurrently.
    #[arg(short, long, default_value_t = 8)]
    jobs: usize,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long)]
    max_connections: Option<u32>,
}

#[derive(Debug)]
struct FormInsert {
    form_name: &'static str,
    event_name: String,
    form_data: Value,
    source_mdate: DateTime<Utc>,
    variables_with_data: i32,
    variables_without_data: i32,
    total_variables: i32,
    percent_complete: f64,
}

#[derive(Debug)]
struct SubjectImport {
    subject_id: String,
    forms: Vec<FormInsert>,
}

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn read_csv(path: &Path) -> ImportResult<Vec<BTreeMap<String, String>>> {
    let mut reader = csv::ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = reader.headers()?.clone();
    let mut records = Vec::new();

    for record in reader.records() {
        let record = record?;
        let mut values = BTreeMap::new();
        for (header, value) in headers.iter().zip(record.iter()) {
            values.insert(header.to_owned(), value.to_owned());
        }
        records.push(values);
    }
    Ok(records)
}

fn parse_rpms_datetime(value: &str) -> Option<DateTime<Utc>> {
    let naive = if value.len() == 10 {
        NaiveDate::parse_from_str(value, "%m/%d/%Y")
            .or_else(|_| NaiveDate::parse_from_str(value, "%d/%m/%Y"))
            .ok()?
            .and_hms_opt(0, 0, 0)?
    } else if value.len() > 10 {
        NaiveDateTime::parse_from_str(value, "%d/%m/%Y %I:%M:%S %p").ok()?
    } else {
        return None;
    };
    Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

fn value_to_json(value: &str) -> Value {
    let value = value.trim();
    if let Ok(number) = value.parse::<i64>() {
        return Value::Number(number.into());
    }
    if let Ok(number) = value.parse::<f64>() {
        if let Some(number) = Number::from_f64(number) {
            return Value::Number(number);
        }
    }
    if let Some(datetime) = parse_rpms_datetime(value) {
        return Value::String(datetime.to_rfc3339());
    }
    Value::String(value.to_owned())
}

fn metadata(row: &BTreeMap<String, String>) -> (i32, i32, i32, f64) {
    // Keep `total_variables` compatible with the Python importer: it counts
    // every original column, while the ignored columns do not affect counts.
    let total = row.len() as i32;
    let mut with_data = 0;
    let mut without_data = 0;

    for (column, value) in row {
        if IGNORED_METADATA_COLUMNS.contains(&column.as_str()) {
            continue;
        }
        if value.is_empty() || value == "-3" || value == "-9" {
            without_data += 1;
        } else {
            with_data += 1;
        }
    }

    let percent = if total == 0 { 0.0 } else { f64::from(with_data) / f64::from(total) * 100.0 };
    (with_data, without_data, total, percent)
}

fn cohort_from_criteria(surveys: &Path, subject_id: &str) -> ImportResult<Option<(String, i32)>> {
    let suffix = form_name_to_rpms_suffix("inclusionexclusion_criteria_review").expect("known form");
    let path = surveys.join(format!("{subject_id}_{suffix}"));
    if !path.exists() {
        return Ok(None);
    }

    let rows = read_csv(&path)?;
    let value = rows.first().and_then(|row| row.get("chrcrit_part"))
        .ok_or_else(|| format!("{} has no chrcrit_part", path.display()))?;
    match value.parse::<i32>()? {
        1 => Ok(Some(("CHR".to_owned(), 1))),
        2 => Ok(Some(("HC".to_owned(), 2))),
        other => Err(format!("unknown chrcrit_part {other} for subject {subject_id}").into()),
    }
}

fn latest_non_flat_mdate(path: &Path) -> ImportResult<Option<String>> {
    let non_flat_path = path.with_extension("");
    if !non_flat_path.exists() {
        warn!("non-flat form file not found: {}", non_flat_path.display());
        return Ok(None);
    }
    let latest = read_csv(&non_flat_path)?
        .into_iter()
        .filter_map(|row| row.get("LastModifiedDate").cloned())
        .filter_map(|value| parse_rpms_datetime(&value).map(|date| (date, value)))
        .max_by_key(|(date, _)| *date)
        .map(|(_, value)| value);
    Ok(latest)
}

fn process_subject(subject_path: PathBuf) -> ImportResult<SubjectImport> {
    let subject_id = subject_path.file_name().and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid subject directory: {}", subject_path.display()))?.to_owned();
    let surveys = subject_path.join("surveys");
    let mut cohort = cohort_from_criteria(&surveys, &subject_id)?;
    let mut forms = Vec::new();

    for &(form_name, suffix) in FORM_NAME_RPMS_SUFFIXES {
        let path = surveys.join(format!("{subject_id}_{suffix}"));
        if !path.exists() {
            continue;
        }

        let mut rows = read_csv(&path)?;
        if path.extension().is_some_and(|extension| extension == "flat") {
            if let Some(last_modified) = latest_non_flat_mdate(&path)? {
                for row in &mut rows {
                    row.insert("LastModifiedDate".to_owned(), last_modified.clone());
                }
            }
        }

        if form_name == "informed_consent_run_sheet" {
            rows.sort_by_key(|row| row.get("chric_consent_date")
                .and_then(|value| parse_rpms_datetime(value)));
            rows.truncate(1);
            if cohort.is_none() {
                let group = rows.first().and_then(|row| row.get("group"))
                    .ok_or_else(|| format!("{path:?} has no group"))?;
                cohort = match group.as_str() {
                    "UHR" => Some(("CHR".to_owned(), 1)),
                    "HealthyControl" => Some(("HC".to_owned(), 2)),
                    other => return Err(format!("{form_name} - unknown cohort: {other}").into()),
                };
            }
            for row in &mut rows {
                row.insert("chric_record_id".to_owned(), subject_id.clone());
            }
        }

        if form_name == "sociodemographics" {
            let (cohort_name, _) = cohort.as_ref()
                .ok_or_else(|| format!("{form_name} - unknown cohort for {subject_id}"))?;
            for row in &mut rows {
                let age = row.get("interview_age").cloned().unwrap_or_default();
                let column = if cohort_name == "CHR" { "chrdemo_age_mos_chr" } else { "chrdemo_age_mos_hc" };
                row.insert(column.to_owned(), age);
            }
        }

        if form_name == "coenrollment_form" {
            rows.truncate(1);
        }
        if form_name == "current_pharmaceutical_treatment_floating_med_125"
            || form_name == "current_pharmaceutical_treatment_floating_med_2650" {
            let target = if form_name.ends_with("125") { "chrpharm_date_mod" } else { "chrpharm_date_mod_2" };
            for row in &mut rows {
                if let Some(value) = row.get("LastModifiedDate").cloned() {
                    row.insert(target.to_owned(), value);
                }
            }
        }

        let source_mdate = DateTime::<Utc>::from(fs::metadata(&path)?.modified().unwrap_or(SystemTime::UNIX_EPOCH));
        let (_, arm) = cohort.as_ref().ok_or_else(|| format!("unknown cohort for subject {subject_id}"))?;
        for row in rows {
            let visit = row.get("visit").ok_or_else(|| format!("{path:?} has no visit"))?.parse::<i32>()?;
            let event = rpms_to_redcap_event(visit).ok_or_else(|| format!("unknown visit {visit} in {}", path.display()))?;
            let (with_data, without_data, total, percent) = metadata(&row);
            let form_data = row.into_iter()
                .filter(|(_, value)| !value.is_empty())
                .map(|(column, value)| (column, value_to_json(&value)))
                .collect::<Map<_, _>>();
            forms.push(FormInsert {
                form_name,
                event_name: format!("{event}_arm_{arm}"),
                form_data: Value::Object(form_data),
                source_mdate,
                variables_with_data: with_data,
                variables_without_data: without_data,
                total_variables: total,
                percent_complete: percent,
            });
        }
    }
    Ok(SubjectImport { subject_id, forms })
}

async fn write_subject(pool: &PgPool, subject: SubjectImport) -> Result<usize, sqlx::Error> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;
    sqlx::query("DELETE FROM forms.forms WHERE subject_id = $1")
        .bind(&subject.subject_id).execute(&mut *tx).await?;

    if subject.forms.is_empty() {
        sqlx::query("DELETE FROM subjects WHERE id = $1")
            .bind(&subject.subject_id).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(0);
    }

    sqlx::query("INSERT INTO subjects (id, site_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
        .bind(&subject.subject_id).bind(&subject.subject_id[..2.min(subject.subject_id.len())])
        .execute(&mut *tx).await?;
    let count = subject.forms.len();
    for form in subject.forms {
        sqlx::query(
            "INSERT INTO forms.forms (subject_id, form_name, event_name, form_data, source_mdate, variables_with_data, variables_without_data, total_variables, percent_complete) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(&subject.subject_id).bind(form.form_name).bind(form.event_name).bind(form.form_data)
        .bind(form.source_mdate).bind(form.variables_with_data).bind(form.variables_without_data)
        .bind(form.total_variables).bind(form.percent_complete).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(count)
}

fn subject_paths(data_root: &Path) -> ImportResult<Vec<PathBuf>> {
    info!("Looking for RPMS CSVs in {data_root:?}");
    let protected = data_root.join("PROTECTED");
    let mut paths = Vec::new();
    for site in fs::read_dir(protected)? {
        let raw = site?.path().join("raw");
        if raw.is_dir() {
            for subject in fs::read_dir(raw)? {
                let path = subject?.path();
                if path.is_dir() { paths.push(path); }
            }
        }
    }
    paths.sort();
    info!("Found {} RPMS CSV files", paths.len());
    Ok(paths)
}

#[tokio::main]
async fn main() -> ImportResult<()> {
    tracing_subscriber::fmt::init(); 
    let cli = Cli::parse();
    let paths = subject_paths(&cli.data_root)?;
    let jobs = cli.jobs.max(1);
    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.unwrap_or(jobs as u32)).await?;
    info!("Importing {} RPMS subjects with {jobs} concurrent workers", paths.len());

    let completed = Arc::new(AtomicUsize::new(0));
    let imported_forms = Arc::new(AtomicUsize::new(0));
    let subjects = stream::iter(paths)
        .map(|path| async move {
            tokio::task::spawn_blocking(move || process_subject(path))
                .await
                .map_err(|error| -> Box<dyn Error + Send + Sync> { Box::new(error) })?
        })
        .buffer_unordered(jobs)
        .try_collect::<Vec<_>>()
        .await?;

    stream::iter(subjects)
        .map(|subject| write_subject(&pool, subject))
        .buffer_unordered(jobs)
        .try_for_each(|count| {
            let completed = Arc::clone(&completed);
            let imported_forms = Arc::clone(&imported_forms);
            async move {
                let completed = completed.fetch_add(1, Ordering::Relaxed) + 1;
                let imported_forms = imported_forms.fetch_add(count, Ordering::Relaxed) + count;
                if completed % 100 == 0 { info!("Processed {completed} subjects ({imported_forms} form rows)"); }
                Ok(())
            }
        }).await?;
    pool.close().await;
    let completed = completed.load(Ordering::Relaxed);
    let imported_forms = imported_forms.load(Ordering::Relaxed);
    info!("Imported {imported_forms} form rows for {completed} subjects");
    Ok(())
}