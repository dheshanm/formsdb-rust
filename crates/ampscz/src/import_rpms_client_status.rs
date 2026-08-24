use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use clap::Parser;
use indicatif::ProgressStyle;
use polars::prelude::{Column, DataFrame};
use tracing::info;
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const CLIENT_STATUS_SUFFIX: &str = "_ClientStatus_AllDates.csv";
const CLIENT_STATUS_RAW_SUFFIX: &str = "_ClientStatusRawData.csv";

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type CsvRow = BTreeMap<String, String>;

/// Import RPMS ClientStatus and ClientStatusRawData CSVs.
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// PHOENIX directory containing PROTECTED data.
    #[arg(short, long)]
    data_root: PathBuf,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long, default_value_t = 1)]
    max_connections: u32,
}

fn get_survey_paths(data_root: &Path, suffix: &str) -> ImportResult<Vec<PathBuf>> {
    let mut paths = Vec::new();

    // `data_root` is the PHOENIX directory, matching the other Rust importers.
    let protected = data_root.join("PROTECTED");
    for rpms_site in fs::read_dir(protected)? {
        let rpms_site = rpms_site?;
        if !rpms_site.file_type()?.is_dir() {
            continue;
        }

        let raw = rpms_site.path().join("raw");
        if !raw.is_dir() {
            continue;
        }

        for subject in fs::read_dir(raw)? {
            let subject = subject?;
            if !subject.file_type()?.is_dir() {
                continue;
            }

            let surveys = subject.path().join("surveys");
            if !surveys.is_dir() {
                continue;
            }

            for file in fs::read_dir(surveys)? {
                let file = file?;
                if file.file_type()?.is_file()
                    && file.file_name().to_string_lossy().ends_with(suffix)
                {
                    paths.push(file.path());
                }
            }
        }
    }

    paths.sort();
    Ok(paths)
}

fn read_csv(path: &Path) -> ImportResult<Vec<CsvRow>> {
    let mut reader = csv::ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = reader.headers()?.clone();
    let mut rows = Vec::new();

    for record in reader.records() {
        let record = record?;
        let mut row = CsvRow::new();
        for (header, value) in headers.iter().zip(record.iter()) {
            row.insert(header.to_owned(), value.to_owned());
        }
        rows.push(row);
    }

    Ok(rows)
}

fn rename_column(row: &mut CsvRow, old: &str, new: &str) {
    if let Some(value) = row.remove(old) {
        row.insert(new.to_owned(), value);
    }
}

fn normalize_client_status_row(mut row: CsvRow) -> CsvRow {
    rename_column(&mut row, "subjectkey", "subject_id");
    row
}

fn raw_redcap_event(main_status: &str) -> &'static str {
    match main_status {
        "Consent Received (Enrolled)" => "consent",
        "Included" => "included",
        "Enrolled, Screening" => "screening",
        "Baseline" => "baseline",
        "Month 1" => "month_1",
        "Month 2" => "month_2",
        "Month 3" => "month_3",
        "Month 4" => "month_4",
        "Month 5" => "month_5",
        "Month 6" => "month_6",
        "Month 7" => "month_7",
        "Month 8" => "month_8",
        "Month 9" => "month_9",
        "Month 10" => "month_10",
        "Month 11" => "month_11",
        "Month 12" => "month_12",
        "Month 18" => "month_18",
        "Month 24" => "month_24",
        // Includes pre-screening and reference-date statuses, as in Python.
        _ => "other",
    }
}

fn normalize_client_status_raw_row(mut row: CsvRow) -> CsvRow {
    rename_column(&mut row, "subjectkey", "subject_id");
    rename_column(&mut row, "Main Status", "main_status");
    rename_column(&mut row, " Sub Status", "sub_status");
    rename_column(&mut row, "Status Date", "status_date");

    let event = row
        .get("main_status")
        .map_or("other", |status| raw_redcap_event(status));
    row.insert("redcap_event".to_owned(), event.to_owned());
    row
}

fn parse_status_date(value: &str) -> Option<NaiveDate> {
    let value = value.trim();
    [
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %I:%M:%S %p",
    ]
    .iter()
    .find_map(|format| {
        NaiveDate::parse_from_str(value, format)
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(value, format).map(|datetime| datetime.date())
            })
            .ok()
    })
}

fn non_empty_csv_value(value: &str) -> Option<&str> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn rows_to_dataframe(rows: &[CsvRow], parse_dates: bool) -> ImportResult<DataFrame> {
    let names = rows
        .iter()
        .flat_map(|row| row.keys().cloned())
        .collect::<BTreeSet<_>>();
    if names.is_empty() {
        return Err("ClientStatus CSVs contain no columns".into());
    }

    let columns = names
        .into_iter()
        .map(|name| {
            if parse_dates && name == "status_date" {
                let values = rows
                    .iter()
                    .map(|row| row.get(&name).and_then(|value| parse_status_date(value)))
                    .collect::<Vec<_>>();
                Column::new(name.into(), values)
            } else {
                let values = rows
                    .iter()
                    .map(|row| row.get(&name).and_then(|value| non_empty_csv_value(value)))
                    .collect::<Vec<_>>();
                Column::new(name.into(), values)
            }
        })
        .collect::<Vec<_>>();
    Ok(DataFrame::new(rows.len(), columns)?)
}

fn load_rows(
    paths: &[PathBuf],
    normalize: fn(CsvRow) -> CsvRow,
    read_span: &tracing::Span,
) -> ImportResult<Vec<CsvRow>> {
    let mut rows = Vec::new();
    for path in paths {
        rows.extend(read_csv(path)?.into_iter().map(normalize));
        read_span.pb_inc(1);
    }
    Ok(rows)
}

async fn import_table(
    pool: &sqlx::PgPool,
    paths: &[PathBuf],
    table_name: &str,
    normalize: fn(CsvRow) -> CsvRow,
    parse_dates: bool,
    progress_style: &ProgressStyle,
) -> ImportResult<usize> {
    let read_span = tracing::info_span!("reading_csv_files", table_name);
    let _read_enter = read_span.enter();
    read_span.pb_set_style(progress_style);
    read_span.pb_set_length(paths.len() as u64);
    read_span.pb_set_message(&format!("Reading {table_name} CSVs"));
    let rows = load_rows(paths, normalize, &read_span)?;
    drop(_read_enter);
    drop(read_span);

    let dataframe = rows_to_dataframe(&rows, parse_dates)?;
    db::df_to_table(pool, &dataframe, "forms", table_name, db::IfExists::Replace).await?;
    Ok(dataframe.height())
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

    let discovery_span = tracing::info_span!("looking_for_client_status_csvs");
    discovery_span.pb_set_style(
        &ProgressStyle::with_template("{spinner:.green} {msg}").expect("valid template"),
    );
    discovery_span.pb_set_message("Looking for ClientStatus CSVs");
    let _discovery_enter = discovery_span.enter();
    let client_status_paths = get_survey_paths(&cli.data_root, CLIENT_STATUS_SUFFIX);
    drop(_discovery_enter);
    drop(discovery_span);
    let client_status_paths = client_status_paths?;
    info!(
        count = client_status_paths.len(),
        "Found ClientStatus CSVs"
    );

    let discovery_span = tracing::info_span!("looking_for_client_status_raw_csvs");
    discovery_span.pb_set_style(
        &ProgressStyle::with_template("{spinner:.green} {msg}").expect("valid template"),
    );
    discovery_span.pb_set_message("Looking for ClientStatusRawData CSVs");
    let _discovery_enter = discovery_span.enter();
    let client_status_raw_paths = get_survey_paths(&cli.data_root, CLIENT_STATUS_RAW_SUFFIX);
    drop(_discovery_enter);
    drop(discovery_span);
    let client_status_raw_paths = client_status_raw_paths?;
    info!(
        count = client_status_raw_paths.len(),
        "Found ClientStatusRawData CSVs"
    );

    if client_status_paths.is_empty() && client_status_raw_paths.is_empty() {
        return Err(format!(
            "no ClientStatus CSV files found under {}",
            cli.data_root.display()
        )
        .into());
    }

    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;
    let progress_style = ProgressStyle::default_bar()
        .template("{span_child_prefix}{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
        .expect("valid template")
        .progress_chars("#>-");

    if !client_status_paths.is_empty() {
        info!("Writing to DB - forms.rpms_client_status");
        let count = import_table(
            &pool,
            &client_status_paths,
            "rpms_client_status",
            normalize_client_status_row,
            false,
            &progress_style,
        )
        .await?;
        info!(count, "Imported ClientStatus rows");
    }

    if !client_status_raw_paths.is_empty() {
        info!("Writing to DB - forms.rpms_client_status_raw");
        let count = import_table(
            &pool,
            &client_status_raw_paths,
            "rpms_client_status_raw",
            normalize_client_status_raw_row,
            true,
            &progress_style,
        )
        .await?;
        info!(count, "Imported ClientStatusRawData rows");
    }

    pool.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(values: &[(&str, &str)]) -> CsvRow {
        values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn normalizes_client_status_subject_id() {
        let normalized = normalize_client_status_row(row(&[("subjectkey", "AB001")]));
        assert_eq!(normalized.get("subject_id"), Some(&"AB001".to_owned()));
        assert!(!normalized.contains_key("subjectkey"));
    }

    #[test]
    fn normalizes_raw_client_status_columns_and_events() {
        let normalized = normalize_client_status_raw_row(row(&[
            ("subjectkey", "AB001"),
            ("Main Status", "Month 12"),
            (" Sub Status", "Scheduled"),
            ("Status Date", "31/01/2025"),
        ]));

        assert_eq!(normalized.get("subject_id"), Some(&"AB001".to_owned()));
        assert_eq!(normalized.get("main_status"), Some(&"Month 12".to_owned()));
        assert_eq!(normalized.get("sub_status"), Some(&"Scheduled".to_owned()));
        assert_eq!(
            normalized.get("status_date"),
            Some(&"31/01/2025".to_owned())
        );
        assert_eq!(normalized.get("redcap_event"), Some(&"month_12".to_owned()));
    }

    #[test]
    fn maps_unrecognized_raw_status_to_other() {
        assert_eq!(raw_redcap_event("Pre-Screening"), "other");
        assert_eq!(raw_redcap_event("unexpected"), "other");
    }

    #[test]
    fn parses_day_first_status_dates() {
        assert_eq!(
            parse_status_date("31/01/2025"),
            Some(NaiveDate::from_ymd_opt(2025, 1, 31).unwrap())
        );
        assert_eq!(parse_status_date("not a date"), None);
    }

    #[test]
    fn maps_blank_cells_to_null_in_dataframe() {
        let rows = vec![
            row(&[("subject_id", "AB001"), ("main_status", ""), ("sub_status", "   ")]),
            row(&[
                ("subject_id", "AB002"),
                ("main_status", "Included"),
                ("sub_status", "Scheduled"),
            ]),
        ];

        let dataframe = rows_to_dataframe(&rows, false).unwrap();
        assert_eq!(dataframe.column("main_status").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("sub_status").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("subject_id").unwrap().null_count(), 0);
    }

    #[test]
    fn finds_both_csv_file_types() {
        let temp =
            std::env::temp_dir().join(format!("rpms-client-status-test-{}", std::process::id()));
        let surveys = temp.join("PROTECTED/Prescient/raw/AB001/surveys");
        fs::create_dir_all(&surveys).unwrap();
        let status = surveys.join("AB001_ClientStatus_AllDates.csv");
        let raw = surveys.join("AB001_ClientStatusRawData.csv");
        fs::write(&status, "subjectkey\nAB001\n").unwrap();
        fs::write(&raw, "subjectkey\nAB001\n").unwrap();

        assert_eq!(
            get_survey_paths(&temp, CLIENT_STATUS_SUFFIX).unwrap(),
            vec![status]
        );
        assert_eq!(
            get_survey_paths(&temp, CLIENT_STATUS_RAW_SUFFIX).unwrap(),
            vec![raw]
        );
        fs::remove_dir_all(temp).unwrap();
    }
}
