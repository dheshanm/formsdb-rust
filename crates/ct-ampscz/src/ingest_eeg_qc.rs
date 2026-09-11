use std::{
    error::Error,
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use clap::Parser;
use sqlx::PgPool;
use tracing::info;

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Ingest EEG QC data from a CSV file into PostgreSQL (`eeg.qc`).
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Path to the EEG QC CSV file.
    #[arg(short = 'c', long)]
    csv_file: PathBuf,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long, default_value_t = 1)]
    max_connections: u32,
}

const INIT_QUERIES: &[&str] = &[
    r#"CREATE SCHEMA IF NOT EXISTS eeg;"#,
    r#"CREATE TABLE IF NOT EXISTS eeg.qc (
        subject_id TEXT NOT NULL,
        eeg_date DATE NOT NULL,
        eeg_day INTEGER NOT NULL,
        eeg_num_runsheets INTEGER NOT NULL,
        eeg_num_zips INTEGER NOT NULL,
        redcap_event_name TEXT NOT NULL,
        qc_score INTEGER,
        qc_comment TEXT,
        PRIMARY KEY (subject_id, eeg_date),
        FOREIGN KEY (subject_id) REFERENCES public.subjects(subject_id) ON DELETE CASCADE
    );"#,
];

#[derive(Debug, PartialEq, Eq)]
struct EegQcRecord {
    subject_id: String,
    eeg_date: NaiveDate,
    eeg_day: i32,
    eeg_num_runsheets: i32,
    eeg_num_zips: i32,
    redcap_event_name: String,
    qc_score: Option<i32>,
    qc_comment: Option<String>,
}

fn map_qc_comment(qc_score: Option<i32>) -> Option<String> {
    match qc_score {
        Some(-9) => Some("unchecked".to_owned()),
        Some(-8) => Some("ignore".to_owned()),
        Some(-7) => Some("under_review".to_owned()),
        Some(1) => Some("fail".to_owned()),
        Some(2) => Some("some_usable".to_owned()),
        Some(3) => Some("pass".to_owned()),
        Some(4) => Some("excellent".to_owned()),
        _ => Some("unknown_score".to_owned()),
    }
}

fn map_redcap_event(eeg_day: i32) -> String {
    match eeg_day {
        -1 => "day_1_arm_1".to_owned(),
        29 => "day_29_arm_1".to_owned(),
        56 => "day_56_arm_1".to_owned(),
        _ => "unknown_event".to_owned(),
    }
}

fn header_index(headers: &csv::StringRecord, column_name: &str) -> ImportResult<usize> {
    headers
        .iter()
        .position(|header| header.trim().eq_ignore_ascii_case(column_name))
        .ok_or_else(|| format!("EEG QC CSV missing required column: {column_name}").into())
}

fn parse_qc_score(raw_qc: &str) -> ImportResult<Option<i32>> {
    let trimmed = raw_qc.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("nan")
        || trimmed.eq_ignore_ascii_case("null")
        || trimmed.eq_ignore_ascii_case("none")
        || trimmed.eq_ignore_ascii_case("na")
    {
        return Ok(None);
    }

    // Handles integer string or float string like "1.0" or "1"
    if let Ok(score) = trimmed.parse::<i32>() {
        Ok(Some(score))
    } else if let Ok(float_score) = trimmed.parse::<f64>() {
        Ok(Some(float_score as i32))
    } else {
        Err(format!("invalid qc score value: '{trimmed}'").into())
    }
}

fn parse_eeg_date(raw_date: &str) -> ImportResult<NaiveDate> {
    let trimmed = raw_date.trim();
    NaiveDate::parse_from_str(trimmed, "%Y_%m_%d")
        .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y-%m-%d"))
        .map_err(|e| format!("invalid date format '{trimmed}' (expected YYYY_MM_DD): {e}").into())
}

fn read_eeg_qc_rows(path: &Path) -> ImportResult<Vec<EegQcRecord>> {
    let mut reader = csv::ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = reader.headers()?.clone();

    let subject_id_index = header_index(&headers, "subjid")?;
    let date_index = header_index(&headers, "date")?;
    let eeg_day_index = header_index(&headers, "eegDay")?;
    let n_sheet_index = header_index(&headers, "nSheet")?;
    let n_zip_index = header_index(&headers, "nZip")?;
    let qc_index = header_index(&headers, "qc")?;

    let mut rows = Vec::new();
    for (row_offset, record) in reader.records().enumerate() {
        let record = record?;
        let row_number = row_offset + 2;

        let subject_id = record
            .get(subject_id_index)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("row {row_number}: missing required value in column 'subjid'"))?
            .to_owned();

        let date_raw = record
            .get(date_index)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("row {row_number}: missing required value in column 'date'"))?;
        let eeg_date = parse_eeg_date(date_raw)?;

        let eeg_day_raw = record
            .get(eeg_day_index)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("row {row_number}: missing required value in column 'eegDay'"))?;
        let eeg_day: i32 = eeg_day_raw
            .parse::<i32>()
            .or_else(|_| eeg_day_raw.parse::<f64>().map(|f| f as i32))
            .map_err(|e| format!("row {row_number}: invalid eegDay '{eeg_day_raw}': {e}"))?;

        let n_sheet_raw = record
            .get(n_sheet_index)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("row {row_number}: missing required value in column 'nSheet'"))?;
        let eeg_num_runsheets: i32 = n_sheet_raw
            .parse::<i32>()
            .or_else(|_| n_sheet_raw.parse::<f64>().map(|f| f as i32))
            .map_err(|e| format!("row {row_number}: invalid nSheet '{n_sheet_raw}': {e}"))?;

        let n_zip_raw = record
            .get(n_zip_index)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("row {row_number}: missing required value in column 'nZip'"))?;
        let eeg_num_zips: i32 = n_zip_raw
            .parse::<i32>()
            .or_else(|_| n_zip_raw.parse::<f64>().map(|f| f as i32))
            .map_err(|e| format!("row {row_number}: invalid nZip '{n_zip_raw}': {e}"))?;

        let qc_raw = record.get(qc_index).map(str::trim).unwrap_or_default();
        let qc_score = parse_qc_score(qc_raw)?;
        let qc_comment = map_qc_comment(qc_score);
        let redcap_event_name = map_redcap_event(eeg_day);

        rows.push(EegQcRecord {
            subject_id,
            eeg_date,
            eeg_day,
            eeg_num_runsheets,
            eeg_num_zips,
            redcap_event_name,
            qc_score,
            qc_comment,
        });
    }

    Ok(rows)
}

async fn init_db(pool: &PgPool) -> ImportResult<()> {
    info!("Initializing database with EEG schema and QC table");
    let queries: Vec<String> = INIT_QUERIES.iter().map(|&q| q.to_string()).collect();
    db::execute_queries_in_transaction(pool, &queries).await?;
    Ok(())
}

async fn upsert_eeg_qc_records(pool: &PgPool, records: &[EegQcRecord]) -> ImportResult<()> {
    let query = r#"
INSERT INTO eeg.qc (
    subject_id,
    eeg_date,
    eeg_day,
    eeg_num_runsheets,
    eeg_num_zips,
    redcap_event_name,
    qc_score,
    qc_comment
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (subject_id, eeg_date) DO UPDATE SET
    eeg_day = EXCLUDED.eeg_day,
    eeg_num_runsheets = EXCLUDED.eeg_num_runsheets,
    eeg_num_zips = EXCLUDED.eeg_num_zips,
    redcap_event_name = EXCLUDED.redcap_event_name,
    qc_score = EXCLUDED.qc_score,
    qc_comment = EXCLUDED.qc_comment;
    "#;

    for record in records {
        sqlx::query(query)
            .bind(&record.subject_id)
            .bind(record.eeg_date)
            .bind(record.eeg_day)
            .bind(record.eeg_num_runsheets)
            .bind(record.eeg_num_zips)
            .bind(&record.redcap_event_name)
            .bind(record.qc_score)
            .bind(&record.qc_comment)
            .execute(pool)
            .await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> ImportResult<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    info!(path = %cli.csv_file.display(), "Reading EEG QC CSV file");
    let rows = read_eeg_qc_rows(&cli.csv_file)?;

    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;

    init_db(&pool).await?;
    upsert_eeg_qc_records(&pool, &rows).await?;

    info!(rows = rows.len(), "Imported EEG QC rows");

    pool.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_qc_comment() {
        assert_eq!(map_qc_comment(Some(-9)), Some("unchecked".to_string()));
        assert_eq!(map_qc_comment(Some(-8)), Some("ignore".to_string()));
        assert_eq!(map_qc_comment(Some(-7)), Some("under_review".to_string()));
        assert_eq!(map_qc_comment(Some(1)), Some("fail".to_string()));
        assert_eq!(map_qc_comment(Some(2)), Some("some_usable".to_string()));
        assert_eq!(map_qc_comment(Some(3)), Some("pass".to_string()));
        assert_eq!(map_qc_comment(Some(4)), Some("excellent".to_string()));
        assert_eq!(map_qc_comment(Some(99)), Some("unknown_score".to_string()));
        assert_eq!(map_qc_comment(None), Some("unknown_score".to_string()));
    }

    #[test]
    fn test_map_redcap_event() {
        assert_eq!(map_redcap_event(-1), "day_1_arm_1");
        assert_eq!(map_redcap_event(29), "day_29_arm_1");
        assert_eq!(map_redcap_event(56), "day_56_arm_1");
        assert_eq!(map_redcap_event(100), "unknown_event");
    }

    #[test]
    fn test_parse_qc_score() {
        assert_eq!(parse_qc_score("").unwrap(), None);
        assert_eq!(parse_qc_score("NaN").unwrap(), None);
        assert_eq!(parse_qc_score("NA").unwrap(), None);
        assert_eq!(parse_qc_score("null").unwrap(), None);
        assert_eq!(parse_qc_score("3").unwrap(), Some(3));
        assert_eq!(parse_qc_score("-9").unwrap(), Some(-9));
        assert_eq!(parse_qc_score("2.0").unwrap(), Some(2));
    }

    #[test]
    fn test_parse_eeg_date() {
        assert_eq!(
            parse_eeg_date("2026_01_01").unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
        );
        assert_eq!(
            parse_eeg_date("2026-01-01").unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
        );
    }
}
