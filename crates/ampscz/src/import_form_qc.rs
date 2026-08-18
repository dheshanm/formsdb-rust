use std::{
    collections::HashMap,
    error::Error,
    path::{Path, PathBuf},
};

use ampscz::constants::{FORM_NAME_TO_ABBRV, VISIT_ORDER};
use calamine::{DataType, Reader, open_workbook_auto};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::{Column, DataFrame};
use tracing::info;

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Keep each `jsonb_to_recordset` parameter well below PostgreSQL's 256 MiB
/// JSONB-array-element limit.
const DB_WRITE_BATCH_SIZE: usize = 10_000;

/// Import form-QC determinations from Excel workbooks into `forms_derived.form_qc`.
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// One or more form-status tracker Excel files.
    #[arg(short = 'f', long, required = true, num_args = 1.., value_name = "EXCEL_FILE")]
    files: Vec<PathBuf>,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long, default_value_t = 1)]
    max_connections: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct FormQcRecord {
    subject_id: String,
    event_name: String,
    form_name: String,
    expected: bool,
    completed: Option<bool>,
    qc_issue: bool,
    comment: Option<String>,
}

/// Classify a tracker-cell value using the same rules as the Python importer.
fn classify_value(value: Option<&str>) -> (bool, Option<bool>, bool, Option<String>) {
    match value {
        None => (true, Some(true), false, None),
        Some("NA") => (false, Some(false), false, Some("NA".to_owned())),
        Some("Not in CSV") | Some("Not Marked Complete") => {
            (true, Some(false), false, value.map(str::to_owned))
        }
        Some(value) => (true, None, true, Some(value.to_owned())),
    }
}

fn records_from_row(
    subject_id: String,
    values: &HashMap<String, Option<String>>,
) -> Vec<FormQcRecord> {
    let mut records = Vec::with_capacity(VISIT_ORDER.len() * FORM_NAME_TO_ABBRV.len());

    for &event_name in VISIT_ORDER {
        for &(form_name, _) in FORM_NAME_TO_ABBRV {
            let column = format!("{form_name}_{event_name}");
            let value = values
                .get(&column)
                // A missing Excel column is treated as the literal `NA`, just as the
                // Python importer's KeyError fallback does.
                .map(|value| value.as_deref())
                .unwrap_or(Some("NA"));
            let (expected, completed, qc_issue, comment) = classify_value(value);

            records.push(FormQcRecord {
                subject_id: subject_id.clone(),
                event_name: event_name.to_owned(),
                form_name: form_name.to_owned(),
                expected,
                completed,
                qc_issue,
                comment,
            });
        }
    }

    records
}

fn cell_value(cell: Option<&calamine::Data>) -> Option<String> {
    cell.filter(|cell| !cell.is_empty())
        .map(ToString::to_string)
}

fn read_form_qc_records(path: &Path) -> ImportResult<Vec<FormQcRecord>> {
    let mut workbook = open_workbook_auto(path)?;
    let sheet_name = workbook
        .sheet_names()
        .first()
        .cloned()
        .ok_or_else(|| format!("Excel workbook contains no worksheets: {}", path.display()))?;
    let range = workbook.worksheet_range(&sheet_name)?;
    let mut rows = range.rows();
    let header_row = rows
        .next()
        .ok_or_else(|| format!("Excel worksheet is empty: {}", path.display()))?;

    let headers = header_row
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let subject_index = headers
        .iter()
        .position(|header| header == "subject")
        .ok_or_else(|| {
            format!(
                "Excel worksheet has no 'subject' column: {}",
                path.display()
            )
        })?;

    let mut records = Vec::new();
    for row in rows {
        let subject_id = cell_value(row.get(subject_index)).unwrap_or_default();
        let values = headers
            .iter()
            .enumerate()
            .map(|(index, header)| (header.clone(), cell_value(row.get(index))))
            .collect::<HashMap<_, _>>();
        records.extend(records_from_row(subject_id, &values));
    }

    Ok(records)
}

fn records_to_dataframe(records: Vec<FormQcRecord>) -> ImportResult<DataFrame> {
    let row_count = records.len();
    let mut subject_ids = Vec::with_capacity(row_count);
    let mut event_names = Vec::with_capacity(row_count);
    let mut form_names = Vec::with_capacity(row_count);
    let mut expected = Vec::with_capacity(row_count);
    let mut completed = Vec::with_capacity(row_count);
    let mut qc_issues = Vec::with_capacity(row_count);
    let mut comments = Vec::with_capacity(row_count);

    for record in records {
        subject_ids.push(record.subject_id);
        event_names.push(record.event_name);
        form_names.push(record.form_name);
        expected.push(record.expected);
        completed.push(record.completed);
        qc_issues.push(record.qc_issue);
        comments.push(record.comment);
    }

    Ok(DataFrame::new(
        row_count,
        vec![
            Column::new("subject_id".into(), subject_ids),
            Column::new("event_name".into(), event_names),
            Column::new("form_name".into(), form_names),
            Column::new("expected".into(), expected),
            Column::new("completed".into(), completed),
            Column::new("qc_issue".into(), qc_issues),
            Column::new("comment".into(), comments),
        ],
    )?)
}

async fn write_records_in_batches(
    pool: &sqlx::PgPool,
    mut records: Vec<FormQcRecord>,
) -> ImportResult<()> {
    if records.is_empty() {
        return Err("form-QC Excel files contain no data rows".into());
    }

    let total_records = records.len();
    let total_batches = total_records.div_ceil(DB_WRITE_BATCH_SIZE);
    let progress = ProgressBar::new(total_records as u64);
    progress.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} Writing form-QC records [{bar:40.cyan/blue}] {pos}/{len} ({percent}%)",
        )
        .expect("valid progress bar template")
        .progress_chars("#>-"),
    );

    let mut batch_number = 0;
    while !records.is_empty() {
        let batch_len = records.len().min(DB_WRITE_BATCH_SIZE);
        let batch = records.drain(..batch_len).collect();
        batch_number += 1;
        info!(
            batch_number,
            total_batches, batch_len, "Writing form-QC batch"
        );

        let dataframe = records_to_dataframe(batch)?;
        db::df_to_table(
            pool,
            &dataframe,
            "forms_derived",
            "form_qc",
            if batch_number == 1 {
                db::IfExists::Replace
            } else {
                db::IfExists::Append
            },
        )
        .await?;
        progress.inc(batch_len as u64);
    }

    progress.finish_with_message("Form-QC import complete");
    Ok(())
}

#[tokio::main]
async fn main() -> ImportResult<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let mut records = Vec::new();
    for (file_number, path) in cli.files.iter().enumerate() {
        info!(
            file_number = file_number + 1,
            total_files = cli.files.len(),
            path = %path.display(),
            "Reading form-QC Excel workbook"
        );
        let file_records = read_form_qc_records(path)?;
        info!(
            file_number = file_number + 1,
            records = file_records.len(),
            "Finished reading form-QC Excel workbook"
        );
        records.extend(file_records);
    }
    info!(
        records = records.len(),
        "Parsed form-QC records; preparing database import"
    );

    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;

    info!("Writing to DB - forms_derived.form_qc");
    let imported_records = records.len();
    write_records_in_batches(&pool, records).await?;
    info!(rows = imported_records, "Imported form-QC records");

    pool.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_qc_values_like_python_importer() {
        assert_eq!(classify_value(None), (true, Some(true), false, None));
        assert_eq!(
            classify_value(Some("NA")),
            (false, Some(false), false, Some("NA".to_owned()))
        );
        assert_eq!(
            classify_value(Some("Not in CSV")),
            (true, Some(false), false, Some("Not in CSV".to_owned()))
        );
        assert_eq!(
            classify_value(Some("Needs review")),
            (true, None, true, Some("Needs review".to_owned()))
        );
    }

    #[test]
    fn missing_columns_are_treated_as_na_and_blank_cells_as_complete() {
        let mut values = HashMap::new();
        values.insert("enrollment_note_screening".to_owned(), None);
        values.insert(
            "informed_consent_run_sheet_screening".to_owned(),
            Some("Not Marked Complete".to_owned()),
        );

        let records = records_from_row("subject-001".to_owned(), &values);
        let blank = records
            .iter()
            .find(|record| {
                record.form_name == "enrollment_note" && record.event_name == "screening"
            })
            .unwrap();
        assert_eq!(blank.completed, Some(true));
        assert!(!blank.qc_issue);
        assert_eq!(blank.comment, None);

        let incomplete = records
            .iter()
            .find(|record| {
                record.form_name == "informed_consent_run_sheet" && record.event_name == "screening"
            })
            .unwrap();
        assert_eq!(incomplete.completed, Some(false));
        assert!(!incomplete.qc_issue);
        assert_eq!(incomplete.comment.as_deref(), Some("Not Marked Complete"));

        let missing = records
            .iter()
            .find(|record| record.form_name == "missing_data" && record.event_name == "screening")
            .unwrap();
        assert!(!missing.expected);
        assert_eq!(missing.completed, Some(false));
        assert_eq!(missing.comment.as_deref(), Some("NA"));
    }

    #[test]
    fn write_batch_size_is_bounded_and_nonzero() {
        assert!(DB_WRITE_BATCH_SIZE > 0);
        assert!(DB_WRITE_BATCH_SIZE < 100_000);
    }
}
