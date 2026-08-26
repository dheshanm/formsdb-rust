use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use ampscz::{
    EntryStatusItem,
    constants::{RPMS_TO_REDCAP_FORM_NAME, rpms_to_redcap_event},
    get_subject_cohort, get_subject_form_completion_variables,
};
use clap::Parser;
use indicatif::ProgressStyle;
use polars::prelude::{Column, DataFrame};
use sqlx::{PgPool, Postgres, Transaction};
use tracing::{error, info, warn};
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const ENTRY_STATUS_FILE_SUFFIX: &str = "_entry_status.csv";

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type CsvRow = BTreeMap<String, String>;

/// Import all RPMS entry-status CSVs into the database.
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Data root containing PROTECTED.
    #[arg(short, long)]
    data_root: PathBuf,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long, default_value_t = 1)]
    max_connections: u32,
}

fn entry_status_paths(data_root: &Path) -> ImportResult<Vec<PathBuf>> {
    info!("Looking for RPMS entry-status CSVs in {data_root:?}");
    let protected = data_root.join("PROTECTED");
    let mut paths = Vec::new();

    for site in fs::read_dir(&protected)? {
        let site = site?;
        if !site.file_type()?.is_dir()
            || !site.file_name().to_string_lossy().starts_with("Prescient")
        {
            continue;
        }

        let raw = site.path().join("raw");
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
                    && file
                        .file_name()
                        .to_string_lossy()
                        .ends_with(ENTRY_STATUS_FILE_SUFFIX)
                {
                    paths.push(file.path());
                }
            }
        }
    }

    paths.sort();
    info!("Found {} RPMS entry-status CSV files", paths.len());
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

fn redcap_form_name(rpms_form_name: &str) -> Option<&'static str> {
    RPMS_TO_REDCAP_FORM_NAME
        .iter()
        .find_map(|&(rpms_name, redcap_name)| (rpms_name == rpms_form_name).then_some(redcap_name))
}

fn normalize_row(mut row: CsvRow) -> CsvRow {
    if let Some(subject_id) = row.remove("subjectkey") {
        row.insert("subject_id".to_owned(), subject_id);
    }
    if let Some(rpms_form_name) = row.remove("InstrumentName") {
        row.insert("rpms_form_name".to_owned(), rpms_form_name);
    }

    let event_name = row
        .get("visit")
        .and_then(|visit| visit.parse::<i32>().ok())
        .and_then(rpms_to_redcap_event);
    let mut form_name = row
        .get("rpms_form_name")
        .and_then(|name| redcap_form_name(name))
        .map(str::to_owned);

    if let (Some(mapped_form_name), Some(event_name)) = (form_name.as_deref(), event_name) {
        let corrected = match (mapped_form_name, event_name) {
            ("sofas_followup", "screening") => "sofas_screening",
            ("psychs_p1p8_fu", "screening") => "psychs_p1p8",
            ("psychs_p9ac32_fu", "screening") => "psychs_p9ac32",
            ("cssrs_followup", "baseline") => "cssrs_baseline",
            _ => mapped_form_name,
        };
        form_name = Some(corrected.to_owned());
    }

    if let Some(form_name) = form_name {
        row.insert("redcap_form_name".to_owned(), form_name);
    }
    if let Some(event_name) = event_name {
        row.insert("redcap_event_name".to_owned(), event_name.to_owned());
    }

    row
}

fn non_empty_csv_value(value: &str) -> Option<&str> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn rows_to_dataframe(rows: &[CsvRow]) -> ImportResult<DataFrame> {
    let columns = rows
        .iter()
        .flat_map(|row| row.keys().cloned())
        .collect::<BTreeSet<_>>();

    if columns.is_empty() {
        return Err("entry-status CSVs contain no columns".into());
    }

    let columns = columns
        .into_iter()
        .map(|name| {
            let values = rows
                .iter()
                .map(|row| row.get(&name).and_then(|value| non_empty_csv_value(value)))
                .collect::<Vec<_>>();
            Column::new(name.into(), values)
        })
        .collect::<Vec<_>>();
    Ok(DataFrame::new(rows.len(), columns)?)
}

fn load_entry_status_rows(
    paths: &[PathBuf],
    read_span: &tracing::Span,
) -> ImportResult<Vec<CsvRow>> {
    let mut rows = Vec::new();
    for path in paths {
        rows.extend(read_csv(path)?.into_iter().map(normalize_row));
        read_span.pb_inc(1);
    }
    Ok(rows)
}

fn row_to_entry_status_item(row: &CsvRow) -> EntryStatusItem {
    EntryStatusItem {
        redcap_event_name: row.get("redcap_event_name").cloned(),
        redcap_form_name: row.get("redcap_form_name").cloned(),
        completion_status: row.get("CompletionStatus").cloned(),
    }
}

async fn write_subject_completion_variables(
    pool: &PgPool,
    subject_id: &str,
    records: &[ampscz::FormCompletionRecord],
) -> Result<usize, sqlx::Error> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    sqlx::query("DELETE FROM forms.forms WHERE subject_id = $1 AND form_name = 'uncategorized'")
        .bind(subject_id)
        .execute(&mut *tx)
        .await?;

    let count = records.len();
    for record in records {
        sqlx::query(
            r#"
            INSERT INTO forms.forms (
                subject_id, form_name, event_name, form_data,
                source_mdate, variables_with_data
            ) VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(&record.subject_id)
        .bind(&record.form_name)
        .bind(&record.event_name)
        .bind(&record.form_data)
        .bind(record.source_mdate)
        .bind(record.variables_with_data)
        .execute(&mut *tx)
        .await?;
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

    let discovery_span = tracing::info_span!("looking_for_rpms_entry_status_csvs");
    discovery_span.pb_set_style(
        &ProgressStyle::with_template("{spinner:.green} {msg}").expect("valid template"),
    );
    discovery_span.pb_set_message("Looking for RPMS entry-status CSVs");
    let _discovery_enter = discovery_span.enter();
    let paths = entry_status_paths(&cli.data_root);
    drop(_discovery_enter);
    drop(discovery_span);
    let paths = paths?;

    if paths.is_empty() {
        return Err(format!(
            "no RPMS entry-status CSV files found under {}",
            cli.data_root.display()
        )
        .into());
    }

    let progress_style = ProgressStyle::default_bar()
        .template("{span_child_prefix}{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
        .expect("valid template")
        .progress_chars("#>-");

    let read_span = tracing::info_span!("reading_csv_files");
    let _read_enter = read_span.enter();
    read_span.pb_set_style(&progress_style);
    read_span.pb_set_length(paths.len() as u64);

    let normalized_rows = load_entry_status_rows(&paths, &read_span)?;
    drop(_read_enter);
    drop(read_span);

    let dataframe = rows_to_dataframe(&normalized_rows)?;
    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;

    info!("Writing to DB - forms.rpms_entry_status");
    db::df_to_table(
        &pool,
        &dataframe,
        "forms",
        "rpms_entry_status",
        db::IfExists::Replace,
    )
    .await?;
    info!("Imported {} RPMS entry-status rows", dataframe.height());

    info!("Computing form completion variables");
    // Group normalized entry status items by subject_id
    let mut subject_items: BTreeMap<String, Vec<EntryStatusItem>> = BTreeMap::new();
    for row in &normalized_rows {
        if let Some(subject_id) = row.get("subject_id") {
            subject_items
                .entry(subject_id.clone())
                .or_default()
                .push(row_to_entry_status_item(row));
        }
    }

    let completion_span = tracing::info_span!("computing_and_writing_completion_variables");
    let _comp_enter = completion_span.enter();
    completion_span.pb_set_style(&progress_style);
    completion_span.pb_set_length(subject_items.len() as u64);

    let mut imported_completion_forms = 0;
    let mut subjects_processed = 0;

    for (subject_id, items) in subject_items {
        completion_span.pb_set_message(&format!("Processing subject {subject_id}"));
        let cohort = match get_subject_cohort(&subject_id, &pool).await {
            Ok(Some(cohort)) => cohort,
            Ok(None) => {
                warn!(
                    "unknown or missing cohort for subject {subject_id}, skipping form completion variables"
                );
                completion_span.pb_inc(1);
                continue;
            }
            Err(e) => {
                error!(
                    "error fetching cohort for subject {subject_id}: {e}, skipping form completion variables"
                );
                completion_span.pb_inc(1);
                continue;
            }
        };

        match get_subject_form_completion_variables(&subject_id, &cohort, &items) {
            Ok(records) => {
                if !records.is_empty() {
                    let count =
                        write_subject_completion_variables(&pool, &subject_id, &records).await?;
                    imported_completion_forms += count;
                }
                subjects_processed += 1;
            }
            Err(e) => {
                error!("error generating completion variables for subject {subject_id}: {e}");
            }
        }
        completion_span.pb_inc(1);
    }
    drop(_comp_enter);
    drop(completion_span);

    info!(
        "Imported {imported_completion_forms} form completion rows for {subjects_processed} subjects"
    );

    pool.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;

    fn row(values: &[(&str, &str)]) -> CsvRow {
        values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn normalizes_headers_and_maps_values() {
        let normalized = normalize_row(row(&[
            ("subjectkey", "AB001"),
            ("InstrumentName", "BPRS"),
            ("visit", "2"),
        ]));

        assert_eq!(normalized.get("subject_id"), Some(&"AB001".to_owned()));
        assert_eq!(normalized.get("rpms_form_name"), Some(&"BPRS".to_owned()));
        assert_eq!(normalized.get("redcap_form_name"), Some(&"bprs".to_owned()));
        assert_eq!(
            normalized.get("redcap_event_name"),
            Some(&"baseline".to_owned())
        );
        assert!(!normalized.contains_key("subjectkey"));
        assert!(!normalized.contains_key("InstrumentName"));
    }

    #[test]
    fn corrects_special_case_form_names() {
        for (rpms_form_name, visit, expected) in [
            ("SOFAS", "1", "sofas_screening"),
            ("PSYCHSP1P8", "1", "psychs_p1p8"),
            ("PSYCHSP9", "1", "psychs_p9ac32"),
            ("CSSRS", "2", "cssrs_baseline"),
        ] {
            let normalized =
                normalize_row(row(&[("InstrumentName", rpms_form_name), ("visit", visit)]));
            assert_eq!(
                normalized.get("redcap_form_name"),
                Some(&expected.to_owned())
            );
        }
    }

    #[test]
    fn preserves_unknown_or_malformed_mappings_as_missing() {
        let normalized = normalize_row(row(&[
            ("InstrumentName", "not-a-known-form"),
            ("visit", "not-an-event"),
        ]));

        assert!(!normalized.contains_key("redcap_form_name"));
        assert!(!normalized.contains_key("redcap_event_name"));
    }

    #[test]
    fn dataframe_unions_heterogeneous_columns_and_preserves_nulls() {
        let dataframe = rows_to_dataframe(&[
            row(&[("subject_id", "AB001"), ("first", "one")]),
            row(&[("subject_id", "AB002"), ("second", "two")]),
        ])
        .unwrap();

        assert_eq!(dataframe.height(), 2);
        assert_eq!(
            dataframe.get_column_names(),
            &["first", "second", "subject_id"]
        );
        assert_eq!(
            dataframe.column("first").unwrap().str().unwrap().get(1),
            None
        );
        assert_eq!(
            dataframe.column("second").unwrap().str().unwrap().get(0),
            None
        );
    }

    #[test]
    fn dataframe_maps_blank_cells_to_nulls() {
        let dataframe = rows_to_dataframe(&[
            row(&[("subject_id", "AB001"), ("CompletionStatus", "")]),
            row(&[("subject_id", "AB002"), ("CompletionStatus", "   ")]),
        ])
        .unwrap();

        assert_eq!(dataframe.column("CompletionStatus").unwrap().null_count(), 2);
    }

    #[test]
    fn finds_only_entry_status_files_in_prescient_subject_surveys() {
        let temp =
            std::env::temp_dir().join(format!("rpms-entry-status-test-{}", std::process::id()));
        let expected =
            temp.join("PROTECTED/PrescientSite/raw/AB001/surveys/AB001_entry_status.csv");
        fs::create_dir_all(expected.parent().unwrap()).unwrap();
        fs::write(&expected, "subjectkey,InstrumentName,visit\nAB001,BPRS,2\n").unwrap();
        let ignored = temp.join("PROTECTED/OtherSite/raw/AB002/surveys/AB002_entry_status.csv");
        fs::create_dir_all(ignored.parent().unwrap()).unwrap();
        fs::write(&ignored, "subjectkey\nAB002\n").unwrap();
        let non_entry_status = expected.parent().unwrap().join("AB001_other.csv");
        fs::write(non_entry_status, "subjectkey\nAB001\n").unwrap();

        let paths = entry_status_paths(&temp).unwrap();
        assert_eq!(paths, vec![expected]);

        fs::remove_dir_all(Path::new(&temp)).unwrap();
    }

    #[test]
    fn row_to_entry_status_item_extracts_mapped_fields() {
        let mut row = CsvRow::new();
        row.insert("redcap_event_name".to_string(), "screening".to_string());
        row.insert("redcap_form_name".to_string(), "bprs".to_string());
        row.insert("CompletionStatus".to_string(), "2".to_string());

        let item = row_to_entry_status_item(&row);
        assert_eq!(item.redcap_event_name, Some("screening".to_string()));
        assert_eq!(item.redcap_form_name, Some("bprs".to_string()));
        assert_eq!(item.completion_status, Some("2".to_string()));
    }
}
