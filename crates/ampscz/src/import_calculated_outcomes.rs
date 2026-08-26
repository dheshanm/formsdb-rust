use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use chrono::{DateTime, Utc};
use clap::Parser;
use futures_util::{StreamExt, stream};
use indicatif::ProgressStyle;
use polars::prelude::{Column, DataFrame, DataType, TimeUnit};
use tracing::{info, warn};
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Import calculated outcomes CSV files into `forms_derived.calculated_outcomes`.
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// One or more PHOENIX directories to search.
    ///
    /// For example: `/data/predict1/data_from_nda/Pronet/PHOENIX`.
    #[arg(short, long, required = true, num_args = 1..)]
    data_root: Vec<PathBuf>,

    /// Number of CSV files parsed concurrently.
    #[arg(short, long, default_value_t = 8)]
    jobs: usize,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long, default_value_t = 1)]
    max_connections: u32,
}

#[derive(Debug)]
struct OutcomeRow {
    subject_id: String,
    form_name: String,
    redcap_event_name: String,
    variable: String,
    value: String,
    data_type: String,
    source_m_date: DateTime<Utc>,
}

fn csv_paths(data_root: &Path, discovery_span: &tracing::Span) -> ImportResult<Vec<PathBuf>> {
    let general = data_root.join("GENERAL");
    if !general.is_dir() {
        return Err(format!(
            "{} is not a PHOENIX directory containing GENERAL",
            data_root.display()
        )
        .into());
    }

    let mut paths = Vec::new();
    for site in fs::read_dir(general)? {
        let site = site?;
        discovery_span.pb_tick();
        discovery_span.pb_set_message(&format!("Searching {}", site.path().display()));
        if !site.file_type()?.is_dir() {
            continue;
        }

        let processed = site.path().join("processed");
        if !processed.is_dir() {
            continue;
        }
        for subject in fs::read_dir(processed)? {
            let subject = subject?;
            discovery_span.pb_tick();
            if !subject.file_type()?.is_dir() {
                continue;
            }

            let surveys = subject.path().join("surveys");
            if !surveys.is_dir() {
                continue;
            }
            for file in fs::read_dir(surveys)? {
                let file = file?;
                discovery_span.pb_tick();
                if file.file_type()?.is_file()
                    && file
                        .path()
                        .extension()
                        .is_some_and(|extension| extension == "csv")
                {
                    paths.push(file.path());
                }
            }
        }
    }

    paths.sort();
    Ok(paths)
}

fn is_missing_value(value: &str) -> bool {
    matches!(
        value.trim(),
        "-300" | "-300.0" | "-900" | "-900.0" | "03/03/1903" | "09/09/1909"
    )
}

fn non_empty_csv_value(value: &str) -> Option<&str> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn process_form(path: &Path) -> ImportResult<Vec<OutcomeRow>> {
    let subject_id = path
        .parent()
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("could not derive subject ID from {}", path.display()))?
        .to_owned();
    let form_name = path
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid CSV filename: {}", path.display()))?
        .to_owned();
    let source_m_date = DateTime::<Utc>::from(
        fs::metadata(path)?
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH),
    );

    let mut reader = csv::ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = reader.headers()?.clone();
    let column_index = |name: &str| {
        headers
            .iter()
            .position(|header| header == name)
            .ok_or_else(|| format!("{} is missing required column {name}", path.display()))
    };
    let variable_index = column_index("variable")?;
    let event_index = column_index("redcap_event_name")?;
    let value_index = column_index("value")?;
    let data_type_index = column_index("data_type")?;

    let mut outcomes = Vec::new();
    for record in reader.records() {
        let record = record?;
        let value = record.get(value_index).unwrap_or_default();
        if is_missing_value(value) {
            continue;
        }
        outcomes.push(OutcomeRow {
            subject_id: subject_id.clone(),
            form_name: form_name.clone(),
            redcap_event_name: record.get(event_index).unwrap_or_default().to_owned(),
            variable: record.get(variable_index).unwrap_or_default().to_owned(),
            value: value.to_owned(),
            data_type: record.get(data_type_index).unwrap_or_default().to_owned(),
            source_m_date,
        });
    }
    Ok(outcomes)
}

fn rows_to_dataframe(rows: Vec<OutcomeRow>) -> ImportResult<DataFrame> {
    let height = rows.len();
    let mut subject_ids = Vec::with_capacity(height);
    let mut form_names = Vec::with_capacity(height);
    let mut event_names = Vec::with_capacity(height);
    let mut variables = Vec::with_capacity(height);
    let mut values = Vec::with_capacity(height);
    let mut data_types = Vec::with_capacity(height);
    let mut source_m_dates = Vec::with_capacity(height);

    for row in rows {
        subject_ids.push(row.subject_id);
        form_names.push(row.form_name);
        event_names.push(row.redcap_event_name);
        variables.push(row.variable);
        values.push(row.value);
        data_types.push(row.data_type);
        source_m_dates.push(row.source_m_date.timestamp_millis());
    }

    let source_m_dates = Column::new("source_m_date".into(), source_m_dates)
        .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))?;

    let subject_ids = subject_ids
        .iter()
        .map(|value| non_empty_csv_value(value))
        .collect::<Vec<_>>();
    let form_names = form_names
        .iter()
        .map(|value| non_empty_csv_value(value))
        .collect::<Vec<_>>();
    let event_names = event_names
        .iter()
        .map(|value| non_empty_csv_value(value))
        .collect::<Vec<_>>();
    let variables = variables
        .iter()
        .map(|value| non_empty_csv_value(value))
        .collect::<Vec<_>>();
    let values = values
        .iter()
        .map(|value| non_empty_csv_value(value))
        .collect::<Vec<_>>();
    let data_types = data_types
        .iter()
        .map(|value| non_empty_csv_value(value))
        .collect::<Vec<_>>();

    Ok(DataFrame::new(
        height,
        vec![
            Column::new("subject_id".into(), subject_ids),
            Column::new("form_name".into(), form_names),
            Column::new("redcap_event_name".into(), event_names),
            Column::new("variable".into(), variables),
            Column::new("value".into(), values),
            Column::new("data_type".into(), data_types),
            source_m_dates,
        ],
    )?)
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
    let mut paths = Vec::new();
    for data_root in &cli.data_root {
        info!(data_root = %data_root.display(), "Looking for calculated outcomes CSVs");
        let discovery_span = tracing::info_span!("looking_for_calculated_outcomes_csvs");
        discovery_span.pb_set_style(
            &ProgressStyle::with_template("{spinner:.green} {msg}").expect("valid template"),
        );
        discovery_span.pb_set_message(&format!("Searching {}", data_root.display()));
        let discovery_enter = discovery_span.enter();
        let root_paths = csv_paths(data_root, &discovery_span);
        drop(discovery_enter);
        drop(discovery_span);
        let root_paths = root_paths?;
        info!(data_root = %data_root.display(), count = root_paths.len(), "Found calculated outcomes CSVs");
        paths.extend(root_paths);
    }
    paths.sort();

    if paths.is_empty() {
        warn!("No calculated outcomes CSV files found; database table was not changed");
        return Ok(());
    }

    info!(
        count = paths.len(),
        jobs = cli.jobs.max(1),
        "Processing calculated outcomes CSVs"
    );
    let progress_style = ProgressStyle::default_bar()
		.template("{span_child_prefix}{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
		.expect("valid template")
		.progress_chars("#>-");
    let processing_span = tracing::info_span!("processing_calculated_outcomes_csvs");
    processing_span.pb_set_style(&progress_style);
    processing_span.pb_set_length(paths.len() as u64);
    processing_span.pb_set_message("Processing calculated outcomes CSVs");
    let processing_enter = processing_span.enter();

    let jobs = cli.jobs.max(1);
    let mut outcomes = Vec::new();
    let mut failed_forms = 0_usize;
    let mut forms_without_outcomes = 0_usize;
    let mut processing = stream::iter(paths.into_iter().map(|path| async move {
        let display_path = path.display().to_string();
        let result = tokio::task::spawn_blocking(move || process_form(&path)).await;
        (display_path, result)
    }))
    .buffer_unordered(jobs);

    while let Some((path, result)) = processing.next().await {
        processing_span.pb_inc(1);
        match result {
            Ok(Ok(rows)) if rows.is_empty() => forms_without_outcomes += 1,
            Ok(Ok(rows)) => outcomes.extend(rows),
            Ok(Err(error)) => {
                failed_forms += 1;
                warn!(path, error = %error, "Skipping calculated outcomes CSV");
            }
            Err(error) => {
                failed_forms += 1;
                warn!(path, error = %error, "Calculated outcomes worker failed");
            }
        }
    }
    drop(processing_enter);
    drop(processing_span);

    if outcomes.is_empty() {
        warn!(
            failed_forms,
            forms_without_outcomes,
            "No calculated outcomes rows to import; database table was not changed"
        );
        return Ok(());
    }

    let dataframe = rows_to_dataframe(outcomes)?;
    info!(
        rows = dataframe.height(),
        failed_forms, forms_without_outcomes, "Writing calculated outcomes to database"
    );
    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;
    db::df_to_table(
        &pool,
        &dataframe,
        "forms_derived",
        "calculated_outcomes",
        db::IfExists::Truncate,
    )
    .await?;
    pool.close().await;
    info!(rows = dataframe.height(), "Imported calculated outcomes");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_python_missing_codes() {
        for value in ["-300", "-900", "03/03/1903", "09/09/1909"] {
            assert!(is_missing_value(value));
        }
        assert!(!is_missing_value("-3"));
        assert!(!is_missing_value("observed value"));
    }

    #[test]
    fn finds_and_processes_outcomes_csvs() {
        let temp =
            std::env::temp_dir().join(format!("calculated-outcomes-test-{}", std::process::id()));
        let surveys = temp.join("GENERAL/Pronet/processed/AB001/surveys");
        fs::create_dir_all(&surveys).unwrap();
        let csv = surveys.join("test_outcomes.csv");
        fs::write(
			&csv,
			"variable,redcap_event_name,value,data_type,ignored\nscore,baseline_arm_1,42,integer,yes\nmissing,baseline_arm_1,-900,integer,yes\n",
		)
		.unwrap();

        let discovery_span = tracing::info_span!("test_csv_discovery");
        assert_eq!(csv_paths(&temp, &discovery_span).unwrap(), vec![csv.clone()]);
        let rows = process_form(&csv).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].subject_id, "AB001");
        assert_eq!(rows[0].form_name, "test_outcomes");
        assert_eq!(rows[0].variable, "score");
        assert_eq!(rows[0].value, "42");

        fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn rejects_csv_without_required_columns() {
        let temp = std::env::temp_dir().join(format!(
            "calculated-outcomes-columns-test-{}",
            std::process::id()
        ));
        let surveys = temp.join("GENERAL/Pronet/processed/AB001/surveys");
        fs::create_dir_all(&surveys).unwrap();
        let csv = surveys.join("test_outcomes.csv");
        fs::write(&csv, "variable,value\nscore,42\n").unwrap();

        let error = process_form(&csv).unwrap_err();
        assert!(error.to_string().contains("redcap_event_name"));
        fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn dataframe_maps_blank_cells_to_nulls() {
        let rows = vec![OutcomeRow {
            subject_id: "AB001".to_owned(),
            form_name: "test_outcomes".to_owned(),
            redcap_event_name: "".to_owned(),
            variable: "   ".to_owned(),
            value: "".to_owned(),
            data_type: " ".to_owned(),
            source_m_date: DateTime::<Utc>::from(SystemTime::UNIX_EPOCH),
        }];

        let dataframe = rows_to_dataframe(rows).unwrap();
        assert_eq!(dataframe.column("redcap_event_name").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("variable").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("value").unwrap().null_count(), 1);
        assert_eq!(dataframe.column("data_type").unwrap().null_count(), 1);
    }
}
