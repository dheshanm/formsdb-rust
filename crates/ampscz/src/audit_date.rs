use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use chrono::{Datelike, NaiveDate, NaiveDateTime};
use clap::Parser;
use futures_util::{StreamExt, TryStreamExt, stream};
use indicatif::ProgressStyle;
use tracing::{info, warn};
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Candidate formats a date-like value is tested against, grouped by date type.
const DATE_FORMATS: &[&str] = &["%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d"];
const DATETIME_FORMATS: &[&str] = &[
    "%m/%d/%Y %I:%M:%S %p",
    "%d/%m/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%d/%m/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
];
/// Columns with a known format that are left out of the audit.
const IGNORED_COLUMNS: &[&str] = &["LastModifiedDate"];
/// Dates before this year are missing-data placeholders (e.g. 03/03/1903, 09/09/1909).
const PLACEHOLDER_BEFORE_YEAR: i32 = 1950;
const LOG_FREQ: usize = 100;

type AuditResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Infer the date format of every date and datetime column in the RPMS survey CSVs.
///
/// Scans `<DATA_ROOT>/PHOENIX/PROTECTED/*/raw/*/surveys/*.csv` (and the `*.csv.flat` files
/// that import_rpms_csvs reads for some forms) and writes one row per
/// (form file, variable, date format). A variable whose values use more than one
/// format gets one row per format.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Network data root containing PHOENIX (e.g. /data/predict1/data_from_nda/Prescient).
    #[arg(short, long)]
    data_root: PathBuf,

    /// Output CSV path.
    #[arg(short, long, default_value = "rpms_date_formats.csv")]
    output: PathBuf,

    /// Number of subjects scanned concurrently.
    #[arg(short, long, default_value_t = 8)]
    jobs: usize,

    /// Drop variables whose only values are placeholder dates (status `placeholder_only`).
    #[arg(long)]
    skip_placeholders: bool,
}

/// Values that parse under the same set of candidate formats.
#[derive(Debug, Default)]
struct CandidateStats {
    n_values: u64,
    /// Values whose candidate formats disagree on the date (e.g. 04/03/2024).
    n_ambiguous: u64,
    n_placeholder: u64,
    subjects: BTreeSet<String>,
    /// (is_placeholder, subject_id, value); the minimum prefers a real date.
    example: Option<(bool, String, String)>,
}

impl CandidateStats {
    fn add(&mut self, subject_id: &str, value: &str, class: ValueClass) {
        self.n_values += 1;
        self.n_ambiguous += u64::from(class.ambiguous);
        self.n_placeholder += u64::from(class.placeholder);
        self.subjects.insert(subject_id.to_owned());
        let candidate = (class.placeholder, subject_id.to_owned(), value.to_owned());
        if self.example.as_ref().is_none_or(|example| candidate < *example) {
            self.example = Some(candidate);
        }
    }

    fn merge(&mut self, other: CandidateStats) {
        self.n_values += other.n_values;
        self.n_ambiguous += other.n_ambiguous;
        self.n_placeholder += other.n_placeholder;
        self.subjects.extend(other.subjects);
        if let Some(candidate) = other.example {
            if self.example.as_ref().is_none_or(|example| candidate < *example) {
                self.example = Some(candidate);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ValueClass {
    /// The candidate formats disagree on the date (e.g. 04/03/2024).
    ambiguous: bool,
    /// Every reading is a missing-data placeholder date.
    placeholder: bool,
}

/// (form_file, variable, date_type)
type ColumnKey = (String, String, &'static str);
type ColumnStats = BTreeMap<Vec<&'static str>, CandidateStats>;
type Audit = BTreeMap<ColumnKey, ColumnStats>;

#[derive(Debug)]
struct OutputRow {
    form_file: String,
    variable: String,
    date_type: &'static str,
    date_format: String,
    status: &'static str,
    n_values: u64,
    n_ambiguous: u64,
    n_placeholder: u64,
    n_subjects: usize,
    example_subject_id: String,
    example_value: String,
}

fn looks_like_date(value: &str) -> bool {
    (8..=25).contains(&value.len())
        && value.starts_with(|c: char| c.is_ascii_digit())
        && value.contains(['/', '-'])
}

fn plausible_year(year: i32) -> bool {
    (1000..=9999).contains(&year)
}

/// Returns the date type, the formats the value parses under, and its class.
fn classify_value(value: &str) -> Option<(&'static str, Vec<&'static str>, ValueClass)> {
    if !looks_like_date(value) {
        return None;
    }

    for (date_type, formats) in [("date", DATE_FORMATS), ("datetime", DATETIME_FORMATS)] {
        let mut matched = Vec::new();
        let mut parsed = BTreeSet::new();
        for &format in formats {
            let datetime = if date_type == "date" {
                NaiveDate::parse_from_str(value, format)
                    .ok()
                    .and_then(|date| date.and_hms_opt(0, 0, 0))
            } else {
                NaiveDateTime::parse_from_str(value, format).ok()
            };
            if let Some(datetime) = datetime.filter(|datetime| plausible_year(datetime.year())) {
                matched.push(format);
                parsed.insert(datetime);
            }
        }
        if !matched.is_empty() {
            let class = ValueClass {
                ambiguous: parsed.len() > 1,
                placeholder: parsed.iter().all(|datetime| datetime.year() < PLACEHOLDER_BEFORE_YEAR),
            };
            return Some((date_type, matched, class));
        }
    }
    None
}

/// Survey CSVs, including the `.csv.flat` files that import_rpms_csvs reads for some forms.
fn is_survey_csv(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".csv") || name.ends_with(".csv.flat"))
}

fn form_file_name(subject_id: &str, file_name: &str) -> String {
    file_name
        .strip_prefix(subject_id)
        .map(|rest| rest.trim_start_matches(['_', '.']))
        .unwrap_or(file_name)
        .to_owned()
}

fn scan_csv(path: &Path, subject_id: &str, audit: &mut Audit) -> AuditResult<()> {
    let file_name = path.file_name().and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid file name: {}", path.display()))?;
    let form_file = form_file_name(subject_id, file_name);

    let mut reader = csv::ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = reader.byte_headers()?.iter()
        .map(|header| String::from_utf8_lossy(header).into_owned())
        .collect::<Vec<_>>();

    for record in reader.byte_records() {
        let record = record?;
        for (header, value) in headers.iter().zip(record.iter()) {
            if IGNORED_COLUMNS.contains(&header.as_str()) {
                continue;
            }
            let value = String::from_utf8_lossy(value);
            let value = value.trim();
            let Some((date_type, formats, class)) = classify_value(value) else {
                continue;
            };
            audit
                .entry((form_file.clone(), header.clone(), date_type))
                .or_default()
                .entry(formats)
                .or_default()
                .add(subject_id, value, class);
        }
    }
    Ok(())
}

fn scan_subject(surveys: PathBuf) -> AuditResult<Audit> {
    let subject_id = surveys.parent().and_then(Path::file_name).and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid subject directory: {}", surveys.display()))?.to_owned();
    let mut audit = Audit::new();

    let mut files = fs::read_dir(&surveys)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file() && is_survey_csv(path))
        .collect::<Vec<_>>();
    files.sort();

    for path in files {
        if let Err(error) = scan_csv(&path, &subject_id, &mut audit) {
            warn!("failed to read {}: {error}", path.display());
        }
    }
    Ok(audit)
}

fn merge_audit(target: &mut Audit, source: Audit) {
    for (key, column) in source {
        let target_column = target.entry(key).or_default();
        for (formats, stats) in column {
            target_column.entry(formats).or_default().merge(stats);
        }
    }
}

fn survey_dirs(data_root: &Path) -> AuditResult<Vec<PathBuf>> {
    let protected = data_root.join("PHOENIX").join("PROTECTED");
    info!("Looking for RPMS survey directories in {protected:?}");
    let mut paths = Vec::new();
    for site in fs::read_dir(&protected)? {
        let raw = site?.path().join("raw");
        if !raw.is_dir() {
            continue;
        }
        for subject in fs::read_dir(raw)? {
            let surveys = subject?.path().join("surveys");
            if surveys.is_dir() {
                paths.push(surveys);
            }
        }
    }
    paths.sort();
    info!("Found {} subject survey directories", paths.len());
    Ok(paths)
}

/// Assigns each candidate set to a format and builds the output rows for one column.
///
/// A format counts as observed when at least one value parses under it alone. Values
/// that fit several formats go to the single observed format among them; if none or
/// several of their formats were observed, they get a row of their own.
fn column_rows((form_file, variable, date_type): ColumnKey, column: ColumnStats) -> Vec<OutputRow> {
    let observed = column.keys()
        .filter(|formats| formats.len() == 1)
        .map(|formats| formats[0])
        .collect::<BTreeSet<_>>();
    let placeholder_only = column.values().all(|stats| stats.n_placeholder == stats.n_values);
    let status = match observed.len() {
        0 if placeholder_only => "placeholder_only",
        0 => "undetermined",
        1 => "consistent",
        _ => "mixed",
    };

    // Per format: all assigned values, plus the values that parse under that format
    // alone, whose example proves the format.
    let mut by_format = BTreeMap::<String, (CandidateStats, CandidateStats)>::new();
    for (formats, stats) in column {
        let assigned = formats.iter().copied().filter(|format| observed.contains(format)).collect::<Vec<_>>();
        let label = match assigned.as_slice() {
            [format] => format.to_string(),
            [] => formats.join(" | "),
            several => several.join(" | "),
        };
        let (all, proof) = by_format.entry(label).or_default();
        if formats.len() == 1 {
            proof.merge(CandidateStats { example: stats.example.clone(), ..Default::default() });
        }
        all.merge(stats);
    }

    by_format.into_iter().map(|(date_format, (stats, proof))| {
        let (_, example_subject_id, example_value) = proof.example.or(stats.example).unwrap_or_default();
        OutputRow {
            form_file: form_file.clone(),
            variable: variable.clone(),
            date_type,
            date_format,
            status,
            n_values: stats.n_values,
            n_ambiguous: stats.n_ambiguous,
            n_placeholder: stats.n_placeholder,
            n_subjects: stats.subjects.len(),
            example_subject_id,
            example_value,
        }
    }).collect()
}

fn write_output(path: &Path, audit: Audit, skip_placeholders: bool) -> AuditResult<usize> {
    let status_rank = |status: &str| match status {
        "mixed" => 0,
        "undetermined" => 1,
        "placeholder_only" => 2,
        _ => 3,
    };
    let mut rows = audit.into_iter()
        .flat_map(|(key, column)| column_rows(key, column))
        .filter(|row| !(skip_placeholders && row.status == "placeholder_only"))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        (status_rank(a.status), &a.form_file, &a.variable, a.date_type, &a.date_format)
            .cmp(&(status_rank(b.status), &b.form_file, &b.variable, b.date_type, &b.date_format))
    });

    let mut writer = csv::Writer::from_path(path)?;
    writer.write_record([
        "form_file", "variable", "date_type", "date_format", "status", "n_values",
        "n_ambiguous", "n_placeholder", "n_subjects", "example_subject_id", "example_value",
    ])?;
    for row in &rows {
        writer.write_record([
            row.form_file.as_str(), row.variable.as_str(), row.date_type, row.date_format.as_str(),
            row.status, &row.n_values.to_string(), &row.n_ambiguous.to_string(),
            &row.n_placeholder.to_string(), &row.n_subjects.to_string(),
            row.example_subject_id.as_str(), row.example_value.as_str(),
        ])?;
    }
    writer.flush()?;
    Ok(rows.len())
}

#[tokio::main]
async fn main() -> AuditResult<()> {
    let indicatif_layer = IndicatifLayer::new();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
        .with(indicatif_layer)
        .init();

    let cli = Cli::parse();
    let paths = survey_dirs(&cli.data_root)?;
    let total = paths.len();
    let jobs = cli.jobs.max(1);

    let scan_span = tracing::info_span!("scanning_rpms_subjects");
    scan_span.pb_set_style(
        &ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
            .expect("valid template")
            .progress_chars("#>-"),
    );
    scan_span.pb_set_length(total as u64);
    scan_span.pb_set_message("Scanning RPMS survey CSVs");
    let _scan_enter = scan_span.enter();

    let mut subject_stream = stream::iter(paths)
        .map(|path| async move {
            tokio::task::spawn_blocking(move || scan_subject(path))
                .await
                .map_err(|error| -> Box<dyn Error + Send + Sync> { Box::new(error) })?
        })
        .buffer_unordered(jobs);

    let mut audit = Audit::new();
    let mut scanned = 0;
    while let Some(subject_audit) = subject_stream.try_next().await? {
        merge_audit(&mut audit, subject_audit);
        scanned += 1;
        scan_span.pb_inc(1);
        if scanned % LOG_FREQ == 0 || scanned == total {
            info!(scanned, total, "Scanned RPMS subjects");
        }
    }
    drop(_scan_enter);
    drop(scan_span);

    let rows = write_output(&cli.output, audit, cli.skip_placeholders)?;
    info!("Wrote {rows} rows to {}", cli.output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLAIN: ValueClass = ValueClass { ambiguous: false, placeholder: false };
    const AMBIGUOUS: ValueClass = ValueClass { ambiguous: true, placeholder: false };
    const PLACEHOLDER: ValueClass = ValueClass { ambiguous: false, placeholder: true };

    #[test]
    fn classifies_dates_and_datetimes() {
        assert_eq!(classify_value("10/31/2023"), Some(("date", vec!["%m/%d/%Y"], PLAIN)));
        assert_eq!(classify_value("26/07/2024"), Some(("date", vec!["%d/%m/%Y"], PLAIN)));
        assert_eq!(classify_value("04/03/2024"), Some(("date", vec!["%m/%d/%Y", "%d/%m/%Y"], AMBIGUOUS)));
        assert_eq!(classify_value("05/05/2024"), Some(("date", vec!["%m/%d/%Y", "%d/%m/%Y"], PLAIN)));
        assert_eq!(classify_value("03/03/1903"), Some(("date", vec!["%m/%d/%Y", "%d/%m/%Y"], PLACEHOLDER)));
        assert_eq!(
            classify_value("14/11/2024 1:14:00 AM"),
            Some(("datetime", vec!["%d/%m/%Y %I:%M:%S %p"], PLAIN))
        );
        assert_eq!(
            classify_value("3/03/1903 12:00:00 AM"),
            Some(("datetime", vec!["%m/%d/%Y %I:%M:%S %p", "%d/%m/%Y %I:%M:%S %p"], PLACEHOLDER))
        );
        assert_eq!(classify_value("14/11/23"), None);
        assert_eq!(classify_value("17.5.1"), None);
        assert_eq!(classify_value("free text 01/02/2024"), None);
    }

    #[test]
    fn strips_subject_prefix_from_form_file() {
        assert_eq!(form_file_name("BM00644", "BM00644_psychs_p1p8.csv"), "psychs_p1p8.csv");
        assert_eq!(
            form_file_name("BM83373", "BM83373.Prescient.Run_sheet_PennCNB_1.csv"),
            "Prescient.Run_sheet_PennCNB_1.csv"
        );
        assert_eq!(
            form_file_name("BM00644", "BM00644_adverse_events.csv.flat"),
            "adverse_events.csv.flat"
        );
    }

    #[test]
    fn selects_csv_and_flat_survey_files() {
        assert!(is_survey_csv(Path::new("surveys/BM00644_psychs_p1p8.csv")));
        assert!(is_survey_csv(Path::new("surveys/BM00644_adverse_events.csv.flat")));
        assert!(!is_survey_csv(Path::new("surveys/BM00644_notes.txt")));
        assert!(!is_survey_csv(Path::new("surveys/BM00644_adverse_events.flat")));
    }

    #[test]
    fn splits_mixed_columns_into_one_row_per_format() {
        let key = ("form.csv".to_owned(), "var".to_owned(), "date");
        let mut column = ColumnStats::new();
        column.entry(vec!["%m/%d/%Y"]).or_default().add("AA00001", "10/31/2023", PLAIN);
        column.entry(vec!["%d/%m/%Y"]).or_default().add("AA00002", "26/07/2024", PLAIN);
        column.entry(vec!["%m/%d/%Y", "%d/%m/%Y"]).or_default().add("AA00003", "04/03/2024", AMBIGUOUS);

        let rows = column_rows(key, column);
        let formats = rows.iter().map(|row| row.date_format.as_str()).collect::<Vec<_>>();
        assert_eq!(formats, ["%d/%m/%Y", "%m/%d/%Y", "%m/%d/%Y | %d/%m/%Y"]);
        assert!(rows.iter().all(|row| row.status == "mixed"));
        assert_eq!(rows[2].n_ambiguous, 1);
    }

    #[test]
    fn assigns_ambiguous_values_to_the_only_observed_format() {
        let key = ("form.csv".to_owned(), "var".to_owned(), "date");
        let mut column = ColumnStats::new();
        column.entry(vec!["%m/%d/%Y", "%d/%m/%Y"]).or_default().add("AA00001", "04/03/2024", AMBIGUOUS);
        column.entry(vec!["%d/%m/%Y"]).or_default().add("AA00002", "26/07/2024", PLAIN);

        let rows = column_rows(key, column);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].date_format, "%d/%m/%Y");
        assert_eq!(rows[0].status, "consistent");
        assert_eq!((rows[0].n_values, rows[0].n_ambiguous, rows[0].n_subjects), (2, 1, 2));
        assert_eq!(rows[0].example_value, "26/07/2024");
    }

    #[test]
    fn separates_placeholder_only_columns_from_undetermined() {
        let key = || ("form.csv".to_owned(), "var".to_owned(), "date");
        let mut column = ColumnStats::new();
        column.entry(vec!["%m/%d/%Y", "%d/%m/%Y"]).or_default().add("AA00001", "03/03/1903", PLACEHOLDER);
        column.entry(vec!["%m/%d/%Y", "%d/%m/%Y"]).or_default().add("AA00002", "09/09/1909", PLACEHOLDER);
        let rows = column_rows(key(), column);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "placeholder_only");
        assert_eq!((rows[0].n_values, rows[0].n_placeholder), (2, 2));

        let mut column = ColumnStats::new();
        column.entry(vec!["%m/%d/%Y", "%d/%m/%Y"]).or_default().add("AA00001", "03/03/1903", PLACEHOLDER);
        column.entry(vec!["%m/%d/%Y", "%d/%m/%Y"]).or_default().add("AA00002", "05/05/2024", PLAIN);
        let rows = column_rows(key(), column);
        assert_eq!(rows[0].status, "undetermined");
        // A real date is a better example than a placeholder, even from a later subject.
        assert_eq!((rows[0].example_subject_id.as_str(), rows[0].example_value.as_str()), ("AA00002", "05/05/2024"));
    }
}
