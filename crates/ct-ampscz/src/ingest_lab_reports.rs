use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    path::{Path, PathBuf},
};

use chrono::NaiveDateTime;
use clap::Parser;
use indicatif::ProgressStyle;
use serde_json::{Map, Value};
use sqlx::{PgPool, Postgres, QueryBuilder, types::Json};
use tracing::{info, warn};
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";
const FILE_NAME_MARKER: &str = "_lab_report_";

/// Ingest LabCorp clinical-trial lab report CSVs into `lab.accessions` and
/// `lab.results`.
///
/// Files are expected at
/// `PHOENIX/PROTECTED/<site>/raw/<subject>/lab_reports/<subject>_lab_report_*.csv`.
/// LabCorp transfers are cumulative, so files are applied in transfer order
/// (FILCRDTM) and a row is only overwritten by an equal or newer transfer.
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Lab report CSV files to ingest. Glob patterns are supported
    /// (for example, '/data/predict2/Procan/PHOENIX/PROTECTED/*/raw/*/lab_reports/*.csv').
    #[arg(required = true)]
    files: Vec<String>,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long, default_value_t = 1)]
    max_connections: u32,

    /// Exit successfully even if some files fail to ingest. Failures are
    /// still logged as warnings and counted in the summary. Fatal errors (DB,
    /// glob, init) still exit non-zero.
    #[arg(long)]
    allow_file_errors: bool,
}

const INIT_QUERIES: &[&str] = &[
    r#"CREATE SCHEMA IF NOT EXISTS lab;"#,
    r#"CREATE TABLE IF NOT EXISTS lab.accessions (
        accession_number TEXT PRIMARY KEY,
        subject_id TEXT NOT NULL,
        redcap_event_name TEXT NOT NULL,
        visit_number INTEGER NOT NULL,
        visit_type TEXT NOT NULL,
        investigator_name TEXT,
        sex TEXT,
        collected_at TIMESTAMP NOT NULL,
        received_at TIMESTAMP,
        last_activity_at TIMESTAMP,
        transfer_created_at TIMESTAMP NOT NULL,
        source_file TEXT NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        FOREIGN KEY (subject_id) REFERENCES public.subjects(subject_id) ON DELETE CASCADE
    );"#,
    r#"CREATE INDEX IF NOT EXISTS accessions_subject_event_idx
        ON lab.accessions (subject_id, redcap_event_name);"#,
    r#"CREATE TABLE IF NOT EXISTS lab.results (
        accession_number TEXT NOT NULL,
        battery_id TEXT NOT NULL,
        battery_name TEXT NOT NULL,
        lab_test_id TEXT NOT NULL,
        lab_test_name TEXT NOT NULL,
        test_code TEXT,
        test_name TEXT,
        loinc_code TEXT,
        category TEXT,
        specimen_type TEXT,
        specimen_condition TEXT,
        test_status TEXT NOT NULL,
        test_comment TEXT,
        tested_at TIMESTAMP,
        reported_at TIMESTAMP,
        result_type TEXT,
        result_status TEXT,
        result_text TEXT,
        result_numeric DOUBLE PRECISION,
        result_unit TEXT,
        ref_range_low DOUBLE PRECISION,
        ref_range_high DOUBLE PRECISION,
        si_result_text TEXT,
        si_result_numeric DOUBLE PRECISION,
        si_unit TEXT,
        si_ref_range_low DOUBLE PRECISION,
        si_ref_range_high DOUBLE PRECISION,
        alert_flag TEXT,
        excluded BOOLEAN NOT NULL,
        raw_record JSONB NOT NULL,
        transfer_created_at TIMESTAMP NOT NULL,
        source_file TEXT NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (accession_number, battery_id, lab_test_id),
        FOREIGN KEY (accession_number) REFERENCES lab.accessions(accession_number) ON DELETE CASCADE
    );"#,
];

/// Insert column order for `lab.accessions`; must match `push_accession`.
const ACCESSION_COLUMNS: &[&str] = &[
    "accession_number",
    "subject_id",
    "redcap_event_name",
    "visit_number",
    "visit_type",
    "investigator_name",
    "sex",
    "collected_at",
    "received_at",
    "last_activity_at",
    "transfer_created_at",
    "source_file",
];

/// Insert column order for `lab.results`; must match `push_result`.
const RESULT_COLUMNS: &[&str] = &[
    "accession_number",
    "battery_id",
    "battery_name",
    "lab_test_id",
    "lab_test_name",
    "test_code",
    "test_name",
    "loinc_code",
    "category",
    "specimen_type",
    "specimen_condition",
    "test_status",
    "test_comment",
    "tested_at",
    "reported_at",
    "result_type",
    "result_status",
    "result_text",
    "result_numeric",
    "result_unit",
    "ref_range_low",
    "ref_range_high",
    "si_result_text",
    "si_result_numeric",
    "si_unit",
    "si_ref_range_low",
    "si_ref_range_high",
    "alert_flag",
    "excluded",
    "raw_record",
    "transfer_created_at",
    "source_file",
];

/// Source (CDISC LAB) columns read by this importer; checked against each
/// file header before any row is parsed.
const SOURCE_COLUMNS: &[&str] = &[
    "FILCRDTM", "SUBJID", "INVNAM", "SEX", "VISITNUM", "VISIT", "VISITTYP", "ACCSNNUM", "LACTDTM",
    "LBDTM", "RCVDTM", "SPECCND", "LBSPEC", "BATTRID", "BATTRNAM", "PLBNUM", "LBTESTCD", "LBTEST",
    "TSTCD", "TSTNAM", "LBLOINC", "TSTDESC", "TSTSTAT", "TSTCOM", "TSTDTM", "RPTRESC", "RPTRESN",
    "RPTNRLO", "RPTNRHI", "RPTU", "SIRESC", "SIRESN", "SINRLO", "SINRHI", "SIU", "RPTRTYP",
    "RPTRSTAT", "ALRTFL", "EXCLFL", "RPTDTM",
];

/// Flag columns between RPTRSTAT and RPTDTM. LabCorp occasionally drops one
/// of these (always empty) fields on rows it also mis-quotes.
const FLAG_COLUMNS: &[&str] = &["ALRTFL", "DELTFL", "TOXGR", "TOXGRCD", "EXCLFL", "BLNDFL"];

/// Where a view column comes from: a typed column on `lab.accessions` (`a`)
/// or `lab.results` (`r`), or the source text kept in `raw_record`.
#[derive(Debug, Clone, Copy)]
enum ViewSource {
    Acc,
    Res,
    Raw,
}

use ViewSource::{Acc, Raw, Res};

/// Every LabCorp CSV column in file order: (CSV header, readable name in
/// `lab.reports`, source). Typed sources use the readable name as their
/// column name.
const CSV_COLUMNS: &[(&str, &str, ViewSource)] = &[
    ("VERSION", "file_format_version", Raw),
    ("FILCRDTM", "transfer_created_at", Res),
    ("TRMSRNUM", "transmission_source_id", Raw),
    ("TRMSRNAM", "transmission_source_name", Raw),
    ("STUDYID", "study_id", Raw),
    ("STUDNAM", "study_name", Raw),
    ("TRMTYP", "transmission_type", Raw),
    ("SITEID", "site_code", Raw),
    ("INVID", "investigator_id", Raw),
    ("INVNAM", "investigator_name", Acc),
    ("SCRNNUM", "screening_number", Raw),
    ("SUBJID", "subject_id", Acc),
    ("SUBJSID", "subject_secondary_id", Raw),
    ("SUBJINIT", "subject_initials", Raw),
    ("SEX", "sex", Acc),
    ("SEXCD", "sex_code", Raw),
    ("BRTHDTM", "birth_datetime", Raw),
    ("RACE", "race", Raw),
    ("RACECD", "race_code", Raw),
    ("VISITNUM", "visit_number", Acc),
    ("VISIT", "redcap_event_name", Acc),
    ("VISITTYP", "visit_type", Acc),
    ("VISITMOD", "visit_modifier", Raw),
    ("LBNUM", "lab_id", Raw),
    ("LBNAM", "lab_name", Raw),
    ("ACCSNNUM", "accession_number", Acc),
    ("LACTDTM", "last_activity_at", Acc),
    ("RECEXTYP", "record_extension_type", Raw),
    ("SPECNUM", "specimen_number", Raw),
    ("LBDTM", "collected_at", Acc),
    ("PTMEL", "planned_time_elapsed", Raw),
    ("PTMELTX", "planned_time_elapsed_text", Raw),
    ("COLENDTM", "collection_end_at", Raw),
    ("RCVDTM", "received_at", Acc),
    ("SPECCND", "specimen_condition", Res),
    ("SPECCOM", "specimen_comment", Raw),
    ("SPECICOM", "specimen_internal_comment", Raw),
    ("SPECMNUM", "specimen_material_number", Raw),
    ("SPECMCD", "specimen_material_code", Raw),
    ("LBSPEC", "specimen_type", Res),
    ("AGEATCOL", "age_at_collection", Raw),
    ("AGEU", "age_unit", Raw),
    ("FASTSTAT", "fasting_status", Raw),
    ("BATTRID", "battery_id", Res),
    ("BATTRNAM", "battery_name", Res),
    ("PLBNUM", "performing_lab_id", Raw),
    ("PLBNAM", "performing_lab_name", Raw),
    ("LBTESTCD", "lab_test_id", Res),
    ("LBTEST", "lab_test_name", Res),
    ("TSTCD", "test_code", Res),
    ("TSTNAM", "test_name", Res),
    ("LBLOINC", "loinc_code", Res),
    ("LOINCCD", "loinc_version", Raw),
    ("TSTDESC", "category", Res),
    ("TSTSTAT", "test_status", Res),
    ("TSTCOM", "test_comment", Res),
    ("TSTDTM", "tested_at", Res),
    ("TSTTYP", "test_type", Raw),
    ("RPTRESC", "result_text", Res),
    ("RPTRESCD", "result_code", Raw),
    ("RPTRESN", "result_numeric", Res),
    ("RPTRESNP", "result_numeric_precision", Raw),
    ("RPTNRLO", "ref_range_low", Res),
    ("RPTNRHI", "ref_range_high", Res),
    ("RPTU", "result_unit", Res),
    ("RPTUCD", "result_unit_code", Raw),
    ("CNVRESC", "conventional_result_text", Raw),
    ("CNVRESCD", "conventional_result_code", Raw),
    ("CNVRESN", "conventional_result_numeric", Raw),
    ("CNVRESNP", "conventional_result_numeric_precision", Raw),
    ("CNVNRLO", "conventional_ref_range_low", Raw),
    ("CNVNRHI", "conventional_ref_range_high", Raw),
    ("CNVU", "conventional_unit", Raw),
    ("CNVUCD", "conventional_unit_code", Raw),
    ("SIRESC", "si_result_text", Res),
    ("SIRESCD", "si_result_code", Raw),
    ("SIRESN", "si_result_numeric", Res),
    ("SIRESNP", "si_result_numeric_precision", Raw),
    ("SINRLO", "si_ref_range_low", Res),
    ("SINRHI", "si_ref_range_high", Res),
    ("SIU", "si_unit", Res),
    ("SIUCD", "si_unit_code", Raw),
    ("RPTRTYP", "result_type", Res),
    ("RPTRSTAT", "result_status", Res),
    ("ALRTFL", "alert_flag", Res),
    ("DELTFL", "delta_flag", Raw),
    ("TOXGR", "toxicity_grade", Raw),
    ("TOXGRCD", "toxicity_grade_code", Raw),
    ("EXCLFL", "excluded", Res),
    ("BLNDFL", "blinded_flag", Raw),
    ("RPTDTM", "reported_at", Res),
    ("TRNSTYP", "transaction_type", Raw),
];

/// `lab.reports_csv` mirrors the source CSV (same headers, text values);
/// `lab.reports` is the same data with readable names and typed columns.
/// Views are dropped and recreated so column changes apply cleanly.
fn view_queries() -> Vec<String> {
    let csv_select: Vec<String> = CSV_COLUMNS
        .iter()
        .map(|(csv, _, _)| format!("r.raw_record ->> '{csv}' AS \"{csv}\""))
        .collect();
    let readable_select: Vec<String> = CSV_COLUMNS
        .iter()
        .map(|(csv, readable, source)| match source {
            Acc => format!("a.{readable}"),
            Res => format!("r.{readable}"),
            Raw => format!("r.raw_record ->> '{csv}' AS {readable}"),
        })
        .chain(["r.source_file".to_string(), "r.ingested_at".to_string()])
        .collect();

    let mut queries = vec![
        "DROP VIEW IF EXISTS lab.reports_csv;".to_string(),
        "DROP VIEW IF EXISTS lab.reports;".to_string(),
        format!(
            "CREATE VIEW lab.reports_csv AS SELECT\n    {}\nFROM lab.results r;",
            csv_select.join(",\n    ")
        ),
        format!(
            "CREATE VIEW lab.reports AS SELECT\n    {}\nFROM lab.results r\n\
             JOIN lab.accessions a ON a.accession_number = r.accession_number;",
            readable_select.join(",\n    ")
        ),
        "COMMENT ON VIEW lab.reports_csv IS 'Lab results laid out like the LabCorp CSV (one row per test, original headers).';".to_string(),
        "COMMENT ON VIEW lab.reports IS 'Lab results joined with their accession, with readable column names.';".to_string(),
    ];
    queries.extend(CSV_COLUMNS.iter().map(|(csv, readable, _)| {
        format!("COMMENT ON COLUMN lab.reports.{readable} IS 'LabCorp CSV column {csv}';")
    }));
    queries
}

#[derive(Debug, Clone, PartialEq)]
struct Accession {
    accession_number: String,
    subject_id: String,
    redcap_event_name: String,
    visit_number: i32,
    visit_type: String,
    investigator_name: Option<String>,
    sex: Option<String>,
    collected_at: NaiveDateTime,
    received_at: Option<NaiveDateTime>,
    last_activity_at: Option<NaiveDateTime>,
}

#[derive(Debug, Clone, PartialEq)]
struct LabResult {
    accession_number: String,
    battery_id: String,
    battery_name: String,
    lab_test_id: String,
    lab_test_name: String,
    test_code: Option<String>,
    test_name: Option<String>,
    loinc_code: Option<String>,
    category: Option<String>,
    specimen_type: Option<String>,
    specimen_condition: Option<String>,
    test_status: String,
    test_comment: Option<String>,
    tested_at: Option<NaiveDateTime>,
    reported_at: Option<NaiveDateTime>,
    result_type: Option<String>,
    result_status: Option<String>,
    result_text: Option<String>,
    result_numeric: Option<f64>,
    result_unit: Option<String>,
    ref_range_low: Option<f64>,
    ref_range_high: Option<f64>,
    si_result_text: Option<String>,
    si_result_numeric: Option<f64>,
    si_unit: Option<String>,
    si_ref_range_low: Option<f64>,
    si_ref_range_high: Option<f64>,
    alert_flag: Option<String>,
    excluded: bool,
    raw_record: Map<String, Value>,
}

#[derive(Debug)]
struct LabReport {
    source_file: String,
    transfer_created_at: NaiveDateTime,
    accessions: Vec<Accession>,
    results: Vec<LabResult>,
    repaired_rows: usize,
}

/// Header name -> field index for one file.
struct Columns {
    names: Vec<String>,
    index: HashMap<String, usize>,
}

impl Columns {
    fn new(headers: &csv::StringRecord) -> ImportResult<Self> {
        let mut names: Vec<String> = headers
            .iter()
            .map(|h| h.trim_start_matches('\u{feff}').trim().to_string())
            .collect();
        // Every LabCorp header ends with a trailing comma.
        if names.last().is_some_and(|h| h.is_empty()) {
            names.pop();
        }

        let index: HashMap<String, usize> = names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        let missing: Vec<&str> = SOURCE_COLUMNS
            .iter()
            .chain(FLAG_COLUMNS)
            .copied()
            .filter(|c| !index.contains_key(*c))
            .collect();
        if !missing.is_empty() {
            return Err(format!("missing columns: {}", missing.join(", ")).into());
        }
        Ok(Self { names, index })
    }

    fn idx(&self, name: &str) -> usize {
        // Every name used is validated in `Columns::new`.
        self.index[name]
    }
}

/// Realign a raw record to the header. Returns the fields and whether the
/// row needed repair beyond dropping the trailing empty field.
///
/// LabCorp does not quote BATTRNAM values containing a comma
/// (`WBC, ADDITIONAL`), which shifts every later field right by one. Those
/// rows also lose one field later on: either the trailing comma or one of
/// the empty flag fields before RPTDTM.
fn repair_record(
    columns: &Columns,
    record: &csv::StringRecord,
) -> ImportResult<(Vec<String>, bool)> {
    let expected = columns.names.len();
    let mut fields: Vec<String> = record.iter().map(str::to_string).collect();
    let mut repaired = false;

    let battery_name = columns.idx("BATTRNAM");
    // PLBNUM is never legitimately space-prefixed; a leading space means the
    // tail of BATTRNAM spilled into it.
    if fields
        .get(battery_name + 1)
        .is_some_and(|f| f.starts_with(' '))
    {
        let spill = fields.remove(battery_name + 1);
        fields[battery_name] = format!("{},{}", fields[battery_name], spill);
        repaired = true;
    }

    if fields.len() == expected + 1 && fields.last().is_some_and(|f| f.is_empty()) {
        fields.pop();
    }

    let first_flag = columns.idx(FLAG_COLUMNS[0]);
    let reported_at = columns.idx("RPTDTM");
    // Right length but shifted: RPTDTM landed in the last flag slot and the
    // remaining "field" is the trailing comma.
    if repaired
        && fields.len() == expected
        && fields.last().is_some_and(|f| f.is_empty())
        && !fields[reported_at - 1].is_empty()
    {
        fields.pop();
    }

    if repaired && fields.len() + 1 == expected {
        // The flags shifted left by one; only safe to re-pad if all of them
        // are empty, otherwise we cannot tell which one was dropped.
        if fields[first_flag..reported_at - 1]
            .iter()
            .all(|f| f.is_empty())
        {
            fields.insert(reported_at - 1, String::new());
        }
    }

    if fields.len() != expected {
        return Err(format!("expected {expected} fields, found {}", fields.len()).into());
    }
    Ok((fields, repaired))
}

fn parse_optional(raw: &str) -> Option<String> {
    let value = raw.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn parse_datetime(raw: &str) -> ImportResult<Option<NaiveDateTime>> {
    let value = raw.trim();
    if value.is_empty() {
        return Ok(None);
    }
    NaiveDateTime::parse_from_str(value, DATETIME_FORMAT)
        .map(Some)
        .map_err(|e| format!("invalid datetime '{value}': {e}").into())
}

fn parse_number(raw: &str) -> ImportResult<Option<f64>> {
    let value = raw.trim();
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<f64>()
        .map(Some)
        .map_err(|e| format!("invalid number '{value}': {e}").into())
}

/// One realigned CSV row with typed accessors.
struct Row<'a> {
    columns: &'a Columns,
    fields: Vec<String>,
}

impl Row<'_> {
    fn raw(&self, name: &str) -> &str {
        &self.fields[self.columns.idx(name)]
    }

    fn optional(&self, name: &str) -> Option<String> {
        parse_optional(self.raw(name))
    }

    fn required(&self, name: &str) -> ImportResult<String> {
        self.optional(name)
            .ok_or_else(|| format!("{name} is empty").into())
    }

    fn datetime(&self, name: &str) -> ImportResult<Option<NaiveDateTime>> {
        parse_datetime(self.raw(name)).map_err(|e| format!("{name}: {e}").into())
    }

    fn required_datetime(&self, name: &str) -> ImportResult<NaiveDateTime> {
        self.datetime(name)?
            .ok_or_else(|| format!("{name} is empty").into())
    }

    fn number(&self, name: &str) -> ImportResult<Option<f64>> {
        parse_number(self.raw(name)).map_err(|e| format!("{name}: {e}").into())
    }

    fn raw_record(&self) -> Map<String, Value> {
        self.columns
            .names
            .iter()
            .zip(&self.fields)
            .filter_map(|(name, field)| {
                parse_optional(field).map(|value| (name.clone(), Value::String(value)))
            })
            .collect()
    }

    fn accession(&self) -> ImportResult<Accession> {
        let visit_number = self.required("VISITNUM")?;
        Ok(Accession {
            accession_number: self.required("ACCSNNUM")?,
            subject_id: self.required("SUBJID")?,
            redcap_event_name: self.required("VISIT")?,
            visit_number: visit_number
                .parse()
                .map_err(|e| format!("VISITNUM: invalid integer '{visit_number}': {e}"))?,
            visit_type: self.required("VISITTYP")?,
            investigator_name: self.optional("INVNAM"),
            sex: self.optional("SEX"),
            collected_at: self.required_datetime("LBDTM")?,
            received_at: self.datetime("RCVDTM")?,
            last_activity_at: self.datetime("LACTDTM")?,
        })
    }

    fn result(&self) -> ImportResult<LabResult> {
        Ok(LabResult {
            accession_number: self.required("ACCSNNUM")?,
            battery_id: self.required("BATTRID")?,
            battery_name: self.required("BATTRNAM")?,
            lab_test_id: self.required("LBTESTCD")?,
            lab_test_name: self.required("LBTEST")?,
            test_code: self.optional("TSTCD"),
            test_name: self.optional("TSTNAM"),
            loinc_code: self.optional("LBLOINC"),
            category: self.optional("TSTDESC"),
            specimen_type: self.optional("LBSPEC"),
            specimen_condition: self.optional("SPECCND"),
            test_status: self.required("TSTSTAT")?,
            test_comment: self.optional("TSTCOM"),
            tested_at: self.datetime("TSTDTM")?,
            reported_at: self.datetime("RPTDTM")?,
            result_type: self.optional("RPTRTYP"),
            result_status: self.optional("RPTRSTAT"),
            result_text: self.optional("RPTRESC"),
            result_numeric: self.number("RPTRESN")?,
            result_unit: self.optional("RPTU"),
            ref_range_low: self.number("RPTNRLO")?,
            ref_range_high: self.number("RPTNRHI")?,
            si_result_text: self.optional("SIRESC"),
            si_result_numeric: self.number("SIRESN")?,
            si_unit: self.optional("SIU"),
            si_ref_range_low: self.number("SINRLO")?,
            si_ref_range_high: self.number("SINRHI")?,
            alert_flag: self.optional("ALRTFL"),
            excluded: self.optional("EXCLFL").as_deref() == Some("EX"),
            raw_record: self.raw_record(),
        })
    }
}

/// Subject ID from `<subject>_lab_report_<...>.csv`.
fn subject_from_file_name(path: &Path) -> ImportResult<String> {
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("invalid file name: {}", path.display()))?;
    file_name
        .split_once(FILE_NAME_MARKER)
        .map(|(subject, _)| subject.to_string())
        .filter(|subject| !subject.is_empty())
        .ok_or_else(|| {
            format!("file name does not contain '{FILE_NAME_MARKER}': {file_name}").into()
        })
}

fn read_lab_report(path: &Path) -> ImportResult<LabReport> {
    let file_subject = subject_from_file_name(path)?;
    let mut reader = csv::ReaderBuilder::new().flexible(true).from_path(path)?;
    let columns = Columns::new(reader.headers()?)?;

    let mut transfer_created_at = None;
    let mut accessions: BTreeMap<String, Accession> = BTreeMap::new();
    let mut results = Vec::new();
    let mut result_keys = HashSet::new();
    let mut repaired_rows = 0;

    for (row_offset, record) in reader.records().enumerate() {
        let row_number = row_offset + 2;
        let row_err = |e: Box<dyn Error + Send + Sync>| format!("row {row_number}: {e}");

        let (fields, repaired) = repair_record(&columns, &record?).map_err(row_err)?;
        let row = Row {
            columns: &columns,
            fields,
        };
        if repaired {
            warn!(file = %path.display(), row = row_number, "Repaired mis-quoted row");
            repaired_rows += 1;
        }

        let file_created = row.required_datetime("FILCRDTM").map_err(row_err)?;
        if *transfer_created_at.get_or_insert(file_created) != file_created {
            return Err(row_err("FILCRDTM differs from earlier rows".into()).into());
        }

        let accession = row.accession().map_err(row_err)?;
        if accession.subject_id != file_subject {
            return Err(row_err(
                format!(
                    "SUBJID {} does not match file name subject {file_subject}",
                    accession.subject_id
                )
                .into(),
            )
            .into());
        }
        match accessions.get(&accession.accession_number) {
            Some(existing) if *existing != accession => {
                return Err(row_err(
                    format!(
                        "accession {} details differ from earlier rows",
                        accession.accession_number
                    )
                    .into(),
                )
                .into());
            }
            Some(_) => {}
            None => {
                accessions.insert(accession.accession_number.clone(), accession);
            }
        }

        let result = row.result().map_err(row_err)?;
        let key = (
            result.accession_number.clone(),
            result.battery_id.clone(),
            result.lab_test_id.clone(),
        );
        if !result_keys.insert(key) {
            return Err(row_err(
                format!(
                    "duplicate result for accession {} battery {} test {}",
                    result.accession_number, result.battery_id, result.lab_test_id
                )
                .into(),
            )
            .into());
        }
        results.push(result);
    }

    Ok(LabReport {
        source_file: path.display().to_string(),
        transfer_created_at: transfer_created_at.ok_or("file has no data rows")?,
        accessions: accessions.into_values().collect(),
        results,
        repaired_rows,
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

/// `ON CONFLICT` clause that updates every non-key column, but only when the
/// incoming row comes from an equal or newer transfer.
fn upsert_clause(table: &str, key_columns: &[&str], columns: &[&str]) -> String {
    let updates: Vec<String> = columns
        .iter()
        .filter(|c| !key_columns.contains(c))
        .map(|c| format!("{c} = EXCLUDED.{c}"))
        .chain(std::iter::once("ingested_at = NOW()".to_string()))
        .collect();
    format!(
        " ON CONFLICT ({}) DO UPDATE SET {} WHERE EXCLUDED.transfer_created_at >= {table}.transfer_created_at",
        key_columns.join(", "),
        updates.join(", "),
    )
}

async fn init_db(pool: &PgPool) -> ImportResult<()> {
    info!("Initializing database with lab schema, lab report tables and views");
    let queries: Vec<String> = INIT_QUERIES
        .iter()
        .map(|&q| q.to_string())
        .chain(view_queries())
        .collect();
    db::execute_queries_in_transaction(pool, &queries).await?;
    Ok(())
}

async fn upsert_lab_report(pool: &PgPool, report: &LabReport) -> ImportResult<()> {
    let mut tx = pool.begin().await?;

    let mut accessions: QueryBuilder<Postgres> = QueryBuilder::new(format!(
        "INSERT INTO lab.accessions ({}) ",
        ACCESSION_COLUMNS.join(", ")
    ));
    accessions.push_values(&report.accessions, |mut b, a| {
        b.push_bind(&a.accession_number)
            .push_bind(&a.subject_id)
            .push_bind(&a.redcap_event_name)
            .push_bind(a.visit_number)
            .push_bind(&a.visit_type)
            .push_bind(&a.investigator_name)
            .push_bind(&a.sex)
            .push_bind(a.collected_at)
            .push_bind(a.received_at)
            .push_bind(a.last_activity_at)
            .push_bind(report.transfer_created_at)
            .push_bind(&report.source_file);
    });
    accessions.push(upsert_clause(
        "lab.accessions",
        &["accession_number"],
        ACCESSION_COLUMNS,
    ));
    accessions.build().execute(&mut *tx).await?;

    let mut results: QueryBuilder<Postgres> = QueryBuilder::new(format!(
        "INSERT INTO lab.results ({}) ",
        RESULT_COLUMNS.join(", ")
    ));
    results.push_values(&report.results, |mut b, r| {
        b.push_bind(&r.accession_number)
            .push_bind(&r.battery_id)
            .push_bind(&r.battery_name)
            .push_bind(&r.lab_test_id)
            .push_bind(&r.lab_test_name)
            .push_bind(&r.test_code)
            .push_bind(&r.test_name)
            .push_bind(&r.loinc_code)
            .push_bind(&r.category)
            .push_bind(&r.specimen_type)
            .push_bind(&r.specimen_condition)
            .push_bind(&r.test_status)
            .push_bind(&r.test_comment)
            .push_bind(r.tested_at)
            .push_bind(r.reported_at)
            .push_bind(&r.result_type)
            .push_bind(&r.result_status)
            .push_bind(&r.result_text)
            .push_bind(r.result_numeric)
            .push_bind(&r.result_unit)
            .push_bind(r.ref_range_low)
            .push_bind(r.ref_range_high)
            .push_bind(&r.si_result_text)
            .push_bind(r.si_result_numeric)
            .push_bind(&r.si_unit)
            .push_bind(r.si_ref_range_low)
            .push_bind(r.si_ref_range_high)
            .push_bind(&r.alert_flag)
            .push_bind(r.excluded)
            .push_bind(Json(&r.raw_record))
            .push_bind(report.transfer_created_at)
            .push_bind(&report.source_file);
    });
    results.push(upsert_clause(
        "lab.results",
        &["accession_number", "battery_id", "lab_test_id"],
        RESULT_COLUMNS,
    ));
    results.build().execute(&mut *tx).await?;

    tx.commit().await?;
    Ok(())
}

#[derive(Debug, Default)]
struct Summary {
    ingested: usize,
    failed: usize,
    accessions: usize,
    results: usize,
    repaired_rows: usize,
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

    let mut summary = Summary::default();
    let mut reports = Vec::with_capacity(paths.len());
    for path in paths {
        match read_lab_report(&path) {
            Ok(report) => reports.push(report),
            Err(e) => {
                warn!(file = %path.display(), error = %e, "Failed to read lab report");
                summary.failed += 1;
            }
        }
    }
    // Transfers are cumulative; apply older transfers first so newer ones win.
    reports.sort_by(|a, b| {
        (a.transfer_created_at, &a.source_file).cmp(&(b.transfer_created_at, &b.source_file))
    });

    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;

    init_db(&pool).await?;

    let progress_style = ProgressStyle::default_bar()
        .template("{span_child_prefix}{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
        .expect("valid template")
        .progress_chars("#>-");

    let ingest_span = tracing::info_span!("ingesting_lab_reports");
    ingest_span.pb_set_style(&progress_style);
    ingest_span.pb_set_length(reports.len() as u64);
    ingest_span.pb_set_message("Ingesting lab reports");

    for report in &reports {
        match upsert_lab_report(&pool, report).await {
            Ok(()) => {
                info!(
                    file = %report.source_file,
                    accessions = report.accessions.len(),
                    results = report.results.len(),
                    "Ingested lab report"
                );
                summary.ingested += 1;
                summary.accessions += report.accessions.len();
                summary.results += report.results.len();
                summary.repaired_rows += report.repaired_rows;
            }
            Err(e) => {
                warn!(file = %report.source_file, error = %e, "Failed to ingest lab report");
                summary.failed += 1;
            }
        }
        ingest_span.pb_inc(1);
    }
    drop(ingest_span);

    info!(
        ingested = summary.ingested,
        failed = summary.failed,
        accession_rows = summary.accessions,
        result_rows = summary.results,
        repaired_rows = summary.repaired_rows,
        "Lab report ingestion complete"
    );

    pool.close().await;

    if summary.failed > 0 {
        let msg = format!(
            "{} file(s) failed lab report ingestion; see warnings above",
            summary.failed
        );
        if cli.allow_file_errors {
            warn!(failed = summary.failed, "{msg}");
        } else {
            return Err(msg.into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "\u{feff}VERSION,FILCRDTM,TRMSRNUM,TRMSRNAM,STUDYID,STUDNAM,TRMTYP,SITEID,INVID,INVNAM,SCRNNUM,SUBJID,SUBJSID,SUBJINIT,SEX,SEXCD,BRTHDTM,RACE,RACECD,VISITNUM,VISIT,VISITTYP,VISITMOD,LBNUM,LBNAM,ACCSNNUM,LACTDTM,RECEXTYP,SPECNUM,LBDTM,PTMEL,PTMELTX,COLENDTM,RCVDTM,SPECCND,SPECCOM,SPECICOM,SPECMNUM,SPECMCD,LBSPEC,AGEATCOL,AGEU,FASTSTAT,BATTRID,BATTRNAM,PLBNUM,PLBNAM,LBTESTCD,LBTEST,TSTCD,TSTNAM,LBLOINC,LOINCCD,TSTDESC,TSTSTAT,TSTCOM,TSTDTM,TSTTYP,RPTRESC,RPTRESCD,RPTRESN,RPTRESNP,RPTNRLO,RPTNRHI,RPTU,RPTUCD,CNVRESC,CNVRESCD,CNVRESN,CNVRESNP,CNVNRLO,CNVNRHI,CNVU,CNVUCD,SIRESC,SIRESCD,SIRESN,SIRESNP,SINRLO,SINRHI,SIU,SIUCD,RPTRTYP,RPTRSTAT,ALRTFL,DELTFL,TOXGR,TOXGRCD,EXCLFL,BLNDFL,RPTDTM,TRNSTYP,";

    // Well-formed row (trailing comma).
    const NORMAL_ROW: &str = "01-0-02,1899-09-01T17:09:19,LABCORP,LABCORP,SCZ201,SCZ201,C,XX,XX,John Doe,,XX00001,,,M,,,,,5,day_1a_predose_arm_1,S,,LABCORP,LABCORP,0000000001,1899-07-02T11:52:48,BASE,,1899-07-01T09:53:00,,,,1899-07-02T05:10:00,,,,,,BLOOD,26,Y,,HM3900,HEMATOLOGY&DIFFERENTIAL PANEL,LABCORP,LABCORP,HMT10,Monocytes,MONO,Monocytes,742-7,LOINC v2.82,HEMATOLOGY,D,,1899-07-02T11:05:19,S,0.3,,0.3,,0.12,0.92,x10^3/uL,,0.3,,0.3,,0.12,0.92,x10^3/uL,,0.3,,0.3,,0.12,0.92,GI/L,,N,F,,,,,,,1899-07-02T11:05:19,I,";

    // Unquoted comma in BATTRNAM and one empty flag field dropped (XX00002).
    const DROPPED_FLAG_ROW: &str = "01-0-02,1899-09-01T17:09:19,LABCORP,LABCORP,SCZ201,SCZ201,C,XX,XX,Jane Doe,,XX00002,,,F,,,,,2,screening_arm_1,S,,LABCORP,LABCORP,0000000002,1899-07-07T11:13:39,BASE,,1899-06-30T14:35:00,,,,1899-07-01T08:10:00,,,,,,BLOOD,26,Y,,HM143,WBC, ADDITIONAL,LABCORP,LABCORP,HMT51,CBC Comment:,WBCMORPH,Leukocyte Cell Morphology,11156-7,LOINC v2.82,HEMATOLOGY,D,,1899-07-02T17:02:05,S,Target Cells Present,,,,,,,,Target Cells Present,,,,,,,,Target Cells Present,,,,,,,,T,F,,,,,,1899-07-02T17:02:05,I,";

    // Unquoted comma in BATTRNAM and trailing comma dropped (XX00003).
    const DROPPED_TRAILING_ROW: &str = "01-0-02,1899-09-01T17:09:19,LABCORP,LABCORP,SCZ201,SCZ201,C,XX,XX,Richard Roe,,XX00003,,,F,,,,,5,day_1a_predose_arm_1,S,,LABCORP,LABCORP,0000000003,1899-08-18T15:52:42,BASE,,1899-08-14T11:00:00,,,,1899-08-15T05:13:00,,,,,,BLOOD,26,Y,,HM497,NEUTROPHIL, ADDITIONAL,LABCORP,LABCORP,HMT20,Bands,NEUTB,Neutrophils Band Form,26507-4,LOINC v2.82,HEMATOLOGY,D,,1899-08-15T16:03:26,S,0.06,,0.06,,0,0.27,x10^3/uL,,0.06,,0.06,,0,0.27,x10^3/uL,,0.06,,0.06,,0,0.27,GI/L,,N,F,,,,,,,1899-08-15T16:03:26,I";

    fn parse_line(line: &str) -> (Columns, csv::StringRecord) {
        let data = format!("{HEADER}\n{line}\n");
        let mut reader = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(data.as_bytes());
        let columns = Columns::new(reader.headers().unwrap()).unwrap();
        let record = reader.records().next().unwrap().unwrap();
        (columns, record)
    }

    fn row_for(line: &str) -> (Columns, Vec<String>, bool) {
        let (columns, record) = parse_line(line);
        let (fields, repaired) = repair_record(&columns, &record).unwrap();
        (columns, fields, repaired)
    }

    #[test]
    fn test_columns_strip_bom_and_trailing() {
        let (columns, _) = parse_line(NORMAL_ROW);
        assert_eq!(columns.names.len(), 92);
        assert_eq!(columns.names[0], "VERSION");
        assert_eq!(columns.names.last().unwrap(), "TRNSTYP");
    }

    #[test]
    fn test_repair_normal_row() {
        let (columns, fields, repaired) = row_for(NORMAL_ROW);
        assert!(!repaired);
        let row = Row {
            columns: &columns,
            fields,
        };
        assert_eq!(row.raw("BATTRNAM"), "HEMATOLOGY&DIFFERENTIAL PANEL");
        assert_eq!(row.raw("TRNSTYP"), "I");
    }

    #[test]
    fn test_repair_dropped_flag_row() {
        let (columns, fields, repaired) = row_for(DROPPED_FLAG_ROW);
        assert!(repaired);
        let row = Row {
            columns: &columns,
            fields,
        };
        assert_eq!(row.raw("BATTRNAM"), "WBC, ADDITIONAL");
        assert_eq!(row.raw("PLBNUM"), "LABCORP");
        assert_eq!(row.raw("LBTESTCD"), "HMT51");
        assert_eq!(row.raw("TSTTYP"), "S");
        assert_eq!(row.raw("RPTRSTAT"), "F");
        assert_eq!(row.raw("BLNDFL"), "");
        assert_eq!(row.raw("RPTDTM"), "1899-07-02T17:02:05");
        assert_eq!(row.raw("TRNSTYP"), "I");
    }

    #[test]
    fn test_repair_dropped_trailing_row() {
        let (columns, fields, repaired) = row_for(DROPPED_TRAILING_ROW);
        assert!(repaired);
        let row = Row {
            columns: &columns,
            fields,
        };
        assert_eq!(row.raw("BATTRNAM"), "NEUTROPHIL, ADDITIONAL");
        assert_eq!(row.raw("LBTESTCD"), "HMT20");
        assert_eq!(row.raw("RPTRESN"), "0.06");
        assert_eq!(row.raw("RPTDTM"), "1899-08-15T16:03:26");
        assert_eq!(row.raw("TRNSTYP"), "I");
    }

    #[test]
    fn test_repair_rejects_unexpected_length() {
        let (columns, record) = parse_line("a,b,c");
        assert!(repair_record(&columns, &record).is_err());
    }

    #[test]
    fn test_parse_result_row() {
        let (columns, fields, _) = row_for(NORMAL_ROW);
        let row = Row {
            columns: &columns,
            fields,
        };
        let accession = row.accession().unwrap();
        assert_eq!(accession.accession_number, "0000000001");
        assert_eq!(accession.subject_id, "XX00001");
        assert_eq!(accession.redcap_event_name, "day_1a_predose_arm_1");
        assert_eq!(accession.visit_number, 5);
        assert_eq!(accession.sex.as_deref(), Some("M"));
        assert_eq!(
            accession.collected_at,
            NaiveDateTime::parse_from_str("1899-07-01T09:53:00", DATETIME_FORMAT).unwrap()
        );

        let result = row.result().unwrap();
        assert_eq!(result.lab_test_id, "HMT10");
        assert_eq!(result.test_code.as_deref(), Some("MONO"));
        assert_eq!(result.result_numeric, Some(0.3));
        assert_eq!(result.ref_range_low, Some(0.12));
        assert_eq!(result.si_unit.as_deref(), Some("GI/L"));
        assert_eq!(result.alert_flag, None);
        assert!(!result.excluded);
        assert_eq!(result.raw_record["CNVU"], "x10^3/uL");
        assert!(!result.raw_record.contains_key("SCRNNUM"));
    }

    #[test]
    fn test_parse_helpers() {
        assert_eq!(parse_optional("  "), None);
        assert_eq!(parse_optional(" Neg "), Some("Neg".to_string()));
        assert_eq!(parse_number("").unwrap(), None);
        assert_eq!(parse_number("-1.5").unwrap(), Some(-1.5));
        assert!(parse_number("<2.00").is_err());
        assert_eq!(parse_datetime("").unwrap(), None);
        assert!(parse_datetime("1899-07-01T09:53:00").unwrap().is_some());
        assert!(parse_datetime("07/01/1899").is_err());
    }

    #[test]
    fn test_subject_from_file_name() {
        let path = Path::new("/x/XX00001_lab_report_LAB_SCZ201_02SEP1899.csv");
        assert_eq!(subject_from_file_name(path).unwrap(), "XX00001");
        assert!(subject_from_file_name(Path::new("/x/XX00001_report.csv")).is_err());
    }

    #[test]
    fn test_csv_columns_match_header() {
        let (columns, _) = parse_line(NORMAL_ROW);
        let csv: Vec<&str> = CSV_COLUMNS.iter().map(|(csv, _, _)| *csv).collect();
        assert_eq!(csv, columns.names);

        let readable: HashSet<&str> = CSV_COLUMNS.iter().map(|(_, r, _)| *r).collect();
        assert_eq!(
            readable.len(),
            CSV_COLUMNS.len(),
            "readable names must be unique"
        );
    }

    #[test]
    fn test_view_typed_sources_exist() {
        for (csv, readable, source) in CSV_COLUMNS {
            let typed = match source {
                Acc => ACCESSION_COLUMNS,
                Res => RESULT_COLUMNS,
                Raw => continue,
            };
            assert!(
                typed.contains(readable),
                "{csv} -> {readable} is not a typed column"
            );
        }
        let queries = view_queries();
        assert!(queries[2].contains(r#"r.raw_record ->> 'VERSION' AS "VERSION""#));
        assert!(queries[3].contains("a.subject_id"));
        assert!(queries[3].contains("r.raw_record ->> 'CNVRESN' AS conventional_result_numeric"));
    }

    #[test]
    fn test_upsert_clause() {
        let clause = upsert_clause(
            "lab.accessions",
            &["accession_number"],
            &["accession_number", "sex"],
        );
        assert_eq!(
            clause,
            " ON CONFLICT (accession_number) DO UPDATE SET sex = EXCLUDED.sex, ingested_at = NOW() \
             WHERE EXCLUDED.transfer_created_at >= lab.accessions.transfer_created_at"
        );
    }

    #[test]
    fn test_read_lab_report_from_disk() {
        let dir = std::env::temp_dir().join(format!("lab_reports_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("XX00001_lab_report_LAB_SCZ201_02SEP1899.csv");
        let second = NORMAL_ROW.replace(
            "HMT10,Monocytes,MONO,Monocytes,742-7",
            "HMT100,MCH,MCH,Ery. Mean Corpuscular Hemoglobin,785-6",
        );
        std::fs::write(&path, format!("{HEADER}\r\n{NORMAL_ROW}\r\n{second}\r\n")).unwrap();
        let report = read_lab_report(&path).unwrap();
        assert_eq!(report.accessions.len(), 1);
        assert_eq!(report.results.len(), 2);
        assert_eq!(report.repaired_rows, 0);

        // Duplicate result keys are rejected.
        std::fs::write(
            &path,
            format!("{HEADER}\r\n{NORMAL_ROW}\r\n{NORMAL_ROW}\r\n"),
        )
        .unwrap();
        assert!(read_lab_report(&path).is_err());

        // Subject must match the file name.
        let other = dir.join("XX00003_lab_report_LAB_SCZ201_02SEP1899.csv");
        std::fs::write(&other, format!("{HEADER}\r\n{NORMAL_ROW}\r\n")).unwrap();
        assert!(read_lab_report(&other).is_err());

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
