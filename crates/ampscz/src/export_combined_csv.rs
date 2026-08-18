use std::{
	collections::{BTreeMap, BTreeSet},
	error::Error,
	fs,
	path::{Path, PathBuf},
};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use clap::Parser;
use futures_util::TryStreamExt;
use indicatif::ProgressStyle;
use serde_json::Value;
use sqlx::{PgPool, Row};
use tracing::info;
use tracing_indicatif::{IndicatifLayer, span_ext::IndicatifSpanExt};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use ampscz::constants::VISIT_ORDER;

type ExportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type CsvRow = BTreeMap<String, String>;

const DERIVED_COLUMNS: &[&str] = &[
	"visit_started",
	"visit_status",
	"visit_completed",
	"converted",
	"converted_visit",
	"removed",
	"removed_visit",
	"removed_date",
	"removed_info_source",
	"recruited",
	"recruitment_status",
	"recruitment_status_v2",
	"gender",
	"cohort",
	"age_at_consent",
];

const MANDATORY_COLUMNS: &[&str] = &[
	"subject_id",
	"visit_started",
	"visit_status",
	"visit_status_string",
	"visit_completed",
	"converted",
	"converted_visit",
	"conversion_date",
	"removed",
	"removed_visit",
	"removed_date",
	"removed_info_source",
	"recruited",
	"recruitment_status",
	"recruitment_status_v2",
	"gender",
	"cohort",
	"age_at_consent",
	"subjectid",
];

/// Export legacy AMPSCZ combined REDCap CSVs.
///
/// Required environment variable: DB_URI. Network membership is resolved via
/// `subjects`, `site`, and `site.network_id`, matching the legacy exporter.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
	/// Directory where combined CSV files are written.
	#[arg(short, long)]
	output_dir: PathBuf,

	/// One or more networks to export (for example: ProNET PRESCIENT).
	#[arg(short, long, required = true, num_args = 1..)]
	network: Vec<String>,

	/// Visits to export. Defaults to conversion, floating_forms, and all standard visits.
	#[arg(short, long, num_args = 1..)]
	visit: Vec<String>,

	/// Maximum PostgreSQL connections used by the export.
	#[arg(long, default_value_t = 1)]
	max_connections: u32,
}

fn default_visits() -> Vec<String> {
	["conversion", "floating_forms"]
		.into_iter()
		.chain(VISIT_ORDER.iter().copied())
		.map(str::to_owned)
		.collect()
}

fn output_name(network: &str, visit: &str) -> String {
	format!("AMPSCZ-combined-redcap_{visit}_{network}-day1to1.csv")
}

fn is_truthy(value: &str) -> bool {
	matches!(value, "True" | "true" | "1")
}

fn legacy_visit_status(visit_status: &str, removed: &str, converted: &str) -> String {
	let status = match visit_status {
		"screening" => "screen".to_owned(),
		"baseline" => "baseln".to_owned(),
		value if value.strip_prefix("month_").is_some() => {
			format!("month{}", value.strip_prefix("month_").unwrap_or_default())
		}
		_ => String::new(),
	};
	if is_truthy(removed) {
		"removed".to_owned()
	} else if is_truthy(converted) {
		"converted".to_owned()
	} else {
		status
	}
}

fn json_value_to_string(value: &Value) -> String {
	match value {
		Value::Null => String::new(),
		Value::Bool(value) => if *value { "1" } else { "0" }.to_owned(),
		Value::Number(value) => value.to_string(),
		Value::String(value) => value.clone(),
		Value::Array(_) | Value::Object(_) => value.to_string(),
	}
}

fn normalize_value(value: String) -> String {
	let value = match value.as_str() {
		"None" | "NaT" => String::new(),
		"True" => "1".to_owned(),
		"False" => "0".to_owned(),
		_ => value,
	};
	let integer_with_decimal_suffix = value
		.strip_suffix(".0")
		.is_some_and(|prefix| prefix.parse::<i64>().is_ok());
	if integer_with_decimal_suffix {
		value[..value.len() - 2].to_owned()
	} else {
		value
	}
}

fn format_redcap_value(value: &str, validation: &str) -> String {
	let value = value.trim();
	if value.is_empty() {
		return String::new();
	}
	let datetime = ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
		.iter()
		.find_map(|format| NaiveDateTime::parse_from_str(value, format).ok());
	match validation {
		"date_ymd" => datetime
			.map(|value| value.date().to_string())
			.or_else(|| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok().map(|value| value.to_string()))
			.unwrap_or_else(|| value.to_owned()),
		"datetime_ymd" => datetime
			.map(|value| value.format("%Y-%m-%d %H:%M").to_string())
			.unwrap_or_else(|| value.to_owned()),
		"time" => datetime
			.map(|value| value.time().format("%H:%M").to_string())
			.or_else(|| {
				["%H:%M:%S%.f", "%H:%M"]
					.iter()
					.find_map(|format| NaiveTime::parse_from_str(value, format).ok())
					.map(|value| value.format("%H:%M").to_string())
			})
			.unwrap_or_else(|| value.to_owned()),
		_ => value.to_owned(),
	}
}

async fn date_validations(pool: &PgPool) -> ExportResult<BTreeMap<String, String>> {
	let rows = sqlx::query(
		"SELECT field_name, text_validation_type_or_show_slider_number FROM forms.data_dictionary WHERE text_validation_type_or_show_slider_number IN ('date_ymd', 'datetime_ymd', 'time')",
	)
	.fetch_all(pool)
	.await?;
	Ok(rows
		.into_iter()
		.filter_map(|row| {
			Some((
				row.try_get::<String, _>("field_name").ok()?,
				row.try_get::<String, _>("text_validation_type_or_show_slider_number").ok()?,
			))
		})
		.collect())
}

/// Read and merge one network/visit. The lateral joins guarantee at most one
/// derived row per subject even when a derived source temporarily has duplicates.
async fn read_visit(
	pool: &PgPool,
	network: &str,
	visit: &str,
	validations: &BTreeMap<String, String>,
	read_span: &tracing::Span,
) -> ExportResult<Vec<CsvRow>> {
	let event_pattern = format!("{visit}_arm_%");
	let sql = r#"
		SELECT f.subject_id, f.form_name, f.form_data,
			   status.timepoint::text AS visit_started, status.timepoint::text AS visit_status,
			   completed.completed_timepoint::text AS visit_completed,
			   conversion.converted::text AS converted, conversion.converted_visit::text AS converted_visit,
			   conversion.conversion_date::text AS conversion_date,
			   removed.removed::text AS removed, removed.removed_event::text AS removed_visit,
			   removed.removed_date::text AS removed_date, removed.removed_info_source::text AS removed_info_source,
			   recruitment.recruited::text AS recruited, recruitment.recruitment_status::text AS recruitment_status,
			   recruitment.recruitment_status_v2::text AS recruitment_status_v2,
			   filters.gender::text AS gender, filters.cohort::text AS cohort, filters.age::text AS age_at_consent,
			   fasting.time_fasting::text AS time_fasting, vials.blood_vial_count::text AS blood_vial_count,
			   vials.saliva_vial_count::text AS saliva_vial_count
				FROM forms.forms AS f
				JOIN subjects ON subjects.id = f.subject_id
				JOIN site ON site.id = subjects.site_id AND site.network_id = $2
				LEFT JOIN forms_derived.filters AS filters ON filters.subject = f.subject_id
		LEFT JOIN LATERAL (SELECT timepoint FROM forms_derived.subject_visit_status WHERE subject_id = f.subject_id LIMIT 1) AS status ON true
		LEFT JOIN LATERAL (SELECT completed_timepoint FROM forms_derived.subject_visit_completed WHERE subject_id = f.subject_id LIMIT 1) AS completed ON true
		LEFT JOIN LATERAL (SELECT converted, converted_visit, conversion_date FROM forms_derived.conversion_status WHERE subject_id = f.subject_id LIMIT 1) AS conversion ON true
		LEFT JOIN LATERAL (SELECT removed, removed_event, removed_date, removed_info_source FROM forms_derived.subject_removed WHERE subject_id = f.subject_id LIMIT 1) AS removed ON true
		LEFT JOIN LATERAL (SELECT recruited, recruitment_status, recruitment_status_v2 FROM forms_derived.recruitment_status WHERE subject_id = f.subject_id LIMIT 1) AS recruitment ON true
		LEFT JOIN LATERAL (SELECT time_fasting FROM forms_derived.subject_time_fasting WHERE subject_id = f.subject_id AND event_name = $1 LIMIT 1) AS fasting ON $1 IN ('baseline', 'month_2')
		LEFT JOIN LATERAL (SELECT blood_vial_count, saliva_vial_count FROM forms_derived.subject_vials_count WHERE subject_id = f.subject_id AND timepoint = $1 LIMIT 1) AS vials ON $1 IN ('baseline', 'month_2')
		WHERE f.event_name LIKE $3
		ORDER BY f.subject_id, f.form_name
	"#;

	let mut rows_by_subject = BTreeMap::<String, CsvRow>::new();
	let mut stream = sqlx::query(sql)
		.bind(visit)
		.bind(network)
		.bind(event_pattern)
		.fetch(pool);
	while let Some(record) = stream.try_next().await? {
		read_span.pb_tick();
		let subject_id: String = record.try_get("subject_id")?;
		let row = rows_by_subject.entry(subject_id.clone()).or_insert_with(|| {
			let mut row = CsvRow::new();
			row.insert("subject_id".to_owned(), subject_id);
			row
		});
		for column in DERIVED_COLUMNS {
			if !row.contains_key(*column) {
				row.insert(
					(*column).to_owned(),
					record.try_get::<Option<String>, _>(*column)?.unwrap_or_default(),
				);
			}
		}
		if matches!(visit, "baseline" | "month_2") {
			for column in ["time_fasting", "blood_vial_count", "saliva_vial_count"] {
				if !row.contains_key(column) {
					let default = if column.ends_with("vial_count") { "0" } else { "" };
					row.insert(
						column.to_owned(),
						record
							.try_get::<Option<String>, _>(column)?
							.unwrap_or_else(|| default.to_owned()),
					);
				}
			}
		}
		let form_data: Value = record.try_get("form_data")?;
		if let Value::Object(values) = form_data {
			for (field, value) in values {
				// Python's `combine_first` retains the first non-null form value.
				if !value.is_null() && row.get(&field).is_none_or(String::is_empty) {
					let value = json_value_to_string(&value);
					let value = validations
						.get(&field)
						.map_or(value.clone(), |validation| format_redcap_value(&value, validation));
					row.insert(field, normalize_value(value));
				}
			}
		}
	}

	let mut rows = rows_by_subject.into_values().collect::<Vec<_>>();
	for row in &mut rows {
		for value in row.values_mut() {
			*value = normalize_value(std::mem::take(value));
		}
		let visit_status = row.get("visit_status").cloned().unwrap_or_default();
		let removed = row.get("removed").cloned().unwrap_or_default();
		let converted = row.get("converted").cloned().unwrap_or_default();
		row.insert("visit_status_string".to_owned(), legacy_visit_status(&visit_status, &removed, &converted));
		row.insert("subjectid".to_owned(), row.get("subject_id").cloned().unwrap_or_default());
	}
	Ok(rows)
}

fn retain_exportable_rows(rows: &mut Vec<CsvRow>) {
	let mandatory = MANDATORY_COLUMNS.iter().copied().collect::<BTreeSet<_>>();
	rows.retain(|row| {
		row.iter()
			.any(|(column, value)| !mandatory.contains(column.as_str()) && !value.is_empty())
	});
}

fn write_csv(path: &Path, rows: &[CsvRow]) -> ExportResult<()> {
	let present = rows.iter().flat_map(|row| row.keys().cloned()).collect::<BTreeSet<_>>();
	let mut headers = MANDATORY_COLUMNS
		.iter()
		.filter(|column| present.contains(**column) && rows.iter().any(|row| row.get(**column).is_some_and(|value| !value.is_empty())))
		.map(|column| (*column).to_owned())
		.collect::<Vec<_>>();
	if !headers.iter().any(|column| column == "subject_id") {
		headers.insert(0, "subject_id".to_owned());
	}
	headers.extend(present.into_iter().filter(|column| !MANDATORY_COLUMNS.contains(&column.as_str())));

	let mut writer = csv::Writer::from_path(path)?;
	writer.write_record(&headers)?;
	for row in rows {
		writer.write_record(headers.iter().map(|header| row.get(header).map_or("", String::as_str)))?;
	}
	writer.flush()?;
	Ok(())
}

#[tokio::main]
async fn main() -> ExportResult<()> {
	let indicatif_layer = IndicatifLayer::new();
	tracing_subscriber::registry()
		.with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
		.with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
		.with(indicatif_layer)
		.init();

	let cli = Cli::parse();
	fs::create_dir_all(&cli.output_dir)?;
	let visits = if cli.visit.is_empty() { default_visits() } else { cli.visit };
	let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
	let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;
	let validations = date_validations(&pool).await?;
	let progress_style = ProgressStyle::default_bar()
		.template("{span_child_prefix}{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")?
		.progress_chars("#>-");
	let networks_span = tracing::info_span!("exporting_networks");
	networks_span.pb_set_style(&progress_style);
	networks_span.pb_set_length(cli.network.len() as u64);

	for network in &cli.network {
		let _network_enter = networks_span.enter();
		networks_span.pb_set_message(&format!("Exporting {network}"));
		let visits_span = tracing::info_span!("exporting_visits", network);
		visits_span.pb_set_style(&progress_style);
		visits_span.pb_set_length(visits.len() as u64);
		for visit in &visits {
			let _visit_enter = visits_span.enter();
			visits_span.pb_set_message(&format!("Exporting {network} {visit}"));
			let read_span = tracing::info_span!("reading_visit", network, visit);
			read_span.pb_set_style(&ProgressStyle::with_template("{span_child_prefix}{spinner:.green} {msg}")?);
			read_span.pb_set_message(&format!("Reading {network} {visit}"));
			let mut rows = read_visit(&pool, network, visit, &validations, &read_span).await?;
			retain_exportable_rows(&mut rows);
			let path = cli.output_dir.join(output_name(network, visit));
			info!(network, visit, rows = rows.len(), path = %path.display(), "Writing combined CSV");
			write_csv(&path, &rows)?;
			visits_span.pb_inc(1);
		}
		networks_span.pb_inc(1);
	}
	pool.close().await;
	info!(output_dir = %cli.output_dir.display(), "Combined CSV export complete");
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn builds_legacy_output_name() {
		assert_eq!(output_name("PRESCIENT", "baseline"), "AMPSCZ-combined-redcap_baseline_PRESCIENT-day1to1.csv");
	}

	#[test]
	fn applies_legacy_status_precedence() {
		assert_eq!(legacy_visit_status("baseline", "", ""), "baseln");
		assert_eq!(legacy_visit_status("month_12", "1", "1"), "removed");
		assert_eq!(legacy_visit_status("month_12", "", "True"), "converted");
	}

	#[test]
	fn formats_redcap_dates_and_times() {
		assert_eq!(format_redcap_value("2025-02-03T04:05:06", "date_ymd"), "2025-02-03");
		assert_eq!(format_redcap_value("2025-02-03T04:05:06", "datetime_ymd"), "2025-02-03 04:05");
		assert_eq!(format_redcap_value("1900-01-01T04:05:06", "time"), "04:05");
	}

	#[test]
	fn drops_rows_without_form_data() {
		let mut rows = vec![
			[("subject_id".to_owned(), "A".to_owned())].into_iter().collect(),
			[("subject_id".to_owned(), "B".to_owned()), ("score".to_owned(), "1".to_owned())].into_iter().collect(),
		];
		retain_exportable_rows(&mut rows);
		assert_eq!(rows.len(), 1);
		assert_eq!(rows[0].get("subject_id"), Some(&"B".to_owned()));
	}
}
