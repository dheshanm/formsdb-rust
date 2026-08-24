use std::{
	collections::{BTreeMap, BTreeSet},
	error::Error,
	fs,
	path::{Path, PathBuf},
};

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime};
use clap::Parser;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

type ExportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Export date-shifted versions of combined CSVs produced by `export_combined_csv`.
///
/// The exporter matches rows by `subject_id` and shifts every parseable date/datetime
/// value in that subject row by the configured number of days.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
	/// Directory containing combined CSVs from `export_combined_csv`.
	#[arg(short, long)]
	input_dir: PathBuf,

	/// Output directory for date-shifted CSVs.
	#[arg(short, long)]
	output_dir: PathBuf,

	/// One or more date offset sources.
	///
	/// Pass one or more paths after `--date-offset`.
	/// Each path can be a file or a directory. Directories are searched
	/// recursively for `*date_offset*.csv`.
	///
	/// Network-specific files are detected from the source path when a path
	/// segment includes names like `Pronet` or `Prescient`.
	#[arg(long = "date-offset", required = true, num_args = 1..)]
	date_offset: Vec<String>,

	/// Optional network filter (for example: ProNET PRESCIENT).
	#[arg(short, long, num_args = 1..)]
	network: Vec<String>,
}

#[derive(Debug, Clone)]
struct OffsetSourceSpec {
	network: Option<String>,
	path: PathBuf,
}

#[derive(Debug, Default)]
struct OffsetRegistry {
	global: BTreeMap<String, i64>,
	by_network: BTreeMap<String, BTreeMap<String, i64>>,
}

#[derive(Debug, Default)]
struct FileShiftStats {
	rows_read: u64,
	rows_with_subject_offset: u64,
	shifted_cells: u64,
}

fn normalize_network(name: &str) -> String {
	name.trim().to_ascii_uppercase()
}

fn network_from_text(value: &str) -> Option<String> {
	let value = normalize_network(value);
	if value.contains("PRONET") {
		return Some("PRONET".to_owned());
	}
	if value.contains("PRESCIENT") {
		return Some("PRESCIENT".to_owned());
	}
	None
}

fn infer_network_from_path(path: &Path) -> Option<String> {
	for segment in path.iter() {
		let Some(segment) = segment.to_str() else {
			continue;
		};
		if let Some(network) = network_from_text(segment) {
			return Some(network);
		}
	}
	None
}

fn parse_offset_source_spec(raw: &str) -> OffsetSourceSpec {
	if let Some((network, path)) = raw.split_once('=') {
		return OffsetSourceSpec {
			network: Some(normalize_network(network)),
			path: PathBuf::from(path),
		};
	}

	let path = PathBuf::from(raw);

	OffsetSourceSpec {
		network: infer_network_from_path(&path),
		path,
	}
}

fn collect_offset_files(path: &Path) -> ExportResult<Vec<PathBuf>> {
	if path.is_file() {
		return Ok(vec![path.to_path_buf()]);
	}

	if !path.is_dir() {
		return Err(format!("Offset source does not exist: {}", path.display()).into());
	}

	let mut files = Vec::new();
	let mut stack = vec![path.to_path_buf()];
	while let Some(dir) = stack.pop() {
		for entry in fs::read_dir(&dir)? {
			let entry = entry?;
			let entry_path = entry.path();
			if entry_path.is_dir() {
				stack.push(entry_path);
				continue;
			}

			let Some(file_name) = entry_path.file_name().and_then(|name| name.to_str()) else {
				continue;
			};
			let file_name = file_name.to_ascii_lowercase();
			if file_name.ends_with(".csv") && file_name.contains("date_offset") {
				files.push(entry_path);
			}
		}
	}

	files.sort();
	if files.is_empty() {
		return Err(
			format!("No date offset CSV files found under directory: {}", path.display()).into(),
		);
	}
	Ok(files)
}

fn parse_days(value: &str) -> Option<i64> {
	let value = value.trim();
	if value.is_empty() {
		return None;
	}
	if let Ok(days) = value.parse::<i64>() {
		return Some(days);
	}
	let days_float = value.parse::<f64>().ok()?;
	if (days_float.fract()).abs() < f64::EPSILON {
		Some(days_float as i64)
	} else {
		None
	}
}

fn parse_subject_offsets(path: &Path) -> ExportResult<BTreeMap<String, i64>> {
	let mut reader = csv::ReaderBuilder::new().flexible(true).from_path(path)?;
	let headers = reader.headers()?.clone();

	let subject_idx = headers
		.iter()
		.position(|column| {
			column.eq_ignore_ascii_case("subject") || column.eq_ignore_ascii_case("subject_id")
		})
		.ok_or_else(|| {
			format!(
				"Missing required subject column (subject or subject_id) in {}",
				path.display()
			)
		})?;

	let days_idx = headers
		.iter()
		.position(|column| {
			column.eq_ignore_ascii_case("days")
				|| column.eq_ignore_ascii_case("day_offset")
				|| column.eq_ignore_ascii_case("offset_days")
		})
		.ok_or_else(|| {
			format!(
				"Missing required days column (days/day_offset/offset_days) in {}",
				path.display()
			)
		})?;

	let mut offsets = BTreeMap::new();
	for record in reader.records() {
		let record = record?;
		let subject = record.get(subject_idx).unwrap_or_default().trim();
		if subject.is_empty() {
			continue;
		}
		let days_raw = record.get(days_idx).unwrap_or_default().trim();
		let Some(days) = parse_days(days_raw) else {
			warn!(
				path = %path.display(),
				subject,
				days = days_raw,
				"Skipping subject with invalid day offset"
			);
			continue;
		};
		offsets.entry(subject.to_owned()).or_insert(days);
	}

	Ok(offsets)
}

fn merge_offsets(
	target: &mut BTreeMap<String, i64>,
	source: BTreeMap<String, i64>,
	source_path: &Path,
	network: Option<&str>,
) {
	for (subject, days) in source {
		match target.get(&subject) {
			Some(existing) if *existing != days => {
				warn!(
					subject,
					existing_days = *existing,
					incoming_days = days,
					source = %source_path.display(),
					network = network.unwrap_or("*"),
					"Conflicting subject offsets; keeping first value"
				);
			}
			Some(_) => {}
			None => {
				target.insert(subject, days);
			}
		}
	}
}

fn load_offsets(specs: &[String]) -> ExportResult<OffsetRegistry> {
	let mut registry = OffsetRegistry::default();

	for raw_spec in specs {
		let spec = parse_offset_source_spec(raw_spec);
		let files = collect_offset_files(&spec.path)?;

		for file in files {
			let offsets = parse_subject_offsets(&file)?;
			if let Some(network) = spec.network.as_deref() {
				let bucket = registry.by_network.entry(network.to_owned()).or_default();
				merge_offsets(bucket, offsets, &file, Some(network));
			} else {
				merge_offsets(&mut registry.global, offsets, &file, None);
			}
		}
	}

	Ok(registry)
}

fn list_combined_csvs(input_dir: &Path) -> ExportResult<Vec<PathBuf>> {
	let mut files = Vec::new();
	for entry in fs::read_dir(input_dir)? {
		let entry = entry?;
		let path = entry.path();
		if !path.is_file() {
			continue;
		}

		let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
			continue;
		};
		if !file_name.ends_with(".csv") {
			continue;
		}
		if !file_name.starts_with("AMPSCZ") {
			continue;
		}
		if file_name.contains("dateShifted") {
			continue;
		}
		files.push(path);
	}

	files.sort();
	Ok(files)
}

fn network_from_combined_name(path: &Path) -> Option<String> {
	let file_name = path.file_name()?.to_str()?;
	let stem = file_name.strip_suffix(".csv").unwrap_or(file_name);
	let stem = stem
		.strip_suffix("_dateShifted-day1to1")
		.unwrap_or(stem);
	let stem = stem.strip_suffix("-dateShifted").unwrap_or(stem);
	let stem = stem.strip_suffix("-day1to1").unwrap_or(stem);
	let (_, network) = stem.rsplit_once('_')?;
	Some(network.to_owned())
}

fn shifted_output_name(input_file: &Path) -> ExportResult<String> {
	let file_name = input_file
		.file_name()
		.and_then(|name| name.to_str())
		.ok_or_else(|| format!("Invalid UTF-8 file name: {}", input_file.display()))?;

	if let Some(stem) = file_name.strip_suffix(".csv") {
		if stem.ends_with("_dateShifted-day1to1") {
			return Ok(file_name.to_owned());
		}
		if let Some(prefix) = stem.strip_suffix("-day1to1") {
			if prefix.ends_with("_dateShifted") {
				return Ok(file_name.to_owned());
			}
			return Ok(format!("{prefix}_dateShifted-day1to1.csv"));
		}
		if stem.ends_with("-dateShifted") {
			return Ok(file_name.to_owned());
		}
		return Ok(format!("{stem}-dateShifted.csv"));
	}

	Ok(format!("{file_name}-dateShifted.csv"))
}

fn shift_date(value: &str, days: i64) -> Option<String> {
	const DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"];
	const DATETIME_FORMATS: &[&str] = &[
		"%Y-%m-%d %H:%M:%S%.f",
		"%Y-%m-%dT%H:%M:%S%.f",
		"%Y-%m-%d %H:%M:%S",
		"%Y-%m-%dT%H:%M:%S",
		"%Y-%m-%d %H:%M",
		"%Y-%m-%dT%H:%M",
		"%m/%d/%Y %H:%M:%S",
		"%m/%d/%Y %H:%M",
		"%m/%d/%Y %I:%M:%S %p",
	];

	let value = value.trim();
	if value.is_empty() || value == "-3" || value == "-9" {
		return None;
	}

	let delta = Duration::days(days);

	for format in DATE_FORMATS {
		if let Ok(date) = NaiveDate::parse_from_str(value, format) {
			return Some((date + delta).format(format).to_string());
		}
	}

	for format in DATETIME_FORMATS {
		if let Ok(datetime) = NaiveDateTime::parse_from_str(value, format) {
			return Some((datetime + delta).format(format).to_string());
		}
	}

	if let Ok(datetime) = DateTime::parse_from_rfc3339(value) {
		return Some((datetime + delta).to_rfc3339());
	}

	None
}

fn process_file(
	input_file: &Path,
	output_file: &Path,
	network_offsets: Option<&BTreeMap<String, i64>>,
	global_offsets: &BTreeMap<String, i64>,
) -> ExportResult<FileShiftStats> {
	let mut reader = csv::ReaderBuilder::new().flexible(true).from_path(input_file)?;
	let headers = reader.headers()?.clone();
	let subject_idx = headers
		.iter()
		.position(|column| {
			column.eq_ignore_ascii_case("subject_id") || column.eq_ignore_ascii_case("subject")
		})
		.ok_or_else(|| {
			format!(
				"CSV missing required subject column (subject_id or subject): {}",
				input_file.display()
			)
		})?;

	let mut writer = csv::WriterBuilder::new().from_path(output_file)?;
	writer.write_record(&headers)?;

	let mut stats = FileShiftStats::default();

	for record in reader.records() {
		let record = record?;
		stats.rows_read += 1;

		let subject = record.get(subject_idx).unwrap_or_default();
		let day_offset = network_offsets
			.and_then(|offsets| offsets.get(subject))
			.or_else(|| global_offsets.get(subject))
			.copied();

		if let Some(days) = day_offset {
			stats.rows_with_subject_offset += 1;
			let mut shifted_record = csv::StringRecord::new();
			for value in &record {
				if let Some(shifted) = shift_date(value, days) {
					if shifted != value {
						stats.shifted_cells += 1;
					}
					shifted_record.push_field(&shifted);
				} else {
					shifted_record.push_field(value);
				}
			}
			writer.write_record(&shifted_record)?;
		} else {
			writer.write_record(&record)?;
		}
	}

	writer.flush()?;
	Ok(stats)
}

fn should_process_network(
	network: Option<&str>,
	selected_networks: &BTreeSet<String>,
) -> bool {
	if selected_networks.is_empty() {
		return true;
	}

	let Some(network) = network else {
		return false;
	};

	selected_networks.contains(&normalize_network(network))
}

fn init_tracing() {
	let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
	tracing_subscriber::fmt()
		.with_env_filter(filter)
		.with_span_events(FmtSpan::CLOSE)
		.init();
}

fn run(cli: Cli) -> ExportResult<()> {
	if !cli.input_dir.exists() {
		return Err(format!("Input directory does not exist: {}", cli.input_dir.display()).into());
	}

	fs::create_dir_all(&cli.output_dir)?;

	let selected_networks = cli
		.network
		.iter()
		.map(|network| normalize_network(network))
		.collect::<BTreeSet<_>>();

	let offset_registry = load_offsets(&cli.date_offset)?;
	info!(
		global_subject_offsets = offset_registry.global.len(),
		network_offset_groups = offset_registry.by_network.len(),
		"Loaded date offset definitions"
	);

	let input_files = list_combined_csvs(&cli.input_dir)?;
	if input_files.is_empty() {
		warn!(input_dir = %cli.input_dir.display(), "No combined CSV files found");
		return Ok(());
	}

	let mut processed_files = 0_u64;
	let mut skipped_files = 0_u64;
	let mut total_rows = 0_u64;
	let mut total_offset_rows = 0_u64;
	let mut total_shifted_cells = 0_u64;

	for input_file in input_files {
		let network = network_from_combined_name(&input_file);
		if !should_process_network(network.as_deref(), &selected_networks) {
			skipped_files += 1;
			info!(
				file = %input_file.display(),
				network = network.as_deref().unwrap_or("unknown"),
				"Skipping file due to network filter"
			);
			continue;
		}

		let network_offsets = network
			.as_deref()
			.map(normalize_network)
			.and_then(|name| offset_registry.by_network.get(&name));

		if network_offsets.is_none() && offset_registry.global.is_empty() {
			skipped_files += 1;
			warn!(
				file = %input_file.display(),
				network = network.as_deref().unwrap_or("unknown"),
				"No matching offsets found (no network-specific offsets and no global offsets); skipping file"
			);
			continue;
		}

		let output_name = shifted_output_name(&input_file)?;
		let output_file = cli.output_dir.join(output_name);

		let stats = process_file(
			&input_file,
			&output_file,
			network_offsets,
			&offset_registry.global,
		)?;

		info!(
			input_file = %input_file.display(),
			output_file = %output_file.display(),
			network = network.as_deref().unwrap_or("unknown"),
			rows_read = stats.rows_read,
			rows_with_subject_offset = stats.rows_with_subject_offset,
			shifted_cells = stats.shifted_cells,
			"Wrote date-shifted CSV"
		);

		processed_files += 1;
		total_rows += stats.rows_read;
		total_offset_rows += stats.rows_with_subject_offset;
		total_shifted_cells += stats.shifted_cells;
	}

	info!(
		processed_files,
		skipped_files,
		total_rows,
		total_rows_with_subject_offset = total_offset_rows,
		total_shifted_cells,
		output_dir = %cli.output_dir.display(),
		"Date offset export complete"
	);

	Ok(())
}

fn main() -> ExportResult<()> {
	init_tracing();
	run(Cli::parse())
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::{fs::File, io::Write};

	#[test]
	fn infers_network_from_offset_path() {
		let spec = parse_offset_source_spec("/data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/date_offset.csv");
		assert_eq!(spec.network.as_deref(), Some("PRONET"));
	}

	#[test]
	fn parses_legacy_network_and_path_offset_spec() {
		let spec = parse_offset_source_spec("PRESCIENT=/tmp/date_offset.csv");
		assert_eq!(spec.network.as_deref(), Some("PRESCIENT"));
		assert_eq!(spec.path, PathBuf::from("/tmp/date_offset.csv"));
	}

	#[test]
	fn parses_global_offset_spec() {
		let spec = parse_offset_source_spec("/tmp/date_offset.csv");
		assert_eq!(spec.network, None);
		assert_eq!(spec.path, PathBuf::from("/tmp/date_offset.csv"));
	}

	#[test]
	fn extracts_network_from_combined_file_name() {
		let path = PathBuf::from("AMPSCZ-combined-redcap_month_2_PRESCIENT-day1to1.csv");
		assert_eq!(network_from_combined_name(&path).as_deref(), Some("PRESCIENT"));
	}

	#[test]
	fn builds_shifted_output_name() {
		let input = PathBuf::from("AMPSCZ-combined-redcap_baseline_ProNET-day1to1.csv");
		assert_eq!(
			shifted_output_name(&input).expect("valid output name"),
			"AMPSCZ-combined-redcap_baseline_ProNET_dateShifted-day1to1.csv"
		);
	}

	#[test]
	fn shifts_dates_and_datetimes() {
		assert_eq!(shift_date("2025-01-01", 7).as_deref(), Some("2025-01-08"));
		assert_eq!(
			shift_date("2025-01-01 13:45", -2).as_deref(),
			Some("2024-12-30 13:45")
		);
		assert_eq!(shift_date("-3", 7), None);
		assert_eq!(shift_date("", 7), None);
	}

	#[test]
	fn processes_csv_with_subject_offsets() -> ExportResult<()> {
		let tmp_dir = std::env::temp_dir().join(format!(
			"formsdb-rust-export-date-offset-{}-{}",
			std::process::id(),
			chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
		));
		fs::create_dir_all(&tmp_dir)?;

		let input = tmp_dir.join("AMPSCZ-combined-redcap_baseline_PRESCIENT-day1to1.csv");
		let output = tmp_dir.join("out.csv");

		let mut input_file = File::create(&input)?;
		writeln!(input_file, "subject_id,visit_date,score")?;
		writeln!(input_file, "AB001,2025-01-01,10")?;
		writeln!(input_file, "AB002,-3,8")?;
		writeln!(input_file, "AB003,2025-01-03,7")?;

		let mut global_offsets = BTreeMap::new();
		global_offsets.insert("AB001".to_owned(), 7);
		global_offsets.insert("AB002".to_owned(), 7);

		let stats = process_file(&input, &output, None, &global_offsets)?;
		assert_eq!(stats.rows_read, 3);
		assert_eq!(stats.rows_with_subject_offset, 2);
		assert_eq!(stats.shifted_cells, 1);

		let output_contents = fs::read_to_string(&output)?;
		assert!(output_contents.contains("AB001,2025-01-08,10"));
		assert!(output_contents.contains("AB002,-3,8"));
		assert!(output_contents.contains("AB003,2025-01-03,7"));

		let _ = fs::remove_file(&input);
		let _ = fs::remove_file(&output);
		let _ = fs::remove_dir_all(&tmp_dir);
		Ok(())
	}
}
