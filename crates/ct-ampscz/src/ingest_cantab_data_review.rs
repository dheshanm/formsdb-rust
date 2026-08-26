use std::{
    collections::BTreeMap,
    error::Error,
    path::{Path, PathBuf},
};

use clap::Parser;
use serde_json::Value;
use sqlx::{PgPool, types::Json};
use tracing::info;

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Import CANTAB Data Review Form (CSV) into `cantab.data_review`.
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Path to the CANTAB Data Review Form CSV.
    #[arg(short = 'd', long)]
    cantab_data_review_form_path: PathBuf,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long, default_value_t = 1)]
    max_connections: u32,
}

const INIT_QUERIES: &[&str] = &[
    r#"DROP TABLE IF EXISTS cantab.data_review;"#,
    r#"CREATE TABLE cantab.data_review (
        visit_id TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        visit_name TEXT NOT NULL,
        reviewer_name TEXT NOT NULL,
        review_details JSONB NOT NULL,
        query_sent_to_site TEXT NOT NULL,
        response_from_site TEXT NOT NULL,
        recommended_action TEXT NOT NULL,
        review_rationale TEXT NOT NULL,
        FOREIGN KEY (visit_id) REFERENCES cantab.visits (id),
        PRIMARY KEY (visit_id)
    );"#,
];

#[derive(Debug)]
struct DataReviewRow {
    subject_id: String,
    visit_name: String,
    reviewer_name: String,
    details_of_review: String,
    query_sent_to_site: String,
    response_from_site: String,
    recommended_action: String,
    review_rationale: String,
}

fn normalize_details_key(key: &str) -> String {
    let mut normalized = String::new();
    let mut underscore_pending = false;

    for character in key.chars() {
        if character.is_ascii_alphanumeric() {
            if underscore_pending && !normalized.is_empty() {
                normalized.push('_');
            }
            underscore_pending = false;
            normalized.push(character.to_ascii_lowercase());
        } else {
            underscore_pending = true;
        }
    }

    normalized
}

fn split_review_list(value: &str, delimiters: &[char]) -> Vec<String> {
    value
        .split(|character| delimiters.contains(&character))
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

fn review_json_value(key: &str, value: String) -> Value {
    match key {
        "test" => {
            let values = split_review_list(&value, &[',', ';']);
            if values.is_empty() {
                Value::String(value)
            } else {
                Value::Array(values.into_iter().map(Value::String).collect())
            }
        }
        "observation" => {
            let values = split_review_list(&value, &[',', ';']);
            if values.is_empty() {
                Value::String(value)
            } else {
                Value::Array(values.into_iter().map(Value::String).collect())
            }
        }
        _ => Value::String(value),
    }
}

fn parse_details_of_review(details: &str) -> Value {
    let details = details.trim();
    if details.is_empty() {
        return Value::Object(serde_json::Map::new());
    }

    let mut parsed = BTreeMap::<String, String>::new();
    let mut current_key: Option<String> = None;

    for raw_line in details.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some((raw_key, raw_value)) = line.split_once(':') {
            let key = normalize_details_key(raw_key);
            if key.is_empty() {
                continue;
            }

            let value = raw_value.trim();
            if !value.is_empty() {
                parsed
                    .entry(key.clone())
                    .and_modify(|existing| {
                        if !existing.is_empty() {
                            existing.push('\n');
                        }
                        existing.push_str(value);
                    })
                    .or_insert_with(|| value.to_owned());
            } else {
                parsed.entry(key.clone()).or_default();
            }

            current_key = Some(key);
            continue;
        }

        if let Some(key) = &current_key {
            let existing = parsed.entry(key.clone()).or_default();
            if !existing.is_empty() {
                existing.push('\n');
            }
            existing.push_str(line);
        }
    }

    if parsed.is_empty() {
        let mut fallback = serde_json::Map::new();
        fallback.insert("text".to_owned(), Value::String(details.to_owned()));
        return Value::Object(fallback);
    }

    let mut object = serde_json::Map::new();
    for (key, value) in parsed {
        object.insert(key.clone(), review_json_value(&key, value));
    }
    Value::Object(object)
}

fn header_index(headers: &csv::StringRecord, column_name: &str) -> ImportResult<usize> {
    headers
        .iter()
        .position(|header| header.trim().eq_ignore_ascii_case(column_name))
        .ok_or_else(|| format!("CANTAB Data Review CSV missing required column: {column_name}").into())
}

fn csv_optional(record: &csv::StringRecord, index: usize) -> String {
    record.get(index).map(str::trim).unwrap_or_default().to_owned()
}

fn csv_required(
    record: &csv::StringRecord,
    index: usize,
    column_name: &str,
    row_number: usize,
) -> ImportResult<String> {
    let value = csv_optional(record, index);
    if value.is_empty() {
        Err(format!("row {row_number}: missing required value in column '{column_name}'").into())
    } else {
        Ok(value)
    }
}

fn read_data_review_rows(path: &Path) -> ImportResult<Vec<DataReviewRow>> {
    let mut reader = csv::ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = reader.headers()?.clone();

    let subject_id_index = header_index(&headers, "Subject ID")?;
    let visit_name_index = header_index(&headers, "Visit Name")?;
    let reviewer_name_index = header_index(&headers, "Reviewer Name")?;
    let details_of_review_index = header_index(&headers, "Details of Review")?;
    let query_sent_to_site_index = header_index(&headers, "Query Sent to Site")?;
    let response_from_site_index = header_index(&headers, "Response from Site")?;
    let recommended_action_index = header_index(&headers, "Recommended Action")?;
    let review_rationale_index = header_index(&headers, "Review Rationale")?;

    let mut rows = Vec::new();
    for (row_offset, record) in reader.records().enumerate() {
        let record = record?;
        let row_number = row_offset + 2;

        rows.push(DataReviewRow {
            subject_id: csv_required(&record, subject_id_index, "Subject ID", row_number)?,
            visit_name: csv_required(&record, visit_name_index, "Visit Name", row_number)?,
            reviewer_name: csv_required(
                &record,
                reviewer_name_index,
                "Reviewer Name",
                row_number,
            )?,
            details_of_review: csv_optional(&record, details_of_review_index),
            query_sent_to_site: csv_optional(&record, query_sent_to_site_index),
            response_from_site: csv_optional(&record, response_from_site_index),
            recommended_action: csv_optional(&record, recommended_action_index),
            review_rationale: csv_optional(&record, review_rationale_index),
        });
    }

    if rows.is_empty() {
        return Err("CANTAB Data Review CSV contains no data rows".into());
    }

    Ok(rows)
}

async fn init_db(pool: &PgPool) -> ImportResult<()> {
    info!("Initializing database with CANTAB Data Review table");

    let queries: Vec<String> = INIT_QUERIES.iter().map(|&q| q.to_string()).collect();
    db::execute_queries_in_transaction(&pool, &queries).await?;

    info!("Database initialized with CANTAB Data Review table");
    Ok(())
}

async fn get_visit_id(
    pool: &PgPool,
    subject_id: &str,
    visit_name: &str,
) -> ImportResult<String> {
    let query = r#"
SELECT v.id::text
FROM cantab.visits v 
INNER JOIN cantab.visit_defs vd ON vd.id = v.visit_def 
WHERE v.subject_id = $1 AND vd."name" = $2
    "#;

    let visit_id = sqlx::query_scalar::<_, String>(query)
        .bind(subject_id)
        .bind(visit_name)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| {
            format!(
                "no cantab visit found for subject_id '{subject_id}' and visit_name '{visit_name}'"
            )
        })?;

    Ok(visit_id)
}

async fn insert_data_review_row(pool: &PgPool, row: &DataReviewRow) -> ImportResult<()> {
    let visit_id = get_visit_id(pool, &row.subject_id, &row.visit_name).await?;
    let review_details = parse_details_of_review(&row.details_of_review);

    sqlx::query(
        r#"
INSERT INTO cantab.data_review (
    visit_id,
    subject_id,
    visit_name,
    reviewer_name,
    review_details,
    query_sent_to_site,
    response_from_site,
    recommended_action,
    review_rationale
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(visit_id)
    .bind(&row.subject_id)
    .bind(&row.visit_name)
    .bind(&row.reviewer_name)
    .bind(Json(review_details))
    .bind(&row.query_sent_to_site)
    .bind(&row.response_from_site)
    .bind(&row.recommended_action)
    .bind(&row.review_rationale)
    .execute(pool)
    .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> ImportResult<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    info!(
        path = %cli.cantab_data_review_form_path.display(),
        "Reading CANTAB Data Review Form CSV"
    );
    let rows = read_data_review_rows(&cli.cantab_data_review_form_path)?;

    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;

    init_db(&pool).await?;
    for row in &rows {
        insert_data_review_row(&pool, row).await?;
    }

    info!(rows = rows.len(), "Imported CANTAB Data Review rows");

    pool.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_structured_details_to_expected_keys() {
        let parsed = parse_details_of_review(
            "Test: SWM, RVP, LIT\nMeasure: LITCOAN + LITCOAP == 0\nObservation: DISTRACTION;DID_NOT_UNDERSTAND_INSTRUCTION\nComment: N/A",
        );

        assert_eq!(parsed["test"], json!(["SWM", "RVP", "LIT"]));
        assert_eq!(parsed["measure"], json!("LITCOAN + LITCOAP == 0"));
        assert_eq!(
            parsed["observation"],
            json!(["DISTRACTION", "DID_NOT_UNDERSTAND_INSTRUCTION"])
        );
        assert_eq!(parsed["comment"], json!("N/A"));
    }

    #[test]
    fn preserves_continued_lines_for_same_key() {
        let parsed = parse_details_of_review(
            "Measure: line one\nline two\nObservation: DISTRACTION;OTHER",
        );

        assert_eq!(parsed["measure"], json!("line one\nline two"));
        assert_eq!(parsed["observation"], json!(["DISTRACTION", "OTHER"]));
    }

    #[test]
    fn falls_back_to_text_for_unstructured_details() {
        let parsed = parse_details_of_review("No structured content present");
        assert_eq!(parsed["text"], json!("No structured content present"));
    }
}