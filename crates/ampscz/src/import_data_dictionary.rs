use std::{
    error::Error,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use clap::Parser;
use polars::prelude::{Column, DataFrame};
use tracing::info;

type ImportResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Import a REDCap data dictionary CSV into `forms.data_dictionary`.
/// Required environment variable: DB_URI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Path to the REDCap data dictionary CSV.
    #[arg(short = 'd', long)]
    data_dictionary_path: PathBuf,

    /// Maximum PostgreSQL connections used by this import.
    #[arg(long, default_value_t = 1)]
    max_connections: u32,
}

/// Remove HTML tags using the same `<[^>]*>` semantics as the Python importer.
fn remove_html_tags(value: &str) -> String {
    let mut cleaned = String::with_capacity(value.len());
    let mut remaining = value;

    while let Some(tag_start) = remaining.find('<') {
        cleaned.push_str(&remaining[..tag_start]);
        let after_tag_start = &remaining[tag_start..];

        let Some(tag_end) = after_tag_start.find('>') else {
            cleaned.push_str(after_tag_start);
            return cleaned;
        };

        remaining = &after_tag_start[tag_end + 1..];
    }

    cleaned.push_str(remaining);
    cleaned
}

fn data_dictionary_dataframe<R: Read>(reader: R) -> ImportResult<DataFrame> {
    let mut reader = csv::ReaderBuilder::new().flexible(true).from_reader(reader);
    let headers = reader.headers()?.clone();

    if headers.is_empty() {
        return Err("data dictionary CSV contains no column headers".into());
    }
    if headers.iter().any(str::is_empty) {
        return Err("data dictionary CSV contains an empty column header".into());
    }

    let header_names = headers.iter().map(str::to_owned).collect::<Vec<_>>();
    let mut values = vec![Vec::new(); header_names.len()];

    for record in reader.records() {
        let record = record?;
        for (index, column) in values.iter_mut().enumerate() {
            column.push(remove_html_tags(record.get(index).unwrap_or_default()));
        }
    }

    let row_count = values.first().map_or(0, Vec::len);
    let columns = header_names
        .into_iter()
        .zip(values)
        .map(|(name, values)| Column::new(name.into(), values))
        .collect::<Vec<_>>();

    Ok(DataFrame::new(row_count, columns)?)
}

fn read_data_dictionary(path: &Path) -> ImportResult<DataFrame> {
    let file = File::open(path)?;
    data_dictionary_dataframe(file)
}

#[tokio::main]
async fn main() -> ImportResult<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    info!(
        path = %cli.data_dictionary_path.display(),
        "Reading data dictionary CSV"
    );
    let dataframe = read_data_dictionary(&cli.data_dictionary_path)?;

    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, cli.max_connections.max(1)).await?;

    info!("Writing to DB - forms.data_dictionary");
    db::df_to_table(
        &pool,
        &dataframe,
        "forms",
        "data_dictionary",
        db::IfExists::Replace,
    )
    .await?;
    info!("Imported {} data dictionary rows", dataframe.height());

    pool.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn removes_html_tags_without_other_normalization() {
        assert_eq!(
            remove_html_tags("Before <strong>bold</strong> and <em>italic</em> after"),
            "Before bold and italic after"
        );
        assert_eq!(remove_html_tags("a &amp; b"), "a &amp; b");
        assert_eq!(remove_html_tags("unclosed <tag"), "unclosed <tag");
    }

    #[test]
    fn cleans_every_column_and_preserves_blank_values() {
        let dataframe = data_dictionary_dataframe(Cursor::new(
            "field_name,field_label,choices\nfield_one,<p>First <b>label</b></p>,\"1, <i>Yes</i> | 0, No\"\nfield_two,,<span>value</span>\n",
        ))
        .unwrap();

        assert_eq!(dataframe.height(), 2);
        assert_eq!(
            dataframe
                .column("field_label")
                .unwrap()
                .str()
                .unwrap()
                .get(0),
            Some("First label")
        );
        assert_eq!(
            dataframe
                .column("field_label")
                .unwrap()
                .str()
                .unwrap()
                .get(1),
            Some("")
        );
        assert_eq!(
            dataframe.column("choices").unwrap().str().unwrap().get(0),
            Some("1, Yes | 0, No")
        );
        assert_eq!(
            dataframe.column("choices").unwrap().str().unwrap().get(1),
            Some("value")
        );
    }

    #[test]
    fn rejects_csv_without_column_headers() {
        let error = data_dictionary_dataframe(Cursor::new("")).unwrap_err();
        assert!(error.to_string().contains("no column headers"));
    }

    #[test]
    fn rejects_empty_column_header() {
        let error =
            data_dictionary_dataframe(Cursor::new("field_name,\nexample,value\n")).unwrap_err();
        assert!(error.to_string().contains("empty column header"));
    }
}
