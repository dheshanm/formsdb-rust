pub mod constants;

use std::collections::BTreeSet;

use chrono::{NaiveDate, Utc};
use db::get_df;
use polars::prelude::DataFrame;
use serde_json::{Map, Number, Value};
use sqlx::PgPool;

/// Form completion record to be inserted into `forms.forms`.
#[derive(Debug, Clone, PartialEq)]
pub struct FormCompletionRecord {
    pub subject_id: String,
    pub form_name: String,
    pub event_name: String,
    pub form_data: Value,
    pub source_mdate: NaiveDate,
    pub variables_with_data: i32,
}

/// Map RPMS completion status code to REDCap status code.
///
/// RPMS status:
/// 0 - Red (No data entered) -> None (empty/skipped)
/// 1 - Orange (Data partially entered) -> Some(0)
/// 2..=4 - Green (All data entered) -> Some(2)
pub fn rpms_to_redcap_entry_status_map(rpms_status: &str) -> Option<i32> {
    let status = rpms_status.parse::<i32>().ok()?;
    match status {
        0 => None,
        1 => Some(0),
        2..=4 => Some(2),
        _ => None,
    }
}

/// Entry status item for calculating completion variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryStatusItem {
    pub redcap_event_name: Option<String>,
    pub redcap_form_name: Option<String>,
    pub completion_status: Option<String>,
}

/// Generates form completion records (`uncategorized` form rows) for a subject.
pub fn get_subject_form_completion_variables(
    subject_id: &str,
    cohort: &str,
    entry_status_items: &[EntryStatusItem],
) -> Result<Vec<FormCompletionRecord>, Box<dyn std::error::Error + Send + Sync>> {
    let arm = match cohort {
        "CHR" => 1,
        "HC" => 2,
        _ => return Err(format!("Invalid cohort: {cohort}").into()),
    };

    let visits = entry_status_items
        .iter()
        .filter_map(|item| item.redcap_event_name.as_deref())
        .collect::<BTreeSet<_>>();

    let today = Utc::now().date_naive();
    let mut completion_records = Vec::new();

    for visit in visits {
        let redcap_event_name = format!("{visit}_arm_{arm}");
        let mut visit_data = Map::new();

        for item in entry_status_items {
            if item.redcap_event_name.as_deref() != Some(visit) {
                continue;
            }

            let Some(redcap_form_name) = item.redcap_form_name.as_deref() else {
                continue;
            };

            let Some(rpms_status_str) = item.completion_status.as_deref() else {
                continue;
            };

            let Some(redcap_status) = rpms_to_redcap_entry_status_map(rpms_status_str) else {
                continue;
            };

            let Ok(rpms_status) = rpms_status_str.parse::<i64>() else {
                continue;
            };

            let redcap_variable = format!("{redcap_form_name}_complete");
            visit_data.insert(
                format!("{redcap_variable}_rpms"),
                Value::Number(Number::from(rpms_status)),
            );
            visit_data.insert(redcap_variable, Value::Number(Number::from(redcap_status)));
        }

        if !visit_data.is_empty() {
            let variables_with_data = visit_data.len() as i32;
            completion_records.push(FormCompletionRecord {
                subject_id: subject_id.to_owned(),
                form_name: "uncategorized".to_owned(),
                event_name: redcap_event_name,
                form_data: Value::Object(visit_data),
                source_mdate: today,
                variables_with_data,
            });
        }
    }

    Ok(completion_records)
}

pub async fn get_subject_cohort(
    subject_id: &str,
    pool: &PgPool,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let sql = format!(
        r#"
        SELECT event_name
        FROM forms.forms
        WHERE subject_id = '{subject_id}'
        LIMIT 1
        "#
    );

    let df = get_df(pool, &sql).await?;

    if df.height() == 0 {
        return Ok(None);
    }

    let event_name_series = df.column("event_name")?;
    let event_name_ca = event_name_series.str()?;
    let event_name = match event_name_ca.get(0) {
        Some(name) => name,
        None => return Ok(None),
    };

    if event_name.ends_with("_arm_1") {
        Ok(Some("CHR".to_string()))
    } else if event_name.ends_with("_arm_2") {
        Ok(Some("HC".to_string()))
    } else {
        Ok(None)
    }
}

/// Fetch the REDCap data dictionary from `forms.data_dictionary`.
///
/// The returned DataFrame contains `field_name` and `form_name`, which are
/// sufficient to associate imported REDCap JSON variables with forms.
pub async fn get_data_dictionary(pool: &PgPool) -> Result<DataFrame, Box<dyn std::error::Error>> {
    Ok(get_df(
        pool,
        "SELECT field_name, form_name FROM forms.data_dictionary",
    )
    .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_rpms_to_redcap_entry_status_map() {
        assert_eq!(rpms_to_redcap_entry_status_map("0"), None);
        assert_eq!(rpms_to_redcap_entry_status_map("1"), Some(0));
        assert_eq!(rpms_to_redcap_entry_status_map("2"), Some(2));
        assert_eq!(rpms_to_redcap_entry_status_map("3"), Some(2));
        assert_eq!(rpms_to_redcap_entry_status_map("4"), Some(2));
        assert_eq!(rpms_to_redcap_entry_status_map("5"), None);
        assert_eq!(rpms_to_redcap_entry_status_map("-1"), None);
        assert_eq!(rpms_to_redcap_entry_status_map("invalid"), None);
    }

    #[test]
    fn test_get_subject_form_completion_variables_chr() {
        let items = vec![
            EntryStatusItem {
                redcap_event_name: Some("screening".to_string()),
                redcap_form_name: Some("bprs".to_string()),
                completion_status: Some("2".to_string()),
            },
            EntryStatusItem {
                redcap_event_name: Some("screening".to_string()),
                redcap_form_name: Some("sofas_screening".to_string()),
                completion_status: Some("1".to_string()),
            },
            EntryStatusItem {
                redcap_event_name: Some("screening".to_string()),
                redcap_form_name: Some("pips".to_string()),
                completion_status: Some("0".to_string()),
            },
            EntryStatusItem {
                redcap_event_name: Some("baseline".to_string()),
                redcap_form_name: Some("cssrs_baseline".to_string()),
                completion_status: Some("3".to_string()),
            },
        ];

        let records = get_subject_form_completion_variables("AB001", "CHR", &items).unwrap();
        assert_eq!(records.len(), 2);

        let baseline = &records[0];
        assert_eq!(baseline.subject_id, "AB001");
        assert_eq!(baseline.form_name, "uncategorized");
        assert_eq!(baseline.event_name, "baseline_arm_1");
        assert_eq!(baseline.variables_with_data, 2);
        assert_eq!(
            baseline.form_data,
            serde_json::json!({
                "cssrs_baseline_complete_rpms": 3,
                "cssrs_baseline_complete": 2,
            })
        );

        let screening = &records[1];
        assert_eq!(screening.subject_id, "AB001");
        assert_eq!(screening.form_name, "uncategorized");
        assert_eq!(screening.event_name, "screening_arm_1");
        assert_eq!(screening.variables_with_data, 4);
        assert_eq!(
            screening.form_data,
            serde_json::json!({
                "bprs_complete_rpms": 2,
                "bprs_complete": 2,
                "sofas_screening_complete_rpms": 1,
                "sofas_screening_complete": 0,
            })
        );
    }

    #[test]
    fn test_get_subject_form_completion_variables_hc() {
        let items = vec![EntryStatusItem {
            redcap_event_name: Some("baseline".to_string()),
            redcap_form_name: Some("bprs".to_string()),
            completion_status: Some("2".to_string()),
        }];

        let records = get_subject_form_completion_variables("AB002", "HC", &items).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event_name, "baseline_arm_2");
        assert_eq!(
            records[0].form_data,
            serde_json::json!({
                "bprs_complete_rpms": 2,
                "bprs_complete": 2,
            })
        );
    }

    #[test]
    fn test_get_subject_form_completion_variables_invalid_cohort() {
        let items = vec![];
        let result = get_subject_form_completion_variables("AB001", "INVALID", &items);
        assert!(result.is_err());
    }

    fn get_db_uri() -> String {
        env::var("DB_URI").expect("DB_URI environment variable must be set")
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "db_tests"), ignore = "requires --features db_tests")]
    async fn test_get_subject_cohort() {
        let uri = get_db_uri();
        let pool = db::create_pool_with_options(&uri, 5)
            .await
            .expect("Failed to create connection pool");

        // Query a subject if any exists in forms.forms to test
        let df = get_df(
            &pool,
            "SELECT subject_id, event_name FROM forms.forms ORDER BY RANDOM() LIMIT 1",
        )
        .await
        .expect("Failed to query forms");

        if df.height() > 0 {
            let subject_id = df
                .column("subject_id")
                .unwrap()
                .str()
                .unwrap()
                .get(0)
                .unwrap();
            let event_name = df
                .column("event_name")
                .unwrap()
                .str()
                .unwrap()
                .get(0)
                .unwrap();
            let cohort = get_subject_cohort(subject_id, &pool)
                .await
                .expect("Failed to get subject cohort");

            if event_name.ends_with("_arm_1") {
                assert_eq!(cohort, Some("CHR".to_string()));
            } else if event_name.ends_with("_arm_2") {
                assert_eq!(cohort, Some("HC".to_string()));
            }
        }

        // Test non-existent subject
        let non_existent = get_subject_cohort("non_existent_subject_id_12345", &pool)
            .await
            .expect("Failed to query non-existent subject");
        assert_eq!(non_existent, None);

        pool.close().await;
    }
}
