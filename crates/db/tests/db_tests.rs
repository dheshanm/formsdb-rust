use std::env;

use db::{
    create_pool_with_options, df_to_table, execute_queries_in_transaction, get_df, IfExists,
};
use polars::prelude::*;
use sqlx::AssertSqlSafe;

fn get_db_uri() -> String {
    env::var("DB_URI").expect("DB_URI environment variable must be set")
}

#[tokio::test]
#[cfg_attr(not(feature = "db_tests"), ignore = "requires --features db_tests")]
async fn test_connect_pool() {
    let uri = get_db_uri();

    let pool = create_pool_with_options(&uri, 5)
        .await
        .expect("Failed to create connection pool");

    assert_eq!(pool.options().get_max_connections(), 5);
    pool.close().await;
}

#[tokio::test]
#[cfg_attr(not(feature = "db_tests"), ignore = "requires --features db_tests")]
async fn test_execute_queries_in_transaction() {
    let uri = get_db_uri();

    let pool = create_pool_with_options(&uri, 5)
        .await
        .expect("Failed to create connection pool");

    let queries = vec!["SELECT 1".to_string(), "SELECT 2 + 2".to_string()];

    execute_queries_in_transaction(&pool, &queries)
        .await
        .expect("Failed to execute queries in transaction");

    pool.close().await;
}

#[tokio::test]
#[cfg_attr(not(feature = "db_tests"), ignore = "requires --features db_tests")]
async fn test_get_df() {
    let uri = get_db_uri();

    let pool = create_pool_with_options(&uri, 5)
        .await
        .expect("Failed to create connection pool");

    // Test non-empty query with multiple columns and data types
    let sql = "SELECT 1 AS id, 'test' AS name, '{\"key\": \"val\"}'::jsonb AS data";
    let df = get_df(&pool, sql).await.expect("Failed to execute get_df");

    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 3);
    assert!(df.column("id").is_ok());
    assert!(df.column("name").is_ok());
    assert!(df.column("data").is_ok());

    // Test empty result query
    let empty_sql = "SELECT 1 AS id WHERE false";
    let empty_df = get_df(&pool, empty_sql)
        .await
        .expect("Failed to execute get_df for empty result");

    assert_eq!(empty_df.height(), 0);

    pool.close().await;
}

#[tokio::test]
#[cfg_attr(not(feature = "db_tests"), ignore = "requires --features db_tests")]
async fn test_df_to_table() {
    let uri = get_db_uri();

    let pool = create_pool_with_options(&uri, 5)
        .await
        .expect("Failed to create connection pool");

    let schema = "test_schema_df_to_table";
    let table = "test_table";

    // Construct a sample DataFrame
    let s_id = Column::new("id".into(), &[1i32, 2i32, 3i32]);
    let s_name = Column::new("name".into(), &["alice", "bob", "charlie"]);
    let s_score = Column::new("score".into(), &[95.5f64, 82.0f64, 99.1f64]);
    let df = DataFrame::new(3, vec![s_id, s_name, s_score]).expect("Failed to create DataFrame");

    // 1. Test Replace mode
    df_to_table(&pool, &df, schema, table, IfExists::Replace)
        .await
        .expect("Failed to write df with IfExists::Replace");

    let read_df = get_df(
        &pool,
        &format!("SELECT * FROM {}.{} ORDER BY id", schema, table),
    )
    .await
    .expect("Failed to query written table");
    assert_eq!(read_df.height(), 3);
    assert_eq!(read_df.width(), 3);

    // 2. Test Fail mode when table already exists
    let fail_result = df_to_table(&pool, &df, schema, table, IfExists::Fail).await;
    assert!(
        fail_result.is_err(),
        "Expected IfExists::Fail to error when table exists"
    );

    // 3. Test Append mode
    let s_id2 = Column::new("id".into(), &[4i32]);
    let s_name2 = Column::new("name".into(), &["david"]);
    let s_score2 = Column::new("score".into(), &[78.4f64]);
    let df2 =
        DataFrame::new(1, vec![s_id2, s_name2, s_score2]).expect("Failed to create DataFrame");

    df_to_table(&pool, &df2, schema, table, IfExists::Append)
        .await
        .expect("Failed to append df with IfExists::Append");

    let read_df2 = get_df(
        &pool,
        &format!("SELECT * FROM {}.{} ORDER BY id", schema, table),
    )
    .await
    .expect("Failed to query appended table");
    assert_eq!(read_df2.height(), 4);

    // 4. Test Truncate mode to overwrite rows without dropping the table.
    df_to_table(&pool, &df2, schema, table, IfExists::Truncate)
        .await
        .expect("Failed to overwrite df with IfExists::Truncate");

    let read_df3 = get_df(
        &pool,
        &format!("SELECT * FROM {}.{} ORDER BY id", schema, table),
    )
    .await
    .expect("Failed to query truncated table");
    assert_eq!(read_df3.height(), 1);

    // 5. Test Replace again to overwrite the table definition and rows.
    df_to_table(&pool, &df2, schema, table, IfExists::Replace)
        .await
        .expect("Failed to replace table with df2");

    let read_df4 = get_df(
        &pool,
        &format!("SELECT * FROM {}.{} ORDER BY id", schema, table),
    )
    .await
    .expect("Failed to query replaced table");
    assert_eq!(read_df4.height(), 1);

    // Cleanup test schema
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {} CASCADE",
        schema
    )))
    .execute(&pool)
    .await
    .expect("Failed to clean up test schema");

    pool.close().await;
}
