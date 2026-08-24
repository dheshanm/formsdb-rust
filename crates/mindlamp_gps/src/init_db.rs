use std::error::Error;

use tracing::info;

type InitResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const INIT_QUERIES: &[&str] = &[
    "CREATE SCHEMA IF NOT EXISTS mindlamp;",
    
    r#"CREATE TABLE IF NOT EXISTS mindlamp.gps_observations (
        site_id       text NOT NULL,
        subject_id    text NOT NULL,
        observed_at   timestamptz NOT NULL,
        accuracy_m    double precision,
        altitude_m    double precision,
        latitude      double precision NOT NULL,
        longitude     double precision NOT NULL,

        -- WGS84 point; longitude first, then latitude
        geom          geometry(Point, 4326) NOT NULL,

        PRIMARY KEY (site_id, subject_id, observed_at)
    );"#,
    
    r#"SELECT create_hypertable(
        'mindlamp.gps_observations',
        by_range('observed_at', INTERVAL '1 week'),
        if_not_exists => TRUE
    );"#,
    
    r#"CREATE INDEX IF NOT EXISTS mindlamp_gps_observations_subject_time_idx
    ON mindlamp.gps_observations (site_id, subject_id, observed_at DESC);"#,
    
    r#"CREATE INDEX IF NOT EXISTS mindlamp_gps_observations_geom_gix
    ON mindlamp.gps_observations
    USING GIST (geom);"#,
];

#[tokio::main]
async fn main() -> InitResult<()> {
    tracing_subscriber::fmt::init();

    info!("Initializing Mindlamp schema and GPS observations table...");

    let db_uri = std::env::var("DB_URI").map_err(|_| "DB_URI environment variable must be set")?;
    let pool = db::create_pool_with_options(&db_uri, 1).await?;

    let queries: Vec<String> = INIT_QUERIES.iter().map(|&q| q.to_string()).collect();
    db::execute_queries_in_transaction(&pool, &queries).await?;

    info!("Mindlamp schema initialization completed successfully.");

    pool.close().await;
    Ok(())
}
