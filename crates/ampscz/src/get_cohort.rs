use clap::Parser;
use tracing::{error, info, warn};

/// CLI application to get the cohort of a subject by subject_id
/// Required environment variable: DB_URI to be set
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Subject ID to query for cohort
    #[arg(short, long)]
    subject_id: String,
}

fn main() {
    tracing_subscriber::fmt::init(); 
    let cli = Cli::parse();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let db_uri = std::env::var("DB_URI").expect("DB_URI environment variable must be set");
        let pool = db::create_pool_with_options(&db_uri, 5)
            .await
            .expect("Failed to create connection pool");

        match ampscz::get_subject_cohort(&cli.subject_id, &pool).await {
            Ok(Some(cohort)) => info!("{}", cohort),
            Ok(None) => warn!("Subject {} is not in a known cohort", cli.subject_id),
            Err(e) => error!("Error retrieving cohort for subject {}: {}", cli.subject_id, e),
        }
    });
}
