mod buffered_logs;
mod process;
mod processors;
mod reader;
#[cfg(test)]
mod test_utils;

use crate::{
    processors::{Alerts, Processor, Stats},
    reader::AsyncReader,
};
use std::env::current_dir;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "🏹 Log Ingestor CLI",
    author("💻  Roberto Huertas <roberto.huertas@outlook.com"),
    long_about = "🧰  Small utility to process http access logs"
)]
pub struct Cli {
    /// The path to the csv file containing the logs
    #[structopt(parse(from_os_str))]
    pub path: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::from_args();
    dotenv::dotenv().ok();
    set_up_tracing();
    tracing::info!("Starting the Log Ingestor CLI");

    // supporting both a path or stdin as input
    let mut reader: Box<AsyncReader> = if let Some(path) = cli.path {
        let file_path = current_dir()?.join(path);
        Box::new(tokio::fs::File::open(file_path).await?)
    } else {
        Box::new(tokio::io::stdin())
    };

    let mut writer = tokio::io::stdout();

    let processors: Vec<Box<dyn Processor>> = vec![Box::new(Alerts::new(10)), Box::new(Stats {})];

    process::process_logs(&mut reader, &mut writer, processors).await?;
    Ok(())
}

fn set_up_tracing() {
    let tracing = tracing_subscriber::fmt()
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env());

    if cfg!(debug_assertions) {
        tracing.pretty().init();
    } else {
        tracing.json().init();
    }
}
