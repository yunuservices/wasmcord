use anyhow::Result;
use tracing_subscriber::EnvFilter;

pub fn init_logging() -> Result<()> {
    let level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "ynsrvcs=info".into());

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&level)?)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(true)
        .init();

    Ok(())
}
