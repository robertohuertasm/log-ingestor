use std::sync::Arc;

use crate::{
    buffered_logs::BufferedLogs,
    processors::Processor,
    reader::{read_csv_async, AsyncReader},
};
use futures::StreamExt;
use rayon::iter::{IntoParallelRefMutIterator, ParallelIterator};
use tracing::instrument;

/// Processes all the logs coming from an async reader
#[instrument(skip(reader, processors))]
pub async fn process_logs<'a>(
    reader: &'a mut AsyncReader,
    mut processors: Vec<Box<dyn Processor>>,
) -> anyhow::Result<()> {
    // reading and buffering in order to order the logs
    // we'll use a 2 secs buffer
    let log_stream = read_csv_async(reader).await;
    let mut log_stream = BufferedLogs::new(log_stream, 2);

    // sending logs to all processors in a parallel way
    while let Some(log) = log_stream.next().await {
        let log = Arc::new(log);
        processors.par_iter_mut().for_each(|processor| {
            if let Err(e) = processor.process(&log.clone(), &mut std::io::stdout()) {
                tracing::error!("Error processing log: {:?} - {:?}", log, e);
            }
        });
    }
    tracing::info!("Processing done!");
    Ok(())
}
