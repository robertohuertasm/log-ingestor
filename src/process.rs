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
    let mut grouped_log_stream = BufferedLogs::new(log_stream, 2);

    // sending logs to all processors in a parallel way
    while let Some(log_group) = grouped_log_stream.next().await {
        let log_group = Arc::new(log_group);
        processors.par_iter_mut().for_each(|processor| {
            if let Err(e) = processor.process(&log_group.clone(), &mut std::io::stdout()) {
                tracing::error!("Error processing log group: {:?} - {:?}", log_group, e);
            }
        });
    }
    tracing::info!("Processing done!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::processors::MockProcessor;

    use super::*;

    #[tokio::test]
    async fn each_processor_is_called_n_times() {
        let mut input = r#"
"remotehost","rfc931","authuser","date","request","status","bytes"
"10.0.0.2","-","apache",1549573860,"GET /api/user HTTP/1.0",200,1234
"10.0.0.4","-","apache",1549573860,"GET /api/user HTTP/1.0",200,1234
"10.0.0.4","-","apache",1549573860,"GET /api/user HTTP/1.0",200,1234
"10.0.0.2","-","apache",1549573860,"GET /api/help HTTP/1.0",200,1234
"10.0.0.5","-","apache",1549573860,"GET /api/help HTTP/1.0",200,1234
"10.0.0.4","-","apache",1549573859,"GET /api/help HTTP/1.0",200,1234
"10.0.0.5","-","apache",1549573860,"POST /report HTTP/1.0",500,1307
"10.0.0.3","-","apache",1549573860,"POST /report HTTP/1.0",200,1234
"10.0.0.3","-","apache",1549573860,"GET /report HTTP/1.0",200,1194
"10.0.0.4","-","apache",1549573861,"GET /api/user HTTP/1.0",200,1136
"10.0.0.5","-","apache",1549573861,"GET /api/user HTTP/1.0",200,1194
"10.0.0.1","-","apache",1549573861,"GET /api/user HTTP/1.0",200,1261
"10.0.0.3","-","apache",1549573860,"GET /api/help HTTP/1.0",200,1234
"10.0.0.2","-","apache",1549573861,"GET /api/help HTTP/1.0",200,1194
"10.0.0.5","-","apache",1549573860,"GET /api/help HTTP/1.0",200,1234
"10.0.0.2","-","apache",1549573861,"GET /report HTTP/1.0",200,1136
"10.0.0.5","-","apache",1549573861,"POST /report HTTP/1.0",200,1136
"10.0.0.5","-","apache",1549573862,"GET /report HTTP/1.0",200,1261
"10.0.0.2","-","apache",1549573863,"POST /api/user HTTP/1.0",404,1307
"10.0.0.2","-","apache",1549573862,"GET /api/user HTTP/1.0",200,1234
"10.0.0.4","-","apache",1549573861,"GET /api/user HTTP/1.0",200,1234
"10.0.0.1","-","apache",1549573862,"GET /api/help HTTP/1.0",500,1136
"10.0.0.4","-","apache",1549573862,"POST /api/help HTTP/1.0",200,1234
"10.0.0.1","-","apache",1549573862,"GET /api/help HTTP/1.0",200,1234
"10.0.0.1","-","apache",1549573862,"GET /report HTTP/1.0",500,1194
"10.0.0.2","-","apache",1549573862,"GET /report HTTP/1.0",200,1307
"10.0.0.2","-","apache",1549573863,"GET /report HTTP/1.0",200,1194"#
            .as_bytes();

        let mut mock_processor = MockProcessor::new();
        mock_processor
            .expect_process()
            .times(5)
            .returning(|_, _| Ok(()));

        let mut mock_processor2 = MockProcessor::new();
        mock_processor2
            .expect_process()
            .times(5)
            .returning(|_, _| Ok(()));

        let processors: Vec<Box<dyn Processor>> =
            vec![Box::new(mock_processor), Box::new(mock_processor2)];

        let result = process_logs(&mut input, processors).await;

        assert!(result.is_ok());
    }
}
