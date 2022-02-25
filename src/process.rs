use crate::reader::{read_csv_async, AsyncReader, AsyncWriter};
use futures::StreamExt;
use tracing::instrument;

/// Processes all the logs coming from an async reader
/// and writes the results to an async writer.
#[instrument(skip(reader, _writer))]
pub async fn process_logs(
    reader: &mut AsyncReader,
    _writer: &mut AsyncWriter,
) -> anyhow::Result<()> {
    let mut log_stream = read_csv_async(reader).await;

    while let Some(log) = log_stream.next().await {
        match log {
            Ok(log) => {
                // if let Err(e) = engine.process_transaction(log).await {
                //     tracing::error!(error=?e, "Error processing transaction: {}", e);
                // }
                tracing::info!("Processed log: {:?}", log);
            }
            Err(e) => tracing::error!("CSV deserialization error: {}", e),
        }
    }

    // let report = engine.report().await?;
    // write_csv_async(writer, report).await?;

    Ok(())
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use payments_engine::Engine;
//     use payments_engine_store_memory::MemoryStore;
//     use tokio::io::BufWriter;

//     #[tokio::test]
//     async fn it_works() {
//         let mut input = r"
//         type,client,tx,amount
//         deposit,1,1,100
//         withdrawal,1,2,50
//         deposit,2,3,100
//         deposit,1,4,200
//         dispute,1,4
//         resolve,1,4
//         dispute,2,3
//         chargeback,2,3
//         dispute,1,3"
//             .as_bytes();

//         let mut output = BufWriter::new(Vec::<u8>::new());

//         let engine = Engine::new(MemoryStore::default());

//         process_transactions(&mut input, &mut output, engine)
//             .await
//             .unwrap();

//         let buffer = output.into_inner();
//         let csv = String::from_utf8_lossy(&buffer);

//         // the order is not guaranteed
//         let expected = (csv
//             == "client,available,held,total,locked\n1,250,0,250,false\n2,0,0,0,true\n")
//             || (csv == "client,available,held,total,locked\n2,0,0,0,true\n1,250,0,250,false\n");

//         assert!(expected);
//     }
// }
