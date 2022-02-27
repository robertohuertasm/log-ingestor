use serde::{Deserialize, Deserializer, Serialize};
use tokio_stream::StreamExt;
use tracing::instrument;

pub type AsyncReader = dyn tokio::io::AsyncRead + Send + Sync + Unpin;
pub type AsyncWriter = dyn tokio::io::AsyncWrite + Send + Sync + Unpin;
/// Represents a Log Request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogRequest {
    /// Verb of the request.
    pub verb: String,
    /// Path of the request.
    pub path: String,
    /// First part of the path.
    pub section: String,
    /// Protocol of the request.
    pub protocol: String,
}

impl LogRequest {
    fn from_str(line: &str) -> anyhow::Result<Self> {
        let mut parts = line.split_whitespace();

        let verb = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("Invalid line, no verb: {}", line))?;
        let path = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("Invalid line, no path: {}", line))?;
        let protocol = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("Invalid line, no protocol: {}", line))?;

        let section = path
            .chars()
            .enumerate()
            .take_while(|(i, c)| *i == 0 || *c != '/')
            .map(|(_, c)| c)
            .collect::<String>();

        Ok(Self {
            verb: verb.to_string(),
            path: path.to_string(),
            section: section.to_string(),
            protocol: protocol.to_string(),
        })
    }
}

fn deserialize_log_request<'de, D>(deserializer: D) -> Result<LogRequest, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    LogRequest::from_str(&buf).map_err(serde::de::Error::custom)
}

/// Represents an Http Log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HttpLog {
    /// The remote host IP.
    #[serde(rename = "remotehost")]
    pub remote_host: String,
    /// The rc931.
    pub rfc931: String,
    /// The Auth user.
    #[serde(rename = "authuser")]
    pub auth_user: String,
    /// Epoch time of the log.
    pub time: usize,
    /// The request: verb, path, protocol.
    #[serde(deserialize_with = "deserialize_log_request")]
    pub request: LogRequest,
    /// The status code.
    pub status: u16,
    /// The amount of bytes.
    pub bytes: usize,
}

/// Reads a CSV file asynchronously.
#[instrument(skip(reader))]
pub async fn read_csv_async(
    reader: &mut AsyncReader,
) -> impl futures::Stream<Item = Result<HttpLog, anyhow::Error>> + '_ {
    csv_async::AsyncReaderBuilder::new()
        .flexible(true)
        .trim(csv_async::Trim::All)
        .create_reader(reader)
        .into_records()
        .map(|record| {
            record
                .and_then(|r| r.deserialize::<HttpLog>(None).map(std::convert::Into::into))
                .map_err(anyhow::Error::from)
        })
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use futures::{FutureExt, TryStreamExt};
//     use payments_engine_core::dec;

//     const ERR: &'static str = "err";

//     #[tokio::test]
//     async fn reads_csv_async_works_ok() {
//         let mut input = r"
//         type,client,tx,amount
//         deposit,1,10,100
//         deposito,1,11,100.0
//         withdrawal,1,12,200.0
//         resolve,1,13,
//         resolve,1,14, 100.000
//         dispute,1,15,
//         dispute,1,16, 100.000
//         chargeback,1,17,
//         chargeback,1,18, 100.000
//         deposit,1,19,5.001
//         withdrawal,1,20,43.3423
//         withdrawal,1,21,
//         deposit,1,22,"
//             .as_bytes();

//         let result = read_csv_async(&mut input)
//             .map(|tx| tx.map_err(|_| ERR))
//             .await
//             .collect::<Vec<_>>()
//             .await;

//         let expected = vec![
//             Ok(EngineTransaction::deposit(10, 1, dec!(100.00))),
//             Err(ERR),
//             Ok(EngineTransaction::withdrawal(12, 1, dec!(200.00))),
//             Ok(EngineTransaction::resolve(13, 1)),
//             Ok(EngineTransaction::resolve(14, 1)),
//             Ok(EngineTransaction::dispute(15, 1)),
//             Ok(EngineTransaction::dispute(16, 1)),
//             Ok(EngineTransaction::chargeback(17, 1)),
//             Ok(EngineTransaction::chargeback(18, 1)),
//             Ok(EngineTransaction::deposit(19, 1, dec!(5.001))),
//             Ok(EngineTransaction::withdrawal(20, 1, dec!(43.3423))),
//             Ok(EngineTransaction::withdrawal(21, 1, dec!(0.0))),
//             Ok(EngineTransaction::deposit(22, 1, dec!(0.0))),
//         ];

//         assert_eq!(result, expected)
//     }

//     #[tokio::test]
//     async fn reads_csv_async_works_ok_with_untrimmed_content() {
//         let mut input = r"
//         type    ,client,        tx,     amount
//             deposit   ,1  , 10,   100
//           deposito,      1, 11 , 100.0,
//         withdrawal,1,12,   200.0
//         resolve,1,     13,
//         resolve,1,    14, 100.000,
//         dispute   ,1,   15,
//          dispute  ,1,    16,   100.000,
//            chargeback ,1,17,
//          chargeback     ,1, 18, 100.000,
//            deposit  ,1, 19, 5.001
//         withdrawal, 1,    20,  43.3423
//          withdrawal ,1, 21   ,
//             deposit   , 1, 22,  "
//             .as_bytes();

//         let result = read_csv_async(&mut input)
//             .map(|tx| tx.map_err(|_| ERR))
//             .await
//             .collect::<Vec<_>>()
//             .await;

//         let expected = vec![
//             Ok(EngineTransaction::deposit(10, 1, dec!(100.00))),
//             Err(ERR),
//             Ok(EngineTransaction::withdrawal(12, 1, dec!(200.00))),
//             Ok(EngineTransaction::resolve(13, 1)),
//             Ok(EngineTransaction::resolve(14, 1)),
//             Ok(EngineTransaction::dispute(15, 1)),
//             Ok(EngineTransaction::dispute(16, 1)),
//             Ok(EngineTransaction::chargeback(17, 1)),
//             Ok(EngineTransaction::chargeback(18, 1)),
//             Ok(EngineTransaction::deposit(19, 1, dec!(5.001))),
//             Ok(EngineTransaction::withdrawal(20, 1, dec!(43.3423))),
//             Ok(EngineTransaction::withdrawal(21, 1, dec!(0.0))),
//             Ok(EngineTransaction::deposit(22, 1, dec!(0.0))),
//         ];

//         assert_eq!(result, expected)
//     }

//     #[tokio::test]
//     async fn reads_csv_async_works_ok_with_no_trailing_comma() {
//         let mut input = r"
//         type,client,tx,amount
//         dispute,1,10
//         resolve,1,11"
//             .as_bytes();

//         let result = read_csv_async(&mut input)
//             .map(|tx| tx.map_err(|e| e.to_string()))
//             .await
//             .collect::<Vec<_>>()
//             .await;

//         let expected = vec![
//             Ok(EngineTransaction::dispute(10, 1)),
//             Ok(EngineTransaction::resolve(11, 1)),
//         ];

//         assert_eq!(result, expected)
//     }
// }
