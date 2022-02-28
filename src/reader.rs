use serde::{Deserialize, Deserializer, Serialize};
use tokio_stream::StreamExt;
use tracing::instrument;

pub type AsyncReader = dyn tokio::io::AsyncRead + Send + Sync + Unpin;

/// Represents a Log Request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, TryStreamExt};

    const ERR: &'static str = "err";

    fn build_test_http_log(time: usize) -> HttpLog {
        HttpLog {
            remote_host: "10.0.0.1".to_string(),
            auth_user: "apache".to_string(),
            rfc931: "-".to_string(),
            time,
            request: LogRequest {
                verb: "GET".to_string(),
                path: "/api/user".to_string(),
                section: "/api".to_string(),
                protocol: "HTTP/1.0".to_string(),
            },
            status: 200,
            bytes: 1234,
        }
    }

    #[tokio::test]
    #[ignore]
    async fn reads_csv_async_works_ok() {
        let mut input = r#"
"remotehost","rfc931","authuser","date","request","status","bytes"
"10.0.0.1","-","apache",1549573860,"GET /api/user HTTP/1.0",200,1234
"10.0.0.1","-","apache","a","GET /api/user HTTP/1.0",200,1234
"10.0.0.1","-","apache",1549573860,"GET /api/user HTTP/1.0",200,1234"#
            .as_bytes();

        let result = read_csv_async(&mut input)
            .map(|tx| tx.map_err(|_| ERR))
            .await
            .collect::<Vec<_>>()
            .await;

        let expected = vec![
            Ok(build_test_http_log(1549573860)),
            Err(ERR),
            Ok(build_test_http_log(1549573860)),
        ];

        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn reads_csv_async_works_ok_with_untrimmed_content_no_quotes() {
        let mut input = r#"
  "remotehost" ,"rfc931"  ,"authuser"  ,"date"  ,  "request", "status",  "bytes"
10.0.0.1  , -  ,   apache  ,  1549573860  , GET /api/user HTTP/1.0   , 200 , 1234
  10.0.0.1  , -   ,apache, a  ,  GET /api/user HTTP/1.0 , 200 ,  1234 "#
            .as_bytes();

        let result = read_csv_async(&mut input)
            .map(|tx| tx.map_err(|_| ERR))
            .await
            .collect::<Vec<_>>()
            .await;

        let expected = vec![Ok(build_test_http_log(1549573860)), Err(ERR)];

        assert_eq!(result, expected)
    }

    #[tokio::test]
    async fn reads_csv_async_works_ok_with_untrimmed_content_with_quotes_no_front_spaces() {
        let mut input = r#"
  "remotehost" ,"rfc931"  ,"authuser"  ,"date"  ,  "request", "status",  "bytes"
"10.0.0.1" ,"-" ,"apache"  ,1549573860  ,"GET /api/user HTTP/1.0"  , 200 , 1234
"10.0.0.1","-","apache","a" ,"GET /api/user HTTP/1.0" ,200 ,1234 "#
            .as_bytes();

        let result = read_csv_async(&mut input)
            .map(|tx| tx.map_err(|_| ERR))
            .await
            .collect::<Vec<_>>()
            .await;

        let expected = vec![Ok(build_test_http_log(1549573860)), Err(ERR)];

        assert_eq!(result, expected)
    }

    #[tokio::test]
    async fn reads_csv_async_does_not_work_with_untrimmed_content_with_quotes_and_front_spaces() {
        let mut input = r#"
  "remotehost" ,"rfc931"  ,"authuser"  ,"date"  ,  "request", "status",  "bytes"
  "10.0.0.1" ,  "-" ,  "apache"  ,   1549573860  , "GET /api/user HTTP/1.0"  , 200 , 1234
"10.0.0.1","-","apache","a" ,"GET /api/user HTTP/1.0" ,200 ,1234 "#
            .as_bytes();

        let result = read_csv_async(&mut input)
            .map(|tx| tx.map_err(|_| ERR))
            .await
            .collect::<Vec<_>>()
            .await;

        let expected = vec![Ok(build_test_http_log(1549573860)), Err(ERR)];
        assert_ne!(result, expected)
    }

    #[tokio::test]
    async fn reads_csv_async_works_ok_with_trailing_comma() {
        let mut input = r#"
"remotehost","rfc931","authuser","date","request","status","bytes",
"10.0.0.1","-","apache",1549573860,"GET /api/user HTTP/1.0",200,1234,
"10.0.0.1","-","apache","a","GET /api/user HTTP/1.0",200,1234,
"10.0.0.1","-","apache",1549573860,"GET /api/user HTTP/1.0",200,1234,"#
            .as_bytes();

        let result = read_csv_async(&mut input)
            .map(|tx| tx.map_err(|_| ERR))
            .await
            .collect::<Vec<_>>()
            .await;

        let expected = vec![
            Ok(build_test_http_log(1549573860)),
            Err(ERR),
            Ok(build_test_http_log(1549573860)),
        ];

        assert_eq!(result, expected);
    }
}
