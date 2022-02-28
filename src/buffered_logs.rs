use crate::reader::HttpLog;
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
};

pub type LogResult = Result<HttpLog, anyhow::Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupedHttpLogs {
    pub time: usize,
    pub logs: Vec<HttpLog>,
}

#[pin_project]
#[must_use = "streams do nothing unless polled"]
#[derive(Debug)]
pub struct BufferedLogs<St>
where
    St: Stream<Item = LogResult>,
{
    #[pin]
    stream: futures::stream::Fuse<St>,
    seconds: usize,
    time_buffer: HashMap<usize, Vec<HttpLog>>,
    ordered_time_buffer: Vec<usize>,
    minor_time_in_buffer: usize,
    major_time_in_buffer: usize,
}

impl<St> BufferedLogs<St>
where
    St: Stream<Item = LogResult>,
{
    pub fn new(stream: St, seconds: usize) -> Self {
        Self {
            stream: stream.fuse(),
            seconds,
            time_buffer: HashMap::new(),
            ordered_time_buffer: Vec::new(),
            minor_time_in_buffer: 0,
            major_time_in_buffer: 0,
        }
    }
}

impl<St> Stream for BufferedLogs<St>
where
    St: Stream<Item = LogResult>,
{
    type Item = GroupedHttpLogs;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        while *this.major_time_in_buffer - *this.minor_time_in_buffer <= *this.seconds
            && !this.stream.is_done()
        {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(x)) => {
                    match x {
                        Ok(log) => {
                            let current_date = log.time;
                            if *this.minor_time_in_buffer == 0 && *this.major_time_in_buffer == 0 {
                                *this.minor_time_in_buffer = current_date;
                                *this.major_time_in_buffer = current_date;
                            }
                            if current_date < *this.minor_time_in_buffer {
                                *this.minor_time_in_buffer = current_date;
                            }
                            if current_date > *this.major_time_in_buffer {
                                *this.major_time_in_buffer = current_date;
                            }
                            // insert and sort
                            let log_set =
                                this.time_buffer.entry(current_date).or_insert_with(|| {
                                    this.ordered_time_buffer.push(current_date);
                                    // major to minor
                                    this.ordered_time_buffer.sort_by(|a, b| b.cmp(a));
                                    Vec::new()
                                });
                            log_set.push(log);
                        }
                        Err(e) => {
                            // swallowing log parsing errors and log it
                            tracing::error!("Error buffering logs: {}", e);
                        }
                    }
                }
                Poll::Ready(None) => break,
                Poll::Pending => (), // keep waiting
            }
        }

        if let Some(log_time) = this.ordered_time_buffer.pop() {
            // modify the minor date and return
            *this.minor_time_in_buffer = this
                .ordered_time_buffer
                .last()
                .copied()
                .unwrap_or(*this.major_time_in_buffer);
            // return the entry
            if let Some(group) = this
                .time_buffer
                .remove(&log_time)
                .map(|logs| GroupedHttpLogs {
                    time: log_time,
                    logs,
                })
            {
                return Poll::Ready(Some(group));
            }
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{reader::read_csv_async, test_utils};
    use futures::StreamExt;

    fn assert_buffered_is_ordered(logs: &Vec<GroupedHttpLogs>) {
        let is_sorted = test_utils::is_sorted_by(logs.iter(), |a, b| a.time.partial_cmp(&b.time));
        assert!(is_sorted);
    }

    #[tokio::test]
    async fn it_buffers_logs_and_returns_them_in_order_from_memory() {
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
        let log_stream = read_csv_async(&mut input).await;
        let log_stream = BufferedLogs::new(log_stream, 2);
        let logs = log_stream.collect::<Vec<_>>().await;
        let log_dates = logs.iter().map(|x| x.time).collect::<Vec<_>>();
        assert_buffered_is_ordered(&logs);
        assert_eq!(
            log_dates,
            vec![1549573859, 1549573860, 1549573861, 1549573862, 1549573863]
        );
    }

    #[tokio::test]
    async fn it_buffers_logs_and_returns_them_in_order_from_file() {
        let file_path = std::env::current_dir().unwrap().join("sample.csv");
        let mut input = tokio::fs::File::open(file_path).await.unwrap();
        let log_stream = read_csv_async(&mut input).await;
        let log_stream = BufferedLogs::new(log_stream, 2);
        let logs = log_stream.collect::<Vec<_>>().await;
        assert_buffered_is_ordered(&logs);
    }
}
