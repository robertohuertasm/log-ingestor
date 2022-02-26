use crate::reader::HttpLog;
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use std::{
    collections::VecDeque,
    fmt::{self, Debug},
    pin::Pin,
    task::{Context, Poll},
};

#[pin_project]
#[must_use = "streams do nothing unless polled"]
pub struct BufferedLogs<St>
where
    St: Stream<Item = Result<HttpLog, anyhow::Error>>,
{
    #[pin]
    stream: futures::stream::Fuse<St>,
    // #[pin]
    buf: VecDeque<Result<HttpLog, anyhow::Error>>,
    seconds: u64,
}

impl<St> Debug for BufferedLogs<St>
where
    St: Stream<Item = Result<HttpLog, anyhow::Error>> + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferedLogs")
            .field("stream", &self.stream)
            .field("buf", &self.buf)
            .field("seconds", &self.seconds)
            .finish()
    }
}

impl<St> BufferedLogs<St>
where
    St: Stream<Item = Result<HttpLog, anyhow::Error>>,
{
    pub fn new(stream: St, seconds: u64) -> Self {
        Self {
            stream: stream.fuse(),
            buf: VecDeque::new(),
            seconds,
        }
    }

    // delegate_access_inner!(stream, St, (.));
}

impl<St> Stream for BufferedLogs<St>
where
    St: Stream<Item = Result<HttpLog, anyhow::Error>>,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures.
        while this.buf.len() < *this.seconds as usize {
            println!("buffering");
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(x)) => this.buf.push_back(x),
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Attempt get the next value from the logs queue
        if let Some(val) = this.buf.pop_back() {
            println!("from buffer");
            return Poll::Ready(Some(val));
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_done() {
            println!("done");
            Poll::Ready(None)
        } else {
            println!("pending");
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::read_csv_async;
    use futures::StreamExt;

    #[tokio::test]
    async fn it_buffers_logs_and_orders_them() {
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

        let mut log_stream = BufferedLogs::new(log_stream, 100);

        while let Some(log) = log_stream.next().await {
            println!("{:?}", log.unwrap().date);
        }
    }
}
