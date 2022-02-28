use super::HttpLog;
use super::Processor;
use tracing::instrument;

#[derive(Debug, Clone, PartialEq)]
pub struct Alerts {
    avg_req_sec_threshold: usize,
    minor_time: usize,
    major_time: usize,
    buffer: Vec<HttpLog>,
    is_alert_set: bool,
    window_size_in_secs: usize,
}

impl Alerts {
    pub fn new(avg_req_sec_threshold: usize, window_size_in_secs: usize) -> Self {
        Self {
            avg_req_sec_threshold,
            minor_time: 0,
            major_time: 0,
            buffer: Vec::new(),
            is_alert_set: false,
            window_size_in_secs,
        }
    }
}

impl Processor for Alerts {
    fn name(&self) -> &'static str {
        "Alerts processor"
    }

    #[instrument(skip(self, writer))]
    fn process(&mut self, log: &HttpLog, writer: &mut dyn std::io::Write) -> anyhow::Result<()> {
        if self.minor_time == 0 && self.major_time == 0 {
            tracing::debug!("Initial time: {}", log.time);
            self.minor_time = log.time;
            self.major_time = log.time;
            self.buffer.push(log.clone());
        } else {
            if log.time < self.minor_time {
                return Err(anyhow::Error::msg(
                    "Log time is less than the minor time. Try to adjust the BufferedLogs seconds property.",
                ));
            }

            self.buffer.push(log.clone());

            self.major_time = log.time;

            let diff_time = self.major_time - self.minor_time;

            if diff_time >= self.window_size_in_secs {
                // set the minor time to major - 120
                self.minor_time = self.major_time - self.window_size_in_secs;
                // draing the logs < minor time
                self.buffer.retain(|log| log.time >= self.minor_time);
                // TODO: optimization: use a vecdeque and pop_front until lot time >= minor time
            }

            // calculate the avg requests per window secs
            let avg_req_per_sec = self.buffer.len() as f64 / self.window_size_in_secs as f64;
            // check if the avg requests per window secs is greater than the threshold
            let is_above_threshold = avg_req_per_sec > self.avg_req_sec_threshold as f64;

            if is_above_threshold && !self.is_alert_set {
                self.is_alert_set = true;
                let msg = format!(
                    "High traffic generated an alert - hits = {}, triggered at {}\n",
                    avg_req_per_sec, log.time
                );
                writer.write_all(msg.as_bytes())?;
            } else if self.is_alert_set && !is_above_threshold {
                self.is_alert_set = false;
                let msg = format!(
                    "Normal traffic recovered - hits = {}, recovered at {}\n",
                    avg_req_per_sec, log.time,
                );

                writer.write_all(msg.as_bytes())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::reader::LogRequest;

    use super::*;
    use std::io::BufWriter;

    fn build_test_http_log(time: usize) -> HttpLog {
        HttpLog {
            remote_host: "10.1.1.1".to_string(),
            auth_user: "auth_user".to_string(),
            rfc931: "-".to_string(),
            time,
            request: LogRequest {
                verb: "GET".to_string(),
                path: "/api/test".to_string(),
                section: "/api".to_string(),
                protocol: "HTTP/1.1".to_string(),
            },
            status: 200,
            bytes: 100,
        }
    }

    #[tokio::test]
    async fn should_alert_if_threshold_is_triggered() {
        let mut alerts = Alerts::new(1, 2);
        let mut writer = BufWriter::new(Vec::<u8>::new());

        let logs = vec![
            build_test_http_log(1),
            build_test_http_log(1),
            build_test_http_log(1),
        ];
        for log in logs {
            alerts.process(&log, &mut writer).unwrap();
        }

        let msg = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        assert_eq!(
            msg,
            "High traffic generated an alert - hits = 1.5, triggered at 1\n"
        );
    }

    #[tokio::test]
    async fn should_not_send_alert_if_threshold_has_been_already_triggered() {
        let mut alerts = Alerts::new(1, 2);
        let mut writer = BufWriter::new(Vec::<u8>::new());

        let logs = vec![
            build_test_http_log(1),
            build_test_http_log(1),
            build_test_http_log(1),
            build_test_http_log(1),
            build_test_http_log(1),
            build_test_http_log(1),
        ];
        for log in logs {
            alerts.process(&log, &mut writer).unwrap();
        }

        let msg = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        assert_eq!(
            msg,
            "High traffic generated an alert - hits = 1.5, triggered at 1\n"
        );
    }

    #[tokio::test]
    async fn should_recover_from_alert() {
        let mut alerts = Alerts::new(1, 2);
        let mut writer = BufWriter::new(Vec::<u8>::new());

        let logs = vec![
            build_test_http_log(1),
            build_test_http_log(1),
            build_test_http_log(1),
            build_test_http_log(4),
        ];
        for log in logs {
            alerts.process(&log, &mut writer).unwrap();
        }

        let msg = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        assert_eq!(
            msg,
            "High traffic generated an alert - hits = 1.5, triggered at 1\nNormal traffic recovered - hits = 0.5, recovered at 4\n"
        );
    }

    #[tokio::test]
    async fn should_not_recover_twice_from_alert() {
        let mut alerts = Alerts::new(1, 2);
        let mut writer = BufWriter::new(Vec::<u8>::new());

        let logs = vec![
            build_test_http_log(1),
            build_test_http_log(1),
            build_test_http_log(1),
            build_test_http_log(4),
            build_test_http_log(8),
        ];
        for log in logs {
            alerts.process(&log, &mut writer).unwrap();
        }

        let msg = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        assert_eq!(
            msg,
            "High traffic generated an alert - hits = 1.5, triggered at 1\nNormal traffic recovered - hits = 0.5, recovered at 4\n"
        );
    }
}
