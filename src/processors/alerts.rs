use super::GroupedHttpLogs;
use super::Processor;
use std::collections::VecDeque;
use tracing::instrument;

#[derive(Debug, Clone, Eq, PartialEq)]
struct LogCounter {
    time: usize,
    req_count: usize,
}

impl From<&GroupedHttpLogs> for LogCounter {
    fn from(g: &GroupedHttpLogs) -> Self {
        LogCounter {
            time: g.time,
            req_count: g.logs.len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Alerts {
    avg_req_sec_threshold: usize,
    minor_time: usize,
    major_time: usize,
    buffer: VecDeque<LogCounter>,
    is_alert_set: bool,
    window_size_in_secs: usize,
}

impl Alerts {
    pub fn new(avg_req_sec_threshold: usize, window_size_in_secs: usize) -> Self {
        Self {
            avg_req_sec_threshold,
            minor_time: 0,
            major_time: 0,
            buffer: VecDeque::new(),
            is_alert_set: false,
            window_size_in_secs,
        }
    }
}

impl Processor for Alerts {
    #[instrument(skip(self, writer))]
    fn process(
        &mut self,
        log_group: &GroupedHttpLogs,
        writer: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        let log_counter = LogCounter::from(log_group);

        self.buffer.push_back(log_counter.clone());
        if self.minor_time == 0 && self.major_time == 0 {
            tracing::debug!("Initial time: {}", log_counter.time);
            self.minor_time = log_counter.time;
        }

        if log_counter.time < self.minor_time {
            return Err(anyhow::Error::msg(
                    "Log group time is less than the minor time. Try to adjust the BufferedLogs seconds property.",
                ));
        }

        self.major_time = log_counter.time;

        let diff_time = self.major_time - self.minor_time;

        if diff_time >= self.window_size_in_secs {
            // set the minor time to major - window secs
            self.minor_time = self.major_time - self.window_size_in_secs;
            // draing the log groups < minor time
            while let Some(log_counter) = self.buffer.front() {
                if log_counter.time >= self.minor_time {
                    break;
                }
                self.buffer.pop_front();
            }
        }

        // calculate the avg requests per window secs
        let total_reqs = self
            .buffer
            .iter()
            .fold(0, |acc, log_counter| acc + log_counter.req_count);

        let avg_req_per_sec = total_reqs as f64 / self.window_size_in_secs as f64;

        // check if the avg requests per window secs is greater than the threshold
        let is_above_threshold = avg_req_per_sec > self.avg_req_sec_threshold as f64;

        if is_above_threshold && !self.is_alert_set {
            self.is_alert_set = true;
            let msg = format!(
                "{}High traffic generated an alert - hits = {}, triggered at {}\n",
                alert_prefix(),
                avg_req_per_sec,
                log_counter.time
            );
            writer.write_all(msg.as_bytes())?;
        } else if self.is_alert_set && !is_above_threshold {
            self.is_alert_set = false;
            let msg = format!(
                "{}Normal traffic recovered - hits = {}, recovered at {}\n",
                alert_prefix(),
                avg_req_per_sec,
                log_counter.time,
            );

            writer.write_all(msg.as_bytes())?;
        }

        Ok(())
    }
}

fn alert_prefix() -> String {
    console::style("\n>>> ALERT\n").bold().red().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::build_test_http_grouped_log;
    use std::io::BufWriter;

    #[tokio::test]
    async fn should_alert_if_threshold_is_triggered() {
        let mut alerts = Alerts::new(1, 2);
        let mut writer = BufWriter::new(Vec::<u8>::new());

        let logs = build_test_http_grouped_log(1, 3, None);
        alerts.process(&logs, &mut writer).unwrap();

        let msg = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        assert_eq!(
            msg,
            format!(
                "{}High traffic generated an alert - hits = 1.5, triggered at 1\n",
                alert_prefix()
            )
        );
    }

    #[tokio::test]
    async fn should_not_send_alert_if_threshold_has_been_already_triggered() {
        let mut alerts = Alerts::new(1, 2);
        let mut writer = BufWriter::new(Vec::<u8>::new());

        let logs = vec![
            build_test_http_grouped_log(1, 3, None),
            build_test_http_grouped_log(2, 3, None),
        ];
        for log in logs {
            alerts.process(&log, &mut writer).unwrap();
        }

        let msg = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        assert_eq!(
            msg,
            format!(
                "{}High traffic generated an alert - hits = 1.5, triggered at 1\n",
                alert_prefix()
            )
        );
    }

    #[tokio::test]
    async fn should_recover_from_alert() {
        let mut alerts = Alerts::new(1, 2);
        let mut writer = BufWriter::new(Vec::<u8>::new());

        let logs = vec![
            build_test_http_grouped_log(1, 3, None),
            build_test_http_grouped_log(4, 1, None),
        ];

        for log in logs {
            alerts.process(&log, &mut writer).unwrap();
        }

        let msg = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        assert_eq!(
            msg,
            format!("{0}High traffic generated an alert - hits = 1.5, triggered at 1\n{0}Normal traffic recovered - hits = 0.5, recovered at 4\n", alert_prefix()
        )
        );
    }

    #[tokio::test]
    async fn should_not_recover_twice_from_alert() {
        let mut alerts = Alerts::new(1, 2);
        let mut writer = BufWriter::new(Vec::<u8>::new());

        let logs = vec![
            build_test_http_grouped_log(1, 3, None),
            build_test_http_grouped_log(4, 1, None),
            build_test_http_grouped_log(8, 1, None),
        ];

        for log in logs {
            alerts.process(&log, &mut writer).unwrap();
        }

        let msg = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        assert_eq!(
            msg,
            format!("{0}High traffic generated an alert - hits = 1.5, triggered at 1\n{0}Normal traffic recovered - hits = 0.5, recovered at 4\n", alert_prefix())
        );
    }
}
