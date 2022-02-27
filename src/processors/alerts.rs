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
}

impl Alerts {
    pub fn new(avg_req_sec_threshold: usize) -> Self {
        Self {
            avg_req_sec_threshold,
            minor_time: 0,
            major_time: 0,
            buffer: Vec::new(),
            is_alert_set: false,
        }
    }
}

impl Processor for Alerts {
    fn name(&self) -> &'static str {
        "Alerts processor"
    }

    #[instrument(skip(self))]
    fn process(&mut self, log: &HttpLog) -> anyhow::Result<()> {
        if self.minor_time == 0 && self.major_time == 0 {
            tracing::debug!("Initial time: {}", log.time);
            self.minor_time = log.time;
            self.major_time = log.time;
        } else {
            if log.time < self.minor_time {
                return Err(anyhow::Error::msg(
                    "Log time is less than the minor time. Try to adjust the BufferedLogs seconds property.",
                ));
            }

            self.buffer.push(log.clone());

            self.major_time = log.time;

            let diff_time = self.major_time - self.minor_time;

            if diff_time < 120 {
                return Ok(());
            }

            // set the minor time to major - 120
            self.minor_time = self.major_time - 120;
            // draing the logs < minor time
            self.buffer.retain(|log| log.time >= self.minor_time);
            // calculate the avg requests per 120 secs
            let avg_req_per_sec = self.buffer.len() as f64 / 120.0;
            // check if the avg requests per 120 secs is greater than the threshold
            let is_above_threshold = avg_req_per_sec > self.avg_req_sec_threshold as f64;
            if is_above_threshold && !self.is_alert_set {
                self.is_alert_set = true;
                println!(
                    "High traffic generated an alert - hits = {}, triggered at {}",
                    avg_req_per_sec, log.time
                );
            } else if self.is_alert_set && !is_above_threshold {
                self.is_alert_set = false;
                println!(
                    "Normal traffic recovered - hits = {}, recovered at {}",
                    avg_req_per_sec, log.time,
                );
            }
        }
        Ok(())
    }
}

// TODO: TESTS
