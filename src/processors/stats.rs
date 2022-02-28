use super::GroupedHttpLogs;
use super::Processor;
use crate::reader::HttpLog;
use std::collections::HashMap;
use tracing::instrument;

#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    period_in_secs: usize,
    buffer: HashMap<String, Vec<HttpLog>>,
    last_time: usize,
}

impl Stats {
    pub fn new(period_in_secs: usize) -> Self {
        Self {
            period_in_secs,
            buffer: HashMap::new(),
            last_time: 0,
        }
    }
}

impl Processor for Stats {
    #[instrument(skip(self, writer))]
    fn process(
        &mut self,
        log_group: &GroupedHttpLogs,
        writer: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        // get individual http logs and group them by section in our buffer
        for log in &log_group.logs {
            let section = log.request.section.clone();
            let entry = self.buffer.entry(section).or_insert(Vec::new());
            entry.push(log.clone());
        }

        // check if we're over our period in secs and if so, print the stats and clear the buffer
        let diff_time = log_group.time - self.last_time;

        if diff_time >= self.period_in_secs {
            tracing::info!("Printing stats");
            self.last_time = log_group.time;
            writer.write_all(format!("\nSTATS ({}s):\n********\n", diff_time).as_bytes())?;
            // TODO: sort the stats by most requested sections
            for (section, logs) in &self.buffer {
                let mut total_reqs = 0;
                let mut total_bytes = 0;
                for log in logs {
                    total_reqs += 1;
                    total_bytes += log.bytes;
                }
                let avg_time = diff_time as f64 / total_reqs as f64;
                let avg_bytes = total_bytes / total_reqs;
                let avg_reqs_sec = total_reqs as f64 / (diff_time) as f64;
                let msg = format!(
                    "Section: {}, Total Hits: {}, Avg Reqs/Sec: {}, Avg Time: {}s, Avg Bytes: {}\n",
                    section, total_reqs, avg_reqs_sec, avg_time, avg_bytes
                );
                writer.write_all(msg.as_bytes())?;
            }
            self.buffer.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::build_test_http_grouped_log;
    use std::io::BufWriter;

    #[tokio::test]
    async fn it_works() {
        let mut stats = Stats::new(3);
        let mut writer = BufWriter::new(Vec::<u8>::new());

        let logs = vec![
            build_test_http_grouped_log(1, 3, Some("/api/users".to_string())),
            build_test_http_grouped_log(2, 3, Some("/api/users".to_string())),
            build_test_http_grouped_log(3, 2, Some("/api/friends".to_string())),
        ];

        for log in logs {
            stats.process(&log, &mut writer).unwrap();
        }

        let msg = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        assert_eq!(
            msg,
            "\nSTATS (3s):\n********\nSection: /api, Total Hits: 8, Avg Reqs/Sec: 2.6666666666666665, Avg Time: 0.375s, Avg Bytes: 100\n"
        );
    }

    #[tokio::test]
    async fn shows_as_many_sections_as_needed() {
        let mut stats = Stats::new(3);
        let mut writer = BufWriter::new(Vec::<u8>::new());

        let logs = vec![
            build_test_http_grouped_log(1, 3, Some("/web/portal".to_string())),
            build_test_http_grouped_log(2, 3, Some("/api/users".to_string())),
            build_test_http_grouped_log(3, 2, Some("/api/friends".to_string())),
        ];

        for log in logs {
            stats.process(&log, &mut writer).unwrap();
        }

        let msg = String::from_utf8(writer.into_inner().unwrap()).unwrap();

        // inner hashmap can't ensure the order for the moment
        let expect = vec!["\nSTATS (3s):\n********\nSection: /web, Total Hits: 3, Avg Reqs/Sec: 1, Avg Time: 1s, Avg Bytes: 100\nSection: /api, Total Hits: 5, Avg Reqs/Sec: 1.6666666666666667, Avg Time: 0.6s, Avg Bytes: 100\n", "\nSTATS (3s):\n********\nSection: /api, Total Hits: 5, Avg Reqs/Sec: 1.6666666666666667, Avg Time: 0.6s, Avg Bytes: 100\nSection: /web, Total Hits: 3, Avg Reqs/Sec: 1, Avg Time: 1s, Avg Bytes: 100\n"];

        assert!(expect.contains(&msg.as_str()));
    }
}
