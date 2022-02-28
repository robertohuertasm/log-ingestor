mod alerts;
mod stats;

pub use alerts::Alerts;
pub use stats::Stats;

use crate::buffered_logs::GroupedHttpLogs;

#[cfg_attr(test, mockall::automock)]
pub trait Processor: Sync + Send {
    fn process(
        &mut self,
        log_group: &GroupedHttpLogs,
        writer: &mut dyn std::io::Write,
    ) -> anyhow::Result<()>;
}
