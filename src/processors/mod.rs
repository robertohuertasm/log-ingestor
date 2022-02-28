mod alerts;
mod stats;

use crate::reader::HttpLog;

pub use alerts::Alerts;
pub use stats::Stats;

pub trait Processor: Sync + Send {
    fn name(&self) -> &'static str;
    fn process(&mut self, log: &HttpLog, writer: &mut dyn std::io::Write) -> anyhow::Result<()>;
}
