mod alerts;
mod stats;

use crate::reader::HttpLog;

pub use alerts::Alerts;
pub use stats::Stats;

pub trait Processor {
    fn process(&mut self, log: HttpLog) -> anyhow::Result<()>;
}
