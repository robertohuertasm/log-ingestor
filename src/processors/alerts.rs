use super::HttpLog;
use super::Processor;

pub struct Alerts {
    pub alert_threshold: usize,
}

impl Processor for Alerts {
    fn process(&mut self, log: HttpLog) -> anyhow::Result<()> {
        //
        Ok(())
    }
}
