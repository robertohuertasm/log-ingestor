use super::HttpLog;
use super::Processor;
use tracing::instrument;

#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    //
}

impl Processor for Stats {
    fn name(&self) -> &'static str {
        "Stats processor"
    }

    #[instrument(skip(self))]
    fn process(&mut self, log: &HttpLog) -> anyhow::Result<()> {
        // println!("Stat processing: {:?}", log);
        Ok(())
    }
}
