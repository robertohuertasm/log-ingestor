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

    #[instrument(skip(self, writer))]
    fn process(&mut self, log: &HttpLog, writer: &mut dyn std::io::Write) -> anyhow::Result<()> {
        // println!("Stat processing: {:?}", log);
        Ok(())
    }
}
