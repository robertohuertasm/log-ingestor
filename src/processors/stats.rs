use super::HttpLog;
use super::Processor;

pub struct Stats {
    //
}

impl Processor for Stats {
    fn process(&mut self, log: HttpLog) -> anyhow::Result<()> {
        //
        Ok(())
    }
}
