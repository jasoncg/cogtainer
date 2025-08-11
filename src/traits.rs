pub trait Truncate {
    fn truncate(&self, offset: u64) -> Result<(), ()>;
}
