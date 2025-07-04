#[derive(Clone, Default, Debug, Copy)]
pub enum OverallocationPolicy {
    #[default]
    None,
    Bytes(u64),
    Percentage(f64),
    PercentageCapped {
        percentage: f64,
        max_add_bytes: u64,
    },
}
impl OverallocationPolicy {
    pub fn calculate(&self, required_bytes: u64) -> u64 {
        match self {
            OverallocationPolicy::None => required_bytes,
            OverallocationPolicy::Bytes(b) => required_bytes + *b,
            OverallocationPolicy::Percentage(pct) => {
                required_bytes.saturating_add(((required_bytes as f64) * pct).floor() as u64)
            }
            OverallocationPolicy::PercentageCapped {
                percentage,
                max_add_bytes,
            } => {
                let over = ((required_bytes as f64) * percentage).floor() as u64;
                required_bytes
                    .saturating_add(over)
                    .min(required_bytes + *max_add_bytes)
            }
        }
    }
}
