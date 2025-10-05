use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct BenchmarkRow {
    pub time: String,

    pub rate: usize,

    pub requested_rate: Option<usize>,
}
