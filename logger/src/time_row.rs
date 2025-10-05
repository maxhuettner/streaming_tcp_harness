use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct TimeRow {
    pub start_time: String,
    pub end_time: String,
    pub first_elem_time: String,
    pub last_elem_time: String,
    pub duration_us: i64,
    pub num_events: usize,
    pub tps: usize,
}
