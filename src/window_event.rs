use std::sync::Mutex;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct WindowEventData {
    first_elem_ts: String,
    last_elem_ts: String,
}

#[derive(Debug)]
pub struct WindowEvent {
    first_elem_time: Mutex<Option<DateTime<Utc>>>,
    last_elem_time: Mutex<Option<DateTime<Utc>>>,
}

impl WindowEvent {
    fn new(first_ts: Option<DateTime<Utc>>, last_ts: Option<DateTime<Utc>>) -> Self {
        Self {
            first_elem_time: Mutex::new(first_ts),
            last_elem_time: Mutex::new(last_ts),
        }
    }

    fn to_data(&self) -> WindowEventData {
        let first_elem_time = self.first_elem_time.lock().unwrap().map_or("".to_string(), |time| time.to_rfc3339().to_string());
        let last_elem_time = self.last_elem_time.lock().unwrap().map_or("".to_string(), |time| time.to_rfc3339().to_string());

        WindowEventData {
            first_elem_ts: first_elem_time,
            last_elem_ts: last_elem_time,
        }
    }

    fn from_data(data: WindowEventData) -> Self {
        Self {
            first_elem_time: Mutex::new(Some(DateTime::parse_from_rfc3339(&data.first_elem_ts).unwrap().to_utc())),
            last_elem_time: Mutex::new(Some(DateTime::parse_from_rfc3339(&data.last_elem_ts).unwrap().to_utc())),
        }
    }

    pub fn compare_and_set_first_elem_time(&self, new_elem_time: DateTime<Utc>) -> bool {
        let mut first_elem_time = self.first_elem_time.lock().unwrap();

        match *first_elem_time {
            Some(ref mut current_time) if (*current_time) > new_elem_time => {
                *current_time = new_elem_time;
                true
            }
            None => {
                *first_elem_time = Some(new_elem_time);
                true
            }
            _ => false,
        }
    }

    pub fn compare_and_set_last_elem_time(&self, new_elem_time: DateTime<Utc>) -> bool {
        let mut last_elem_time = self.last_elem_time.lock().unwrap();

        match *last_elem_time {
            Some(ref mut current_time) if (*current_time) < new_elem_time => {
                *current_time = new_elem_time;
                true
            }
            None => {
                *last_elem_time = Some(new_elem_time);
                true
            }
            _ => false,
        }
    }
}

impl Default for WindowEvent {
    fn default() -> Self {
        Self::new(None, None) // You can change the default values as needed
    }
}

impl Serialize for WindowEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_data().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for WindowEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = WindowEventData::deserialize(deserializer)?;
        Ok(Self::from_data(data))
    }
}