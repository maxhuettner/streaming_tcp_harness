/// Binary serialization with null bitmap support
/// Wire format: [null_bitmap][field_values]
/// Null bitmap: 1 bit per field (1 = null, 0 = not null)
/// Only non-null field values are encoded

#[derive(Debug)]
pub enum FieldValue {
    Int64(i64),
    String(String),
    Timestamp(i64), // millis since epoch
    Null,
}

pub struct BinaryEncoder {
    fields: Vec<FieldValue>,
}

impl BinaryEncoder {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            fields: Vec::with_capacity(capacity),
        }
    }

    pub fn add_int64(&mut self, value: i64) {
        self.fields.push(FieldValue::Int64(value));
    }

    pub fn add_string(&mut self, value: String) {
        self.fields.push(FieldValue::String(value));
    }

    pub fn add_timestamp(&mut self, millis: i64) {
        self.fields.push(FieldValue::Timestamp(millis));
    }

    pub fn add_null(&mut self) {
        self.fields.push(FieldValue::Null);
    }

    /// Encode to bytes with null bitmap: [null_bitmap][field_values]
    pub fn encode_payload(self) -> Vec<u8> {
        let mut payload = Vec::new();

        // Calculate and write null bitmap
        let field_count = self.fields.len();
        let null_bitmap_size = (field_count + 7) / 8;
        let mut null_bitmap = vec![0u8; null_bitmap_size];

        // First pass: build null bitmap
        for (i, field) in self.fields.iter().enumerate() {
            if matches!(field, FieldValue::Null) {
                let byte_pos = i / 8;
                let bit_pos = i % 8;
                null_bitmap[byte_pos] |= 1 << bit_pos;
            }
        }

        payload.extend_from_slice(&null_bitmap);

        // Second pass: encode non-null values
        for field in self.fields {
            match field {
                FieldValue::Null => {
                    // Skip null fields
                }
                FieldValue::Int64(v) => {
                    payload.extend_from_slice(&v.to_be_bytes());
                }
                FieldValue::String(s) => {
                    let bytes = s.as_bytes();
                    payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    payload.extend_from_slice(bytes);
                }
                FieldValue::Timestamp(millis) => {
                    payload.extend_from_slice(&millis.to_be_bytes());
                }
            }
        }

        payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_encoding() {
        let mut encoder = BinaryEncoder::new();
        encoder.add_int64(1000);
        encoder.add_int64(2001);
        encoder.add_int64(5000);
        encoder.add_string("channel-123".to_string());

        let encoded = encoder.encode_payload();

        // 8 + 8 + 8 + 4 + 11 = 39 bytes
        assert_eq!(encoded.len(), 39);

        // First int64 should be 1000
        let val = i64::from_be_bytes([
            encoded[0], encoded[1], encoded[2], encoded[3],
            encoded[4], encoded[5], encoded[6], encoded[7],
        ]);
        assert_eq!(val, 1000);
    }
}
