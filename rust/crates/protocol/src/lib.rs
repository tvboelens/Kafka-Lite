use common::ProducerRecord;
use uuid::Uuid;

enum RequestType {
    Append,
    Fetch,
}

pub struct TcpRequest {
    correlation_id: Uuid,
    request_type: RequestType,
    version: u8,
    payload: Vec<u8>,
}

fn serialize_record(record: ProducerRecord) -> Vec<u8> {
    let buf_size: usize = record.key.len() + record.value.len();
    let mut buf: Vec<u8> = Vec::new();
    buf.resize(buf_size, 0);
    buf
}

impl From<ProducerRecord> for TcpRequest {
    fn from(record: ProducerRecord) -> Self {
        TcpRequest {
            correlation_id: Uuid::new_v4(),
            request_type: RequestType::Append,
            version: 1,
            payload: serialize_record(record),
        }
    }
}

impl TcpRequest {
    pub fn to_le_bytes(&self) -> Vec<u8> {
        let buf_size: usize = 0;
        let mut buf: Vec<u8> = Vec::new();
        buf.resize(buf_size, 0);
        buf
    }
}
