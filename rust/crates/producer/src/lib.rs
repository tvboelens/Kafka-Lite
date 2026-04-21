pub mod producer {
    pub mod error;
    use common::{ProducerRecord, RecordMetaData};
    use core::net::SocketAddr;
    use error::SendError;
    use protocol::TcpRequest;
    use std::io;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::{mpsc, oneshot};
    pub struct RecordSendJob {
        pub record: ProducerRecord,
        pub sender: oneshot::Sender<RecordMetaData>,
    }
    pub struct Producer {
        sender: mpsc::Sender<RecordSendJob>,
    }

    struct Worker {
        receiver: mpsc::Receiver<RecordSendJob>,
        address: SocketAddr,
    }

    impl Producer {
        pub async fn send(&self, record: ProducerRecord) -> Result<RecordMetaData, SendError> {
            let (tx, rx) = oneshot::channel::<RecordMetaData>();
            let job = RecordSendJob {
                record: record,
                sender: tx,
            };
            self.sender.send(job).await?;
            let meta_data = rx.await?;
            Ok(meta_data)
        }
    }

    impl Worker {
        async fn send(&self, record: ProducerRecord) -> io::Result<()> {
            let mut stream = TcpStream::connect(self.address).await?;
            let buf = TcpRequest::from(record).to_le_bytes();
            stream.write_all(&buf).await
        }
    }
}
