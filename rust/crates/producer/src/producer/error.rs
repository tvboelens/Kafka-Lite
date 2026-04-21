use core::fmt;

use super::RecordSendJob;
use tokio::sync::{mpsc, oneshot};
#[derive(Debug)]
pub enum SendError {
    WorkerChannelClosed,
    JobChannelClosed,
}

impl From<mpsc::error::SendError<RecordSendJob>> for SendError {
    fn from(_: mpsc::error::SendError<RecordSendJob>) -> Self {
        SendError::WorkerChannelClosed
    }
}

impl From<oneshot::error::RecvError> for SendError {
    fn from(_: oneshot::error::RecvError) -> Self {
        SendError::JobChannelClosed
    }
}

impl fmt::Display for SendError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorkerChannelClosed => write!(fmt, "worker channel closed"),
            Self::JobChannelClosed => write!(fmt, "job dropped by worker"),
        }
    }
}

impl std::error::Error for SendError {}

/*
    1. RecvError: Sender (the one in the job) is dropped without sending
        1. Backpressure
        2. Shutdown
    2. SendError:
        1. Receiver in the worker has been dropped or close() called
        2. This means shutdown.
*/
