//! Aims to provide a simple pipe-like functionality for async code.
//!
//! # Example
//!
//! ```rust
//! # #[tokio::main]
//! # async fn main() {
//! #    use tokio::io::{AsyncWriteExt, AsyncReadExt};
//!     let (mut reader, mut writer) = simple_async_pipe::pipe(64);
//!
//!     let message = b"hello world";
//!     writer.write_all(message).await.unwrap();
//!
//!     let mut buffer = vec![0u8; message.len()];
//!     reader.read_exact(&mut buffer).await.unwrap();
//!     assert_eq!(&buffer, message);
//! # }
//! ```

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

pub struct PipeWrite {
    sender: mpsc::Sender<Vec<u8>>,
    shared: Arc<Mutex<PipeShared>>,
}

pub struct PipeRead {
    read_remaining: Vec<u8>,
    receiver: mpsc::Receiver<Vec<u8>>,
    shared: Arc<Mutex<PipeShared>>,
}

struct PipeShared {
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
}

/// Creates a in-memory pipe. [PipeWrite] will not succeed instant if the internal buffer is full.
pub fn pipe(buffer: usize) -> (PipeRead, PipeWrite) {
    let (sender, receiver) = mpsc::channel(buffer);
    let shared = Arc::new(Mutex::new(PipeShared {
        read_waker: Default::default(),
        write_waker: Default::default(),
    }));

    let read = PipeRead {
        receiver,
        read_remaining: Default::default(),
        shared: shared.clone(),
    };
    let write = PipeWrite {
        sender,
        shared: shared.clone(),
    };

    (read, write)
}

impl AsyncWrite for PipeWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.sender.try_send(buf.to_vec()) {
            Ok(_) => {
                if let Some(read_waker) = self.shared.lock().unwrap().read_waker.take() {
                    read_waker.wake();
                }
                Poll::Ready(Ok(buf.len()))
            }
            Err(e) => match e {
                TrySendError::Full(_) => {
                    self.shared.lock().unwrap().write_waker = Some(cx.waker().clone());
                    Poll::Pending
                }
                TrySendError::Closed(_) => Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "receiver closed",
                ))),
            },
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        // TODO: Implement kind of flushing
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        // TODO: Check if there is something to do. Maybe drop sender?
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for PipeRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut write_to_buf = |vec: &[u8]| -> Vec<u8> {
            let end = std::cmp::min(buf.remaining(), vec.len());
            let slice_to_write = &vec[0..end];
            buf.put_slice(slice_to_write);

            let rest_of_vec = &vec[end..];
            rest_of_vec.to_vec()
        };

        if self.read_remaining.len() > 0 {
            self.read_remaining = write_to_buf(&mut self.read_remaining);
            return Poll::Ready(Ok(()));
        }

        match self.receiver.poll_recv(cx) {
            Poll::Ready(v) => match v {
                None => Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "sender closed",
                ))),
                Some(v) => {
                    self.read_remaining = write_to_buf(&v);
                    if let Some(waker) = self.shared.lock().unwrap().write_waker.take() {
                        waker.wake();
                    }
                    Poll::Ready(Ok(()))
                }
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn test_single_write() {
        let (mut reader, mut writer) = pipe(512);

        let to_send = b"hello world";
        writer.write_all(to_send).await.expect("error writing");

        let mut buffer = vec![0u8; to_send.len()];
        reader.read_exact(&mut buffer).await.expect("error reading");

        assert_eq!(&buffer, to_send);
    }

    #[tokio::test]
    async fn test_multi_write() {
        let (mut reader, mut writer) = pipe(512);

        let to_send = b"hello world";
        writer.write_all(b"hello").await.expect("error writing");
        writer.write_all(b" world").await.expect("error writing");

        let mut buffer = vec![0u8; to_send.len()];
        reader.read_exact(&mut buffer).await.expect("error reading");

        assert_eq!(&buffer, to_send);
    }

    #[tokio::test]
    async fn test_write_more_than_buffer() {
        let (mut reader, mut writer) = pipe(2);

        let to_send = b"hello world";
        tokio::spawn(async move {
            writer.write_all(b"hello").await.expect("error writing");
            writer.write_all(b" world").await.expect("error writing");
        });

        let mut buffer = vec![0u8; to_send.len()];
        reader.read_exact(&mut buffer).await.expect("error reading");

        assert_eq!(&buffer, to_send);
    }
}
