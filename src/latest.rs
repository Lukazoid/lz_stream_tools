use futures::{Async, Poll, Stream};

pub struct Latest<S: Stream> {
    stream: S,
}

impl<S: Stream> Latest<S> {
    pub(crate) fn new(stream: S) -> Self {
        Latest { stream: stream }
    }
}

impl<S: Stream> Stream for Latest<S> {
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut latest = match try_ready!(self.stream.poll()) {
            Some(value) => value,
            None => return Ok(Async::Ready(None)),
        };

        loop {
            match self.stream.poll() {
                Ok(Async::Ready(Some(value))) => latest = value,
                Ok(Async::Ready(None)) | Ok(Async::NotReady) => break,
                err @ Err(_) => return err,
            }
        }

        Ok(Async::Ready(Some(latest)))
    }
}
