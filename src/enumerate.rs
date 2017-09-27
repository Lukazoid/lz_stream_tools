use futures::{Async, Poll, Stream};

pub struct Enumerate<S: Stream> {
    stream: S,
    i: usize,
}

impl<S: Stream> Enumerate<S> {
    pub(crate) fn new(stream: S) -> Self {
        Enumerate {
            stream: stream,
            i: 0,
        }
    }
}

impl<S: Stream> Stream for Enumerate<S> {
    type Item = (usize, S::Item);
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let next = try_ready!(self.stream.poll());

        let item = next.map(|x| {
            let index = self.i;
            self.i = index + 1;
            (index, x)
        });

        Ok(Async::Ready(item))
    }
}
