use futures::{Async, Poll, Stream};
use futures::stream::Fuse;
use {Latest, StreamTools};
use std::mem;

enum WithLatestFromState<S: Stream, O: Stream> {
    InProgress {
        stream: S,
        other: O,
        latest_value: Option<O::Item>,
    },
    Done,
}

pub struct WithLatestFrom<S: Stream, O: Stream> {
    state: WithLatestFromState<Fuse<S>, Fuse<Latest<O>>>,
}

impl<S: Stream, O: Stream> WithLatestFrom<S, O> {
    pub(crate) fn new(stream: S, other: O) -> Self {
        WithLatestFrom {
            state: WithLatestFromState::InProgress {
                stream: stream.fuse(),
                other: other.latest().fuse(),
                latest_value: None,
            },
        }
    }
}

impl<S: Stream, O: Stream<Error = S::Error>> Stream for WithLatestFrom<S, O>
where
    O::Item: Clone,
{
    type Item = (S::Item, O::Item);
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
        match mem::replace(&mut self.state, WithLatestFromState::Done) {
            WithLatestFromState::InProgress {
                mut stream,
                mut other,
                latest_value,
            } => match (stream.poll(), other.poll(), latest_value) {
                (Err(error), _, _) |
                (_, Err(error), _) => {
                    // If either stream errors, return the error
                    return Err(error);
                },
                (Ok(Async::Ready(None)), _, _) => {
                    // If we reach the end of the main stream stop any further items
                    return Ok(None.into());
                },
                (Ok(Async::Ready(Some(left_value))), Ok(Async::Ready(None)), Some(latest_value)) |
                (Ok(Async::Ready(Some(left_value))), Ok(Async::Ready(Some(latest_value))), _) |
                (Ok(Async::Ready(Some(left_value))), Ok(Async::NotReady), Some(latest_value)) => {
                    // Every time we get a value from the main stream combine it with the latest value
                    // which could be from the latest values stream or our cached value
                    self.state = WithLatestFromState::InProgress {
                        stream: stream,
                        other: other,
                        latest_value: Some(latest_value.clone()),
                    };

                    return Ok(Some((left_value, latest_value)).into());
                },
                (Ok(Async::NotReady), Ok(Async::Ready(latest_value)), _) |
                (Ok(Async::NotReady), Ok(Async::NotReady), latest_value) | 
                (Ok(Async::Ready(Some(_))), Ok(Async::NotReady), latest_value @ None) => {
                    // If the main stream isn't ready or we don't yet have a latest value
                    self.state = WithLatestFromState::InProgress {
                        stream: stream,
                        other: other,
                        latest_value: latest_value,
                    };

                    return Ok(Async::NotReady);
                }, 
                (Ok(Async::Ready(Some(_))), Ok(Async::Ready(None)), None) => {
                    // If the other stream never gave us any values we will keep looping through just
                    // the main stream
                    self.state = WithLatestFromState::InProgress {
                        stream: stream,
                        other: other,
                        latest_value: None,
                    };
                }
            },
            WithLatestFromState::Done => return Ok(None.into()),
        }
        }
    }
}
