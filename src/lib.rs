#[macro_use]
extern crate futures;

use futures::{Async, Poll, Stream};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

struct GroupByState<K, S, F>
    where S: Stream
{
    stream: S,
    callback: F,
    pending_items: Vec<VecDeque<S::Item>>,
    group_indices: HashMap<K, usize>,
    pending_groups: Vec<Result<(K, Group<K, S, F>), S::Error>>,
}

fn poll_next_group_item<K, S, F>(shared_state: &mut Arc<Mutex<GroupByState<K, S, F>>>,
                                 index: usize)
                                 -> Poll<Option<S::Item>, ()>
    where S: Stream,
          K: Clone + Eq + Hash,
          F: FnMut(&S::Item) -> K
{
    let mut state = shared_state.lock().unwrap();

    // Pop an item pending for this group
    if let Some(item) = state.pending_items[index].pop_front() {
        return Ok(Async::Ready(Some(item)));
    }

    // Loop until we find an item for this group or the end of the stream
    loop {
        match state.stream.poll() {
            Ok(Async::Ready(Some(item))) => {
                let key = (&mut state.callback)(&item);

                let existing_index = state.group_indices.get(&key).map(|x| *x);
                match existing_index {
                    Some(existing_index) if existing_index == index => {
                        // We found an item for this group
                        return Ok(Async::Ready(Some(item)));
                    }
                    Some(existing_index) => {
                        // Found an item for another group
                        state.pending_items[existing_index].push_back(item);
                        continue;
                    }
                    None => {
                        // Found an item for a new group
                        let index = state.pending_items.len();

                        let mut group_pending_items = VecDeque::new();
                        group_pending_items.push_back(item);

                        state.pending_items.push(group_pending_items);
                        state.group_indices.insert(key.clone(), index);

                        let group = Group {
                            index: index,
                            state: shared_state.clone(),
                        };

                        state.pending_groups.push(Ok((key, group)));
                        continue;
                    }
                }
            }
            Err(err) => {
                // If an error occurred, store it for the parent GroupBy to send
                state.pending_groups.push(Err(err));

                // After an error there are
                return Ok(Async::Ready(None));
            }
            Ok(async_state) => return Ok(async_state),
        }
    }

}

fn poll_next_group<K, S, F>(shared_state: &mut Arc<Mutex<GroupByState<K, S, F>>>)
                            -> Poll<Option<(K, Group<K, S, F>)>, S::Error>
    where S: Stream,
          K: Clone + Eq + Hash,
          F: FnMut(&S::Item) -> K
{
    let mut state = shared_state.lock().unwrap();

    // Pop a pending group
    if let Some(pending_group_result) = state.pending_groups.pop() {
        return pending_group_result.map(|pending_group| Async::Ready(Some(pending_group)));
    }

    // loop until we find the next group or the end of the stream
    loop {
        let item = try_ready!(state.stream.poll());

        match item {
            None => return Ok(Async::Ready(None)),
            Some(item) => {
                let key = (&mut state.callback)(&item);
                if let Some(existing_index) = state.group_indices.get(&key).map(|x| *x) {
                    // Found an existing group, add this item to its list of pending
                    state.pending_items[existing_index].push_back(item);

                    // We already have a group for this key keep looping until we find the next group
                    continue;

                } else {
                    // Found an item for a new group
                    let index = state.pending_items.len();
                    let mut group_pending_items = VecDeque::new();
                    group_pending_items.push_back(item);

                    state.pending_items.push(group_pending_items);

                    state.group_indices.insert(key.clone(), index);

                    let group = Group {
                        index: index,
                        state: shared_state.clone(),
                    };
                    return Ok(Async::Ready(Some((key, group))));
                }
            }
        };
    }
}

pub struct GroupBy<K, S, F>
    where S: Stream
{
    state: Arc<Mutex<GroupByState<K, S, F>>>,
}

pub struct Group<K, S, F>
    where S: Stream
{
    index: usize,
    state: Arc<Mutex<GroupByState<K, S, F>>>,
}

impl<K, S, F> Stream for Group<K, S, F>
    where S: Stream,
          K: Clone + Eq + Hash,
          F: FnMut(&S::Item) -> K
{
    type Item = S::Item;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        poll_next_group_item(&mut self.state, self.index)
    }
}

impl<K, S, F> Stream for GroupBy<K, S, F>
    where S: Stream,
          K: Clone + Eq + Hash,
          F: FnMut(&S::Item) -> K
{
    type Item = (K, Group<K, S, F>);
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        poll_next_group(&mut self.state)
    }
}

pub trait StreamTools: Stream {
    fn group_by<K, F>(self, f: F) -> GroupBy<K, Self, F>
        where F: FnMut(&Self::Item) -> K,
              K: Clone + Eq + Hash,
              Self: Sized
    {
        GroupBy {
            state: Arc::new(Mutex::new(GroupByState {
                stream: self,
                callback: f,
                pending_items: Default::default(),
                group_indices: Default::default(),
                pending_groups: Default::default(),
            })),
        }
    }
}

impl<S: Stream> StreamTools for S {}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{Future, Stream};
    use futures::stream;

    #[test]
    fn group_by_returns_each_group() {
        let results: Vec<Result<_, ()>> = vec![Ok("A"), Ok("AB"), Ok("C"), Ok("ABC")];
        let stream = stream::iter(results);
        let group_by = stream.group_by(|s| s.len());

        let group_keys: Vec<_> = group_by.map(|(k, _)| k)
            .collect()
            .wait()
            .expect("there should be no error getting the groups");

        assert_eq!(group_keys, vec![1, 2, 3]);
    }

    #[test]
    fn groups_return_correct_items() {
        let results: Vec<Result<_, ()>> = vec![Ok("A"), Ok("AB"), Ok("C"), Ok("ABC")];
        let stream = stream::iter(results);
        let group_by = stream.group_by(|s| s.len());

        let mut groups: Vec<_> = group_by.map(|(_, g)| g)
            .collect()
            .wait()
            .expect("there should be no error getting the groups");

        let first_items: Vec<_> = groups.remove(0)
            .collect()
            .wait()
            .expect("there should be no error reading items from the group");

        assert_eq!(first_items, vec!["A", "C"]);

        let second_items: Vec<_> = groups.remove(0)
            .collect()
            .wait()
            .expect("there should be no error reading items from the group");

        assert_eq!(second_items, vec!["AB"]);

        let third_items: Vec<_> = groups.remove(0)
            .collect()
            .wait()
            .expect("there should be no error reading items from the group");

        assert_eq!(third_items, vec!["ABC"]);
    }
}
