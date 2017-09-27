use futures::{Async, Poll, Stream};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

struct GroupByState<K, S, F>
where
    S: Stream,
{
    stream: S,
    callback: F,

    // The pending items of each group, this will map to None if the group has been dropped
    pending_items: Vec<Option<VecDeque<S::Item>>>,

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
    match state.pending_items[index] {
        Some(ref mut pending_items) => {
            if let Some(item) = pending_items.pop_front() {
                return Ok(Async::Ready(Some(item)));
            }
        }
        None => {
            unreachable!("attempting to get the next group item for a dropped group");
        }
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
                        match state.pending_items.get_mut(existing_index).map(|x| x.as_mut()) {
                            Some(Some(pending_items)) => {
                                pending_items.push_back(item);
                            }
                            Some(None) => {
                                // Found an item for a dropped group
                            },
                            None => unreachable!("there should always be a pending_items entry for each group")
                        };
                        continue;
                    }
                    None => {
                        // Found an item for a new group
                        let index = state.pending_items.len();

                        let mut group_pending_items = VecDeque::new();
                        group_pending_items.push_back(item);

                        state.pending_items.push(Some(group_pending_items));
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
                match  state.group_indices.get(&key).map(|x| *x) {
                    Some(existing_index) => {
                        // Found an existing group, add this item to its list of pending
                        if let Some(ref mut pending_items) = state.pending_items[existing_index] {
                            pending_items.push_back(item);
                        }

                        // We already have a group for this key keep looping until we find the next group
                        continue;

                    },
                    _ => {
                        // Found an item for a new group
                        let index = state.pending_items.len();
                        let mut group_pending_items = VecDeque::new();
                        group_pending_items.push_back(item);

                        state.pending_items.push(Some(group_pending_items));

                        state.group_indices.insert(key.clone(), index);

                        let group = Group {
                            index: index,
                            state: shared_state.clone(),
                        };
                        return Ok(Async::Ready(Some((key, group))));
                    }
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

impl<K, S: Stream, F> Drop for Group<K, S, F> {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();

        state.pending_items[self.index] = None;
    }
}

impl<K: Eq + Hash + Clone, S:Stream, F: FnMut(&S::Item) -> K> GroupBy<K, S, F> {
    pub(crate) fn new(stream: S, callback: F) -> Self {
        GroupBy {
            state: Arc::new(Mutex::new(GroupByState {
                stream: stream,
                callback: callback,
                pending_items: Default::default(),
                group_indices: Default::default(),
                pending_groups: Default::default(),
            })),
        }
    }
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
