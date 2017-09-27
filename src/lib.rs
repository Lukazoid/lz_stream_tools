#[macro_use]
extern crate futures;

use futures::Stream;
use std::hash::Hash;

mod group_by;
pub use group_by::{Group, GroupBy};

pub trait StreamTools: Stream {
    fn group_by<K, F>(self, f: F) -> GroupBy<K, Self, F>
    where
        F: FnMut(&Self::Item) -> K,
        K: Clone + Eq + Hash,
        Self: Sized,
    {
        GroupBy::new(self, f)
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
        let results = vec!["A", "AB", "C", "ABC"];
        let stream = stream::iter_ok::<_, ()>(results);
        let group_by = stream.group_by(|s| s.len());

        let group_keys: Vec<_> = group_by
            .map(|(k, _)| k)
            .collect()
            .wait()
            .expect("there should be no error getting the groups");

        assert_eq!(group_keys, vec![1, 2, 3]);
    }

    #[test]
    fn groups_return_correct_items() {
        let results = vec!["A", "AB", "C", "ABC"];
        let stream = stream::iter_ok::<_, ()>(results);
        let group_by = stream.group_by(|s| s.len());

        let mut groups: Vec<_> = group_by
            .map(|(_, g)| g)
            .collect()
            .wait()
            .expect("there should be no error getting the groups");

        let first_items: Vec<_> = groups
            .remove(0)
            .collect()
            .wait()
            .expect("there should be no error reading items from the group");

        assert_eq!(first_items, vec!["A", "C"]);

        let second_items: Vec<_> = groups
            .remove(0)
            .collect()
            .wait()
            .expect("there should be no error reading items from the group");

        assert_eq!(second_items, vec!["AB"]);

        let third_items: Vec<_> = groups
            .remove(0)
            .collect()
            .wait()
            .expect("there should be no error reading items from the group");

        assert_eq!(third_items, vec!["ABC"]);
    }
}
