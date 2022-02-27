use std::cmp::Ordering;

/// Checks if a given iterator is sorted
pub fn is_sorted_by<T: Iterator, F>(mut collection: T, compare: F) -> bool
where
    T: Sized,
    T::Item: std::fmt::Debug,
    F: FnMut(&T::Item, &T::Item) -> Option<Ordering>,
{
    #[inline]
    fn check<'a, T: std::fmt::Debug>(
        last: &'a mut T,
        mut compare: impl FnMut(&T, &T) -> Option<Ordering> + 'a,
    ) -> impl FnMut(T) -> bool + 'a {
        move |curr| {
            if let Some(Ordering::Greater) | None = compare(&last, &curr) {
                return false;
            }
            *last = curr;
            true
        }
    }

    let mut last = match collection.next() {
        Some(e) => e,
        None => return true,
    };

    collection.all(check(&mut last, compare))
}
