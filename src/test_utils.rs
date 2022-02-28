use std::cmp::Ordering;

use crate::{
    buffered_logs::GroupedHttpLogs,
    reader::{HttpLog, LogRequest},
};

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
                eprintln!("ASSERT FAILED AT {:?} - {:?}", last, curr);
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

pub fn build_test_http_log(time: usize, path: Option<String>) -> HttpLog {
    HttpLog {
        remote_host: "10.1.1.1".to_string(),
        auth_user: "auth_user".to_string(),
        rfc931: "-".to_string(),
        time,
        request: LogRequest {
            verb: "GET".to_string(),
            path: path.unwrap_or("/api/test".to_string()),
            section: "/api".to_string(),
            protocol: "HTTP/1.1".to_string(),
        },
        status: 200,
        bytes: 100,
    }
}

pub fn build_test_http_grouped_log(
    time: usize,
    len: usize,
    path: Option<String>,
) -> GroupedHttpLogs {
    GroupedHttpLogs {
        time,
        logs: (0..len)
            .map(|_| build_test_http_log(time, path.clone()))
            .collect(),
    }
}
