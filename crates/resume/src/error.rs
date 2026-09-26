use std::time::Duration;

use crate::{Permanent, RunFailed, Snooze, Stopping};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    Permanent,
    Snooze(Duration),
    Stopping,
    RunFailed,
    ClaimLost,
    TransientDatabase,
    Other,
}

/// The outermost recognized error decides policy; contextual wrappers preserve it
/// by exposing their cause through `Error::source`.
pub(crate) fn classify(error: &(dyn std::error::Error + 'static)) -> ErrorKind {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if error.is::<Permanent>() {
            return ErrorKind::Permanent;
        }
        if let Some(Snooze(delay)) = error.downcast_ref::<Snooze>() {
            return ErrorKind::Snooze(*delay);
        }
        if error.is::<Stopping>() {
            return ErrorKind::Stopping;
        }
        if error.is::<RunFailed>() {
            return ErrorKind::RunFailed;
        }
        if let Some(code) = error
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(tokio_postgres::Error::code)
        {
            return match code.code() {
                "RS002" => ErrorKind::ClaimLost,
                // Serialization failure, deadlock, lock timeout/unavailable, query cancellation.
                "40001" | "40P01" | "55P03" | "57014" => ErrorKind::TransientDatabase,
                _ => ErrorKind::Other,
            };
        }
        cause = error.source();
    }
    ErrorKind::Other
}
