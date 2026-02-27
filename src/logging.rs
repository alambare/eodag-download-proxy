use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Initialise the `tracing` subscriber.
///
/// Log level is controlled by the `RUST_LOG` environment variable.
/// Falls back to `info` when unset.
pub fn init() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
