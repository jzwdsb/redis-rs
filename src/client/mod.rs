mod async_cli;
mod sync_cli;

// block client
// the io operation will block the current thread from executing
pub use async_cli::AsyncClient;
pub use sync_cli::BlockClient;

mod tests {
    #[allow(unused_imports)]
    use super::*;
}
