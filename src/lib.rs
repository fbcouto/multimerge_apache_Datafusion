//! Sort operator for Apache DataFusion backed by the Adaptive Parallel MultiMerge Sort engine.
//!
//! - [`MultimergeSortExec`] is the physical operator: it reads its input partitions and returns
//!   one sorted partition.
//! - [`MultimergeSortRule`] is the physical optimizer rule that puts the operator in the place of
//!   `SortExec` and `SortPreservingMergeExec` in a full sort (one without a limit).
//! - [`session_state`] builds a session with the rule installed.
//!
//! ```no_run
//! use datafusion::prelude::{SessionConfig, SessionContext};
//! use multimerge_datafusion::{session_state, Mode};
//!
//! let ctx = SessionContext::new_with_state(session_state(SessionConfig::new(), Mode::WholeSort));
//! ```

// A copy of the engine, src/multimerge.rs of the adaptive-parallel-multimerge-sort repository
// without its tests: the original holds the tests and is the source of truth. It is kept exactly as
// it is there, so neither rustfmt nor clippy touch it here.
#[rustfmt::skip]
#[allow(dead_code, clippy::all)]
mod engine;

pub mod exec;
pub mod rule;
pub mod sort;

pub use exec::MultimergeSortExec;
pub use rule::{Mode, MultimergeSortRule, optimizer_rules, session_state};
pub use sort::{Gather, SortPath, SortStats, Tuning, sort_batches, sort_batches_with};
