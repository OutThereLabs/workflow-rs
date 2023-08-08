#[macro_use]
extern crate derive_builder;

pub mod data;
mod in_memory;
mod registry;
pub mod state;
pub mod task;
pub mod workflow;

#[cfg(feature = "temporal")]
mod temporal_executor;

#[cfg(test)]
mod tests;

#[cfg(feature = "macros")]
pub use workflow_rs_macros as macros;

pub mod prelude {
    pub use super::registry::{WorkflowRegistry, WorkflowRegistryBuilder};
    pub use super::workflow::WorkflowContext;
}
