//! M2 domain contracts. These types never contain credential secret material.

pub mod adapter;
pub mod approval;
pub mod catalog;
pub mod deliverable;
pub mod graph;
pub mod identity;
pub mod m3;
pub mod meeting;
pub mod mvp;
pub mod office;
pub mod policy;
pub mod process_environment;
pub mod workspace;

pub use adapter::*;
pub use approval::*;
pub use catalog::*;
pub use deliverable::*;
pub use graph::*;
pub use identity::*;
pub use m3::*;
pub use meeting::*;
pub use mvp::*;
pub use office::*;
pub use policy::*;
pub use process_environment::*;
pub use workspace::*;
