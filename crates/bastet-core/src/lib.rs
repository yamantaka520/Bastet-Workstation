//! M2 domain contracts. These types never contain credential secret material.

pub mod adapter;
pub mod approval;
pub mod catalog;
pub mod graph;
pub mod identity;
pub mod meeting;
pub mod office;
pub mod policy;
pub mod workspace;

pub use adapter::*;
pub use approval::*;
pub use catalog::*;
pub use graph::*;
pub use identity::*;
pub use meeting::*;
pub use office::*;
pub use policy::*;
pub use workspace::*;
