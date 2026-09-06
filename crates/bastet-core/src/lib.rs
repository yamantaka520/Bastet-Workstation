//! M2 domain contracts. These types never contain credential secret material.

pub mod adapter;
pub mod approval;
pub mod catalog;
pub mod identity;
pub mod policy;
pub mod workspace;

pub use adapter::*;
pub use approval::*;
pub use catalog::*;
pub use identity::*;
pub use policy::*;
pub use workspace::*;
