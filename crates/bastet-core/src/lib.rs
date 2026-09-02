//! M2 domain contracts. These types never contain credential secret material.

pub mod adapter;
pub mod catalog;
pub mod identity;
pub mod policy;

pub use adapter::*;
pub use catalog::*;
pub use identity::*;
pub use policy::*;
