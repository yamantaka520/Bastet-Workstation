//! M2 domain contracts. These types never contain credential secret material.

pub mod adapter;
pub mod identity;
pub mod policy;

pub use adapter::*;
pub use identity::*;
pub use policy::*;
