use std::{
    io::{Read, Seek, Write},
    ops::Deref,
};

use rmpv::Value;
use serde::Serialize;

use crate::{basic_api::Cogtainer, container_file::Identifier, error::CogtainerError};

pub struct Cogtainer<F> {
    basic: Cogtainer<F>,
}
impl<F> Deref for Cogtainer<F> {
    type Target = Cogtainer<F>;

    fn deref(&self) -> &Self::Target {
        &self.basic
    }
}
impl<F: Seek + Write + Read> Cogtainer<F> {
    pub fn open(file: F) -> Result<Self, CogtainerError> {
        Ok(Self {
            basic: Cogtainer::open(file)?,
        })
    }

    /// Inserts a block with the given unique identifier.
    /// If a block already exists with the given identifier, it will be replaced.
    pub fn insert<M: Serialize, D: Serialize>(
        &mut self,
        identifier: Identifier,
        data: &D,
        metadata: &M,
    ) -> Result<(), CogtainerError> {
        let data = rmp_serde::to_vec(data)?;
        let metadata = rmp_serde::to_vec(metadata)?;
        Ok(())
    }
}
