use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io::{Seek, SeekFrom},
    ops::{Deref, DerefMut},
};

use serde::{Deserialize, Serialize};

use crate::error::{CogtainerError, HeaderError};

mod footer;
mod header;
mod overallocation;

pub use footer::*;
pub use header::*;
pub use overallocation::*;

// ContainerFile is basically like a zip or tar file, but explicitly supports replacing
// and deleting "blocks". A block is a similar concept to a file. It can have arbitrary
// data within it. When a block is mutated, if there isn't enough free adjacent space to
// hold the new revision of it, the block will be freed and moved to the end of the file.
// The empty space of the old block location is added to the empty_space list (which
// automatically consolidates adjacent areas of empty space). When a new block is requested,
// this space is checked first in an attempt to minimize fragmentation.

// Typically, when a block is created, some additional space will be allocated so there's
// room to grow. This is configurable in the API.

// Compression is not built into this container format. Instead, individual blocks should
// manage their own compression. Domain-specific details may be freely encoded in the
// metadata field.

// Data format is little-endian.

// The file layout is organized into 3 parts:
// - Header: Magic Number, Version, Footer Offset, Footer Checksum
// - Block Data
// - Footer

// The Magic Number is a fixed string defining the file format ("DCCF")
// The version is a u64 number indicating the current version (1)
// The Footer Offset is a u64 number in bytes indicating the start offset of the footer

// The rest of the data up to the Footer Offset is the block data. The particular format of this data
// is defined in the footer (allocated or empty)

// The footer provides metadata about the file data, as well as the layout of the block data region.
// This includes
// - actively used blocks
// - empty space that can be used for expansion/reallocation

pub const DCCF_MAGIC: [u8; 4] = *b"DCCF";
fn calc_checksum(bytes: &[u8]) -> Checksum {
    Checksum(twox_hash::XxHash64::oneshot(4321, bytes))
}

/// Unique identifier for a block.
/// This could take on the form of a file path, or some domain-relevant id.
#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[serde(untagged)]
pub enum Identifier {
    String(String),
    U64(u64),
    Bytes(Vec<u8>),
    Path(Vec<Self>),
}

#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
#[serde(transparent)]
pub struct FileOffset(pub u64);
impl FileOffset {
    pub fn end_offset(&self, len: u64) -> Self {
        Self(self.0 + len)
    }
}
impl Deref for FileOffset {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for FileOffset {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, Eq, Clone, Copy)]
#[serde(transparent)]
pub struct Checksum(pub u64);
impl Deref for Checksum {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Checksum {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
