use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io::{Seek, SeekFrom},
    ops::{Deref, DerefMut},
};

use serde::{Deserialize, Serialize};

use crate::error::{CogtainerError, HeaderError};

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockDescriptor {
    pub file_offset: FileOffset,
    pub used_length: u64,
    pub allocated_length: u64,
    pub checksum: Checksum,
    pub metadata: rmpv::Value,
}

/// Maintains the metadata and overall structure of the file. This includes occupied blocks and empty space.
#[derive(Serialize, Deserialize)]
pub struct ContainerFooter {
    /// Domain-specific information relevant for this file.
    pub metadata: rmpv::Value,

    /// The value is the offset location within the file where the block is stored. Blocks are
    /// not in any guaranteed order.
    pub blocks: HashMap<Identifier, BlockDescriptor>,

    /// When a block is removed (or moved to the end if it's too big), its
    /// space is merged into the empty_space list for use when another block is needed or
    /// to ease defragmenting. (neighboring BlockDescriptors are merged together)
    pub empty_space: BTreeMap<FileOffset, u64>,
}
/// ContainerFooter functions related to writing.
impl ContainerFooter {
    pub(crate) fn create<W: std::io::Write + std::io::Seek>(
        writer: &mut W,
        header: &mut ContainerHeader,
    ) -> Result<Self, CogtainerError> {
        let me = Self {
            metadata: rmpv::Value::Nil,
            blocks: HashMap::new(),
            empty_space: BTreeMap::new(),
        };
        me.write_to(writer, header)?;

        Ok(me)
    }
    /// Writes this footer to the given writer.
    /// Updates the header with the footer's length, and writes that to the file as well.
    pub fn write_to<W: std::io::Write + std::io::Seek>(
        &self,
        writer: &mut W,
        header: &mut ContainerHeader,
    ) -> Result<(), CogtainerError> {
        let initial_position = writer.stream_position()?;
        writer.seek(SeekFrom::Start(header.footer_offset.0))?;

        let bytes = rmp_serde::to_vec(&self)?;
        let calc_checksum = calc_checksum(bytes.as_slice());

        header.footer_length = bytes.len() as u64;
        header.footer_checksum = calc_checksum;

        writer.write_all(&bytes)?;

        // Since the header was updated, and the footer was just written, write the header too
        header.write_to(writer)?;

        writer.seek(SeekFrom::Start(initial_position))?;
        Ok(())
    }
    /// Updates the metadata for the given block.
    /// If the block doesn't exist, it is added with a length of 0.
    pub fn update_block_metadata<W: std::io::Write + std::io::Seek>(
        &mut self,
        writer: &mut W,
        header: &mut ContainerHeader,
        identifier: Identifier,
        metadata: rmpv::Value,
    ) -> Result<(), CogtainerError> {
        if let Some(descriptor) = self.blocks.get_mut(&identifier) {
            descriptor.metadata = metadata;
        } else {
            let descriptor = BlockDescriptor {
                file_offset: FileOffset(0),
                used_length: 0,
                allocated_length: 0,
                checksum: Checksum(0),
                metadata,
            };
            self.blocks.insert(identifier, descriptor);
        }
        self.write_to(writer, header)?;
        return Ok(());
    }
    /// Reserves the requested space and returns the FileOffset and length
    /// - If there is space available in empty_space, removes from there and returns.
    /// - If not, then reserves at the footer's current address and updates the header with the new position after the reserved space.
    fn reserve_space(
        &mut self,
        header: &mut ContainerHeader,
        required_length: u64,
        policy: OverallocationPolicy,
    ) -> (FileOffset, u64) {
        let mut found_space = None;
        for (file_offset, len) in self.empty_space.iter() {
            // if the empty area is at least as large as required, then use it.
            if len >= &required_length {
                found_space = Some(*file_offset);
                break;
            }
        }
        if let Some(offset) = found_space {
            if let Some(available_len) = self.empty_space.remove(&offset) {
                // take only what's needed
                let left_over = available_len - required_length;
                if left_over > 0 {
                    let end_address = offset.0 + required_length;
                    // add leftover space back to empty_space list
                    self.empty_space.insert(FileOffset(end_address), left_over);
                }
                return (offset, required_length);
            }
        }
        let offset = header.footer_offset;
        // only use overallocation policy when we have to move the footer
        let required_length = policy.calculate(required_length);

        // move the header to after the new block
        let new_footer_offset = offset.end_offset(required_length);
        header.footer_offset = new_footer_offset;

        (offset, required_length)
    }

    /// Adds the given block (or replaces it if it already exists).
    pub fn insert_block<W: std::io::Write + std::io::Seek>(
        &mut self,
        writer: &mut W,
        header: &mut ContainerHeader,
        policy: OverallocationPolicy,
        identifier: &Identifier,
        metadata: rmpv::Value,
        data: &[u8],
    ) -> Result<(), CogtainerError> {
        let checksum = calc_checksum(data);
        // always remove the old block. This gives the opportunity to consolidate empty space and simplifies the overall logic in this section.
        if let Some(descriptor) = self.blocks.remove(identifier) {
            if descriptor.allocated_length > 0 {
                self.empty_space
                    .insert(descriptor.file_offset, descriptor.allocated_length);
                self.consolidate_empty_space();
            }
        }
        // write the data
        if data.len() > 0 {
            // find new empty space
            let (insert_file_offset, allocated_length) =
                self.reserve_space(header, data.len() as u64, policy);

            // update the footer with the new offset/metadata
            self.blocks.insert(
                identifier.clone(),
                BlockDescriptor {
                    file_offset: insert_file_offset,
                    used_length: data.len() as u64,
                    allocated_length,
                    checksum,
                    metadata,
                },
            );

            writer.seek(SeekFrom::Start(insert_file_offset.0))?;
            writer.write_all(data)?;
            // fill remaining space with zeros
            if data.len() < allocated_length as usize {
                let zeros = vec![0u8; allocated_length as usize - data.len()];
                writer.write_all(&zeros)?;
            }
        } else {
            // no data block
            self.blocks.insert(
                identifier.clone(),
                BlockDescriptor {
                    file_offset: FileOffset(0),
                    used_length: 0,
                    allocated_length: 0,
                    checksum,
                    metadata,
                },
            );
        }
        // write the footer (which also writes the header)
        self.write_to(writer, header)
    }

    /// Deletes the specified block. Returns an error if the block doesn't exist.
    /// Adds the block to the empty space list.
    /// Note: Does not defragment or shrink the file.
    /// Note: Does not flush/write to disk.
    pub fn delete_block<W: std::io::Write + std::io::Seek>(
        &mut self,
        writer: &mut W,
        header: &mut ContainerHeader,
        identifier: &Identifier,
    ) -> Result<BlockDescriptor, CogtainerError> {
        if let Some(descriptor) = self.blocks.remove(&identifier) {
            self.empty_space
                .insert(descriptor.file_offset, descriptor.allocated_length);
            self.consolidate_empty_space();
            Ok(descriptor)
        } else {
            Err(CogtainerError::BlockNotFound(identifier.clone()))
        }
    }

    // Iterates through empty space, consolidating adjacent empty blocks
    pub(crate) fn consolidate_empty_space(&mut self) {
        let mut empty: Vec<_> = self.empty_space.clone().into_iter().collect();
        empty.sort_by_key(|&(offset, _)| offset);

        let mut merged = Vec::with_capacity(empty.len());
        let mut iter = empty.into_iter().peekable();

        while let Some((offset, len)) = iter.next() {
            let mut current_offset = offset;
            let mut current_len = len;

            while let Some(&(next_offset, next_len)) = iter.peek() {
                if current_offset.0 + current_len == next_offset.0 {
                    // Merge contiguous regions
                    current_len += next_len;
                    iter.next();
                } else {
                    break;
                }
            }
            merged.push((current_offset, current_len));
        }
        merged.sort();
        self.empty_space = merged.into_iter().collect();
    }
}
/// ContainerFooter functions related to reading.
impl ContainerFooter {
    /// Read the footer from the given reader with the pre-fetched header
    pub fn read_from<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        header: &ContainerHeader,
    ) -> Result<Self, CogtainerError> {
        reader.seek(SeekFrom::Start(header.footer_offset.0))?;
        let mut footer_bytes = vec![0u8; header.footer_length as usize];
        reader.read_exact(&mut footer_bytes)?;
        let calc_checksum = calc_checksum(footer_bytes.as_slice());
        if calc_checksum != header.footer_checksum {
            return Err(CogtainerError::FooterChecksumError);
        }

        let footer = rmp_serde::from_slice(footer_bytes.as_slice())?;
        Ok(footer)
    }
    pub fn get_block_metadata<R: std::io::Read + std::io::Seek>(
        &self,
        reader: &mut R,
        identifier: &Identifier,
    ) -> Option<&rmpv::Value> {
        self.blocks.get(identifier).map(|bd| &bd.metadata)
    }

    /// Retrive the specified block from the file as raw bytes
    pub fn get_block<R: std::io::Read + std::io::Seek>(
        &self,
        reader: &mut R,
        identifier: &Identifier,
    ) -> Result<(&rmpv::Value, Vec<u8>), CogtainerError> {
        let descriptor = self
            .blocks
            .get(identifier)
            .ok_or_else(|| CogtainerError::BlockNotFound(identifier.clone()))?;

        // there is no block data, return an empty vec
        if descriptor.allocated_length == 0 {
            return Ok((&descriptor.metadata, vec![]));
        }
        reader.seek(SeekFrom::Start(descriptor.file_offset.0))?;

        let mut bytes = vec![0u8; descriptor.used_length as usize];

        reader.read_exact(&mut bytes)?;
        let calc_checksum = calc_checksum(bytes.as_slice());
        if calc_checksum != descriptor.checksum {
            return Err(CogtainerError::BlockChecksumError(identifier.clone()));
        }

        Ok((&descriptor.metadata, bytes))
    }
}
