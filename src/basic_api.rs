use std::{
    collections::HashMap,
    io::{Read, Seek, Write},
};

use flate2::Compression;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    container_file::{
        BlockDescriptor, ContainerFooter, ContainerHeader, Identifier, OverallocationPolicy,
    },
    error::CogtainerError,
    traits::Truncate,
};

#[derive(Debug)]
pub struct Cogtainer<F> {
    pub(crate) file: F,
    pub(crate) header: ContainerHeader,
    pub(crate) footer: ContainerFooter,

    pub(crate) overallocation_policy: OverallocationPolicy,
}
//#[cfg(test)]
impl<F> Cogtainer<F> {
    pub fn get_inner_file(&mut self) -> &mut F {
        &mut self.file
    }
}

impl<F: Seek + Read> Cogtainer<F> {
    pub fn open(mut file: F) -> Result<Self, CogtainerError> {
        // check format and header for compatibility before opening.
        let header = ContainerHeader::read_from(&mut file)?;
        let footer = ContainerFooter::read_from(&mut file, &header)?;
        Ok(Self {
            file,
            header,
            footer,
            overallocation_policy: OverallocationPolicy::default(),
        })
    }

    /// Returns the actual used size of the data in this container, from the header to the end of the footer.
    /// This can be used to truncate files after defragementing.
    pub fn file_length(&self) -> u64 {
        self.header.file_length()
    }
    /// Get the file metadata from the footer
    pub fn get_container_metadata(&self) -> &rmpv::Value {
        &self.footer.metadata
    }

    /// Return a list of all occupied block identifiers with their metadata
    pub fn get_blocks_list(&self) -> &HashMap<Identifier, BlockDescriptor> {
        &self.footer.blocks
    }

    /// Get the data of a specific block.
    /// Returns Err if block not found.
    pub fn get_block(
        &mut self,
        identifier: &Identifier,
    ) -> Result<(&rmpv::Value, Vec<u8>), CogtainerError> {
        self.footer.get_block(&mut self.file, identifier)
    }
}

impl<F: Seek + Write> Cogtainer<F> {
    /// Creates a new Cogtainer file, initializing the header and footer
    pub fn create(mut file: F) -> Result<Self, CogtainerError> {
        let (header, footer) = ContainerHeader::create(&mut file)?;
        Ok(Self {
            file,
            header,
            footer,
            overallocation_policy: OverallocationPolicy::default(),
        })
    }
    /// Configure optional overallocation to decrease chance that updating a block will require moving the footer and growing the file.
    pub fn set_overallocation_policy(&mut self, policy: OverallocationPolicy) -> &mut Self {
        self.overallocation_policy = policy;
        self
    }
    /// Flush any pending changes to the file and flush the file
    pub fn flush(&mut self) -> Result<&mut Self, CogtainerError> {
        self.footer.write_to(&mut self.file, &mut self.header)?;
        self.file.flush()?;
        Ok(self)
    }

    /// Updates container-wide metadata
    pub fn set_metadata(&mut self, value: rmpv::Value) -> Result<&mut Self, CogtainerError> {
        self.footer.metadata = value;

        self.flush()
    }

    /// Inserts a block with the given unique identifier.
    /// If a block already exists with the given identifier, it will be replaced.
    ///
    /// (Requires a call to flush() to persist changes)
    pub fn insert_block(
        &mut self,
        identifier: &Identifier,
        metadata: rmpv::Value,
        data: &[u8],
    ) -> Result<&mut Self, CogtainerError> {
        self.footer.insert_block(
            &mut self.file,
            &mut self.header,
            self.overallocation_policy,
            identifier,
            metadata,
            data,
        )?;
        Ok(self)
    }
    /// Delete the specified block.
    /// (Requires a call to flush() to persist changes)
    pub fn delete_block(&mut self, identifier: &Identifier) -> Result<&mut Self, CogtainerError> {
        self.footer.delete_block(identifier)?;
        Ok(self)
    }
}
impl<F: Seek + Read + Write + Truncate> Cogtainer<F> {
    pub fn defragment_then_truncate(&mut self) -> Result<&mut Self, CogtainerError> {
        self.defragment()?;
        let length = self.file_length();
        if let Err(_err) = self.file.truncate(length) {
            return Err(CogtainerError::IOError(std::io::Error::other(
                "Unable to truncate file",
            )));
        }
        Ok(self)
    }
}
#[cfg(feature = "full")]
impl<F: Seek + Read + Write> Cogtainer<F> {
    /// Consolidates all blocks to remove all empty space.
    ///
    /// 1. If there is no empty space, returns success
    /// 2. Loop
    /// 2.1. Consolidate and sort empty space list
    /// 2.2. Get the first empty block
    /// 2.3. Get the next used block following the selected empty block. If none found, break.
    /// 2.4. Write the block into the start of the empty space.
    /// 2.5. Add the leftover to the empty list
    /// At this point, all blocks are consolidated, followed by one empty block, followed by the footer
    /// 3. Move the footer to the empty location.
    ///
    /// Note: does not truncate the end of the file.
    pub fn defragment(&mut self) -> Result<&mut Self, CogtainerError> {
        if self.footer.empty_space.is_empty() {
            return Ok(self);
        }

        loop {
            // ensure all free space blocks are consolidated and sorted
            self.footer.consolidate_empty_space();

            // ensure all blocks are sorted.
            // let mut blocks: Vec<_> = self.footer.blocks.iter().collect();
            // blocks.sort_by(|a, b| a.1.file_offset.cmp(&b.1.file_offset));

            // let (empty_offset, empty_len) = match self.footer.empty_space.first_entry() {
            //     Some(e) => e.remove_entry(),
            //     None => {
            //         // there is no empty space
            //         return Ok(self);
            //     }
            // };
            let (empty_offset, _empty_len) = match self.footer.empty_space.iter().next() {
                Some(e) => e,
                None => {
                    // there is no empty space
                    return Ok(self);
                }
            };
            // Get the first occupied block after empty_offset
            let mut found_block_id = None;
            let mut found_block_offset = None;
            for (block_id, block_desc) in self.footer.blocks.iter() {
                // if the occupied block is *after* the empty space (note that accounting for length is unnecessary since blocks don't overlap)
                if empty_offset < &block_desc.file_offset {
                    // if this is the first found block, or if the block is closer than the previous closest, record it
                    if found_block_offset.is_none()
                        || block_desc.file_offset < found_block_offset.unwrap()
                    {
                        found_block_id = Some(block_id.clone());
                        found_block_offset = Some(block_desc.file_offset);
                    }
                }
            }
            // Move the block
            if let Some(block_id) = found_block_id {
                let (metadata, data) = self.get_block(&block_id)?;
                let metadata = metadata.clone();
                // deleting the block automatically consolidates and sorts free space
                self.delete_block(&block_id)?;
                // inserting always goes in the first empty block the data will fit, which since we
                // just deleted and consolidated the space previously occupied by this block will move
                // it closer to the start of the file.
                self.insert_block(&block_id, metadata, data.as_slice())?;
            } else {
                // There are no more blocks after the empty space, so proceed to moving the footer
                break;
            }
        }
        // no more blocks after the empty space
        let (empty_offset, _empty_len) = match self.footer.empty_space.first_entry() {
            Some(e) => e.remove_entry(),
            None => {
                // there is no empty space
                return Ok(self);
            }
        };
        // move the footer
        self.header.footer_offset = empty_offset;
        // handles the new offset, recalculates checksum and length
        self.footer.write_to(&mut self.file, &mut self.header)?;

        Ok(self)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Copy)]
pub enum BlockCompression {
    #[default]
    None,
    Gzip(u32),
}
impl BlockCompression {
    pub fn compress(&self, data: Vec<u8>) -> Result<Vec<u8>, CogtainerError> {
        match &self {
            Self::None => Ok(data),
            Self::Gzip(level) => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), Compression::new(*level));
                encoder.write(data.as_slice())?;

                Ok(encoder.finish()?)
            }
        }
    }
    pub fn decompress(&self, data: Vec<u8>) -> Result<Vec<u8>, CogtainerError> {
        match &self {
            Self::None => Ok(data),
            Self::Gzip(_level) => {
                let mut decoder = flate2::read::GzDecoder::new(data.as_slice());

                let mut dec = vec![];
                decoder.read_to_end(&mut dec)?;

                Ok(dec)
            }
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockHeader<T> {
    pub(crate) compression: BlockCompression,
    pub(crate) metadata: T,
}

#[cfg(feature = "full")]
impl<F: Seek + Read> Cogtainer<F> {
    pub fn get_metadata_as<T: DeserializeOwned>(&self) -> Result<T, CogtainerError> {
        let metadata = self.get_container_metadata();
        let metadata = rmpv::ext::from_value(metadata.clone())?;

        Ok(metadata)
    }
    /// Get the data of a specific block.
    /// Returns None if block not found.
    pub fn get_as<M: DeserializeOwned, D: DeserializeOwned>(
        &mut self,
        identifier: &Identifier,
    ) -> Result<(M, D), CogtainerError> {
        let (metadata, data) = self.get_block(identifier)?;

        let header: BlockHeader<M> = rmpv::ext::from_value(metadata.clone())?;
        let data = rmp_serde::from_slice(header.compression.decompress(data)?.as_slice())?;

        Ok((header.metadata, data))
    }
}
#[cfg(feature = "full")]
impl<F: Seek + Write> Cogtainer<F> {
    pub fn set_metadata_as<T: Serialize>(&mut self, meta: &T) -> Result<&mut Self, CogtainerError> {
        let meta = rmpv::ext::to_value(meta)?;
        self.set_metadata(meta)
    }
    /// Inserts a block with the given unique identifier.
    /// If a block already exists with the given identifier, it will be replaced.
    pub fn insert_block_as<M: Serialize, D: Serialize>(
        &mut self,
        identifier: &Identifier,
        compression: BlockCompression,
        metadata: &M,
        data: &D,
    ) -> Result<&mut Self, CogtainerError> {
        let header = BlockHeader {
            compression,
            metadata,
        };
        let metadata = rmpv::ext::to_value(header)?;
        let data = compression.compress(rmp_serde::to_vec(data)?)?;
        self.insert_block(identifier, metadata, data.as_slice())
    }
}
