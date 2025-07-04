use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io::{Seek, SeekFrom},
    ops::{Deref, DerefMut},
};

use serde::{Deserialize, Serialize};

use crate::error::{CogtainerError, HeaderError};

use super::*;

// Magic Number (DCCF), Version, Footer Offset, Footer Checksum, [reserved bytes]
#[derive(Debug, Clone)]
pub struct ContainerHeader {
    pub magic_number: [u8; 4],
    pub version: u64,
    pub footer_offset: FileOffset,
    pub footer_length: u64,
    pub footer_checksum: Checksum,
    pub reserved: [u64; 4],
}
/// ContainerHeader functions related to writing.
impl ContainerHeader {
    pub(crate) const HEADER_SIZE: usize = 4 + 8 + 8 + 8 + 8 + 8 * 4;

    /// Creates a new empty Container.
    /// This also creates an empty footer, and writes both to the provided writer.
    pub fn create<W: std::io::Write + std::io::Seek>(
        writer: &mut W,
    ) -> Result<(Self, ContainerFooter), CogtainerError> {
        let mut header = Self {
            magic_number: DCCF_MAGIC,
            version: 1,
            footer_offset: FileOffset(Self::HEADER_SIZE as u64),
            footer_length: 0,
            footer_checksum: Checksum(0),
            reserved: [0, 0, 0, 0],
        };
        let footer = ContainerFooter::create(writer, &mut header)?;

        Ok((header, footer))
    }
    /// Returns the actual used size of the data in this container, from the header to the end of the footer.
    /// This can be used to truncate files after defragmenting.
    pub fn file_length(&self) -> u64 {
        self.footer_offset.0 + self.footer_length
    }
    /// Writes this header to the given writer.
    /// Returns the end of the header.
    ///
    /// There are only two situations when this function should be called:
    /// - When the footer is modified and its checksum and/or length change. The footer's `write_to(..)` function handles this.
    /// - When the footer must be moved because the space it occupied is needed by a block or defragmenting moves the footer up.
    ///
    pub fn write_to<W: std::io::Write + std::io::Seek>(
        &self,
        writer: &mut W,
    ) -> Result<FileOffset, CogtainerError> {
        let initial_position = writer.stream_position()?;
        writer.seek(SeekFrom::Start(0))?;

        writer.write_all(&self.magic_number)?;
        writer.write_all(&self.version.to_le_bytes())?;
        writer.write_all(&self.footer_offset.to_le_bytes())?;
        writer.write_all(&self.footer_length.to_le_bytes())?;
        writer.write_all(&self.footer_checksum.to_le_bytes())?;
        for r in self.reserved {
            writer.write_all(&r.to_le_bytes())?;
        }
        writer.seek(SeekFrom::Start(initial_position))?;
        Ok(FileOffset(writer.stream_position()?))
    }
}
/// ContainerHeader functions related to reading.
impl ContainerHeader {
    /// Read the header from the given reader
    pub fn read_from<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
    ) -> Result<Self, CogtainerError> {
        let mut header_bytes = [0u8; Self::HEADER_SIZE];
        reader.seek(SeekFrom::Start(0))?;
        reader.read_exact(&mut header_bytes)?;
        let magic_number = &header_bytes[0..4];
        if magic_number != DCCF_MAGIC {
            return Err(CogtainerError::InvalidHeader(HeaderError::Magic));
        }
        let version = u64::from_le_bytes(
            header_bytes[4..12]
                .try_into()
                .map_err(|_e| CogtainerError::InvalidHeader(HeaderError::Version))?,
        );
        let footer_offset = FileOffset(u64::from_le_bytes(
            header_bytes[12..20]
                .try_into()
                .map_err(|_e| CogtainerError::InvalidHeader(HeaderError::FooterOffset))?,
        ));
        let footer_length = u64::from_le_bytes(
            header_bytes[20..28]
                .try_into()
                .map_err(|_e| CogtainerError::InvalidHeader(HeaderError::FooterLength))?,
        );
        let footer_checksum = Checksum(u64::from_le_bytes(
            header_bytes[28..36]
                .try_into()
                .map_err(|_e| CogtainerError::InvalidHeader(HeaderError::FooterChecksum))?,
        ));

        let header = Self {
            magic_number: DCCF_MAGIC,
            version,
            footer_offset,
            footer_length,
            footer_checksum,
            reserved: [
                u64::from_le_bytes(header_bytes[36..44].try_into().map_err(|_| {
                    CogtainerError::InvalidHeader(HeaderError::Other("Reserved".to_string()))
                })?),
                u64::from_le_bytes(header_bytes[44..52].try_into().map_err(|_| {
                    CogtainerError::InvalidHeader(HeaderError::Other("Reserved".to_string()))
                })?),
                u64::from_le_bytes(header_bytes[52..60].try_into().map_err(|_| {
                    CogtainerError::InvalidHeader(HeaderError::Other("Reserved".to_string()))
                })?),
                u64::from_le_bytes(header_bytes[60..68].try_into().map_err(|_| {
                    CogtainerError::InvalidHeader(HeaderError::Other("Reserved".to_string()))
                })?),
            ],
        };

        Ok(header)
    }

    /// Get the footer from the file
    pub fn get_footer<R: std::io::Read + std::io::Seek>(
        &self,
        reader: &mut R,
    ) -> Result<ContainerFooter, CogtainerError> {
        ContainerFooter::read_from(reader, self)
    }
}
