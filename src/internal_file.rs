use std::io::{Error, Read, Seek, Write};

use crate::{
    basic_api::Cogtainer,
    container_file::{Checksum, Identifier},
};

/// Provides access to a block with a file-like API.
/// Intended for when storing other files in a container.
pub struct InternalFile<'a, F: std::io::Read + std::io::Write + std::io::Seek> {
    file: &'a mut Cogtainer<F>,
    block_id: Identifier,

    cursor: u64,
}
impl<'a, F: std::io::Read + std::io::Write + std::io::Seek> InternalFile<'a, F> {
    pub(crate) fn new(file: &'a mut Cogtainer<F>, block_id: Identifier) -> Self {
        Self {
            file,
            block_id,
            cursor: 0,
        }
    }
}
impl<'a, F: std::io::Read + std::io::Write + std::io::Seek> Seek for InternalFile<'a, F> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.cursor = match pos {
            std::io::SeekFrom::Start(p) => p,
            std::io::SeekFrom::End(p) => {
                let block =
                    self.file.footer.blocks.get(&self.block_id).ok_or_else(|| {
                        Error::new(std::io::ErrorKind::NotFound, "block not found")
                    })?;

                let used_len = block.used_length as i128;

                let new_pos = used_len + (p as i128);
                if new_pos < 0 {
                    return Err(Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Invalid seek from end",
                    ));
                }
                new_pos as u64
            }
            std::io::SeekFrom::Current(p) => {
                let cur = self.cursor as i128;
                let new_pos = cur + (p as i128);
                if new_pos < 0 {
                    return Err(Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Invalid seek from current",
                    ));
                }
                new_pos as u64
            }
        };

        Ok(self.cursor)
    }
}
impl<'a, F: std::io::Read + std::io::Write + std::io::Seek> Read for InternalFile<'a, F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self
            .file
            .footer
            .get_block_slice(&mut self.file.file, &self.block_id, self.cursor, buf)
            .map_err(|e| Error::new(std::io::ErrorKind::NotFound, e))?;

        self.cursor = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| Error::new(std::io::ErrorKind::InvalidInput, "cursor overflow"))?;
        Ok(len as usize)
    }
}
// impl<'a, F: Seek + Read + Write + Truncate> Truncate for InternalFile<'a, F> {
//     fn truncate(&self, offset: u64) -> Result<(), ()> {
//         let block = self
//             .file
//             .footer
//             .blocks
//             .get(&self.block_id)
//             .ok_or_else(|| ())?;
//         self.file.trun
//         todo!()
//     }
// }
impl<'a, F: std::io::Read + std::io::Write + std::io::Seek> Write for InternalFile<'a, F> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        use std::cmp::min;
        use std::io::{Error, ErrorKind, SeekFrom};

        // 0) Trivial fast path
        if buf.is_empty() {
            return Ok(0);
        }

        // 1) Snapshot block descriptor (if exists)
        let desc_opt = self.file.footer.blocks.get(&self.block_id).cloned();

        // When the block doesn't exist yet, fall back to the "rebuild/insert" path below.
        // (This will create it and set metadata = Nil.)
        let Some(desc) = desc_opt else {
            let old_meta = rmpv::Value::Nil;
            //let old_data: Vec<u8> = Vec::new();
            let old_used: u64 = 0;

            let write_len = buf.len() as u64;
            let new_used = old_used
                .checked_add(self.cursor)
                .and_then(|x| {
                    x.checked_add(write_len)
                        .map(|_| old_used.max(self.cursor + write_len))
                })
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "size overflow"))?;

            if new_used > (usize::MAX as u64) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "block too large for platform",
                ));
            }

            let mut new_data = vec![0u8; new_used as usize];
            // overlay new bytes at cursor
            new_data[self.cursor as usize..self.cursor as usize + buf.len()].copy_from_slice(buf);

            self.file
                .insert_block(&self.block_id, old_meta, &new_data)
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

            self.cursor = self.cursor.saturating_add(write_len);
            return Ok(buf.len());
        };

        // 2) We have an existing block; try in-place if within allocation
        let write_len_u64 = u64::try_from(buf.len())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "buffer length overflow"))?;
        let end_pos = self
            .cursor
            .checked_add(write_len_u64)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "size overflow"))?;

        let fits_in_alloc = end_pos <= desc.allocated_length && desc.allocated_length > 0;

        if fits_in_alloc {
            // 2a) If we're extending beyond used_length but still inside allocation, zero-fill the gap [used_length, cursor)
            if self.cursor > desc.used_length {
                let gap = self.cursor - desc.used_length;
                // write zeros into the gap
                self.file
                    .file
                    .seek(SeekFrom::Start(desc.file_offset.0 + desc.used_length))?;
                // Write zeros in reasonable chunks to avoid big temporary vecs
                const ZEROS: [u8; 4096] = [0u8; 4096];
                let mut remaining = gap;
                while remaining > 0 {
                    let chunk = min(remaining, ZEROS.len() as u64) as usize;
                    self.file.file.write_all(&ZEROS[..chunk])?;
                    remaining -= chunk as u64;
                }
            }

            // 2b) Write the user buffer at current cursor
            self.file
                .file
                .seek(SeekFrom::Start(desc.file_offset.0 + self.cursor))?;
            self.file.file.write_all(buf)?;

            // 2c) Update used_length and checksum
            let new_used = desc.used_length.max(end_pos);

            // Recompute checksum by streaming the used bytes (no full in-memory rebuild).
            // (Uses the same seed/algorithm as calc_checksum(..), but streaming.)
            use std::hash::Hasher;
            use twox_hash::XxHash64;

            let mut hasher = XxHash64::with_seed(4321);
            self.file.file.seek(SeekFrom::Start(desc.file_offset.0))?;
            let mut tmp = [0u8; 8192];
            let mut remaining = new_used;
            while remaining > 0 {
                let to_read = min(remaining, tmp.len() as u64) as usize;
                self.file.file.read_exact(&mut tmp[..to_read])?;
                hasher.write(&tmp[..to_read]);
                remaining -= to_read as u64;
            }
            let checksum = Checksum(hasher.finish());

            // 2d) Commit descriptor changes + footer
            if let Some(d) = self.file.footer.blocks.get_mut(&self.block_id) {
                d.used_length = new_used;
                d.checksum = checksum;
                // allocated_length, metadata, file_offset unchanged
            }
            // Persist footer (and header)
            self.file
                .footer
                .write_to(&mut self.file.file, &mut self.file.header)
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

            // 2e) Advance cursor
            self.cursor = end_pos;
            return Ok(buf.len());
        }

        // 3) Doesn't fit in current allocation -> fall back to existing rebuild/insert path
        let (old_meta, old_data, old_used) = {
            let (m, d) = self
                .file
                .footer
                .get_block(&mut self.file.file, &self.block_id)
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
            (m.clone(), d, desc.used_length)
        };

        let write_len = buf.len() as u64;
        let new_used = old_used.max(self.cursor + write_len);

        if new_used > (usize::MAX as u64) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "block too large for platform",
            ));
        }

        let mut new_data = vec![0u8; new_used as usize];
        // copy old
        let to_copy = old_data.len().min(new_data.len());
        new_data[..to_copy].copy_from_slice(&old_data[..to_copy]);
        // overlay
        new_data[self.cursor as usize..self.cursor as usize + buf.len()].copy_from_slice(buf);

        self.file
            .insert_block(&self.block_id, old_meta, &new_data)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;

        self.cursor += write_len;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file
            .flush()
            .map_err(|e| Error::new(std::io::ErrorKind::InvalidInput, e))?;
        Ok(())
    }
}
