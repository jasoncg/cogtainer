#[cfg(test)]
mod tests {
    use crate::{container_file::*, error::CogtainerError};

    use std::io::{Cursor, Read, Seek, SeekFrom, Write};

    fn new_mem_file() -> Cursor<Vec<u8>> {
        Cursor::new(vec![0u8; 1024 * 1024]) // 1MB buffer for easy test
    }

    fn open_new_container() -> (Cursor<Vec<u8>>, ContainerHeader, ContainerFooter) {
        let mut file = new_mem_file();
        let (header, footer) = ContainerHeader::create(&mut file).expect("create");
        (file, header, footer)
    }

    #[test]
    fn test_create_and_read_header_footer() {
        let (mut file, header, footer) = open_new_container();
        // Write to file
        footer
            .write_to(&mut file, &mut { header.clone() })
            .expect("write footer");
        // Read back header/footer
        let header2 = ContainerHeader::read_from(&mut file).expect("read header");
        let footer2 = ContainerFooter::read_from(&mut file, &header2).expect("read footer");
        assert_eq!(header2.version, 1);
        assert!(footer2.blocks.is_empty());
    }

    #[test]
    fn test_insert_single_block_and_read_back() {
        let (mut file, mut header, mut footer) = open_new_container();
        let data = b"hello, world!";
        let id = Identifier::String("test".into());
        let meta = rmpv::Value::from("some meta");

        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id,
                meta.clone(),
                data,
            )
            .expect("insert");
        let (block_metadata, bytes) = footer.get_block(&mut file, &id).expect("read block");
        assert_eq!(&bytes, data);
        assert_eq!(block_metadata.clone(), meta);
    }

    #[test]
    fn test_block_replacement_reuses_space() {
        let (mut file, mut header, mut footer) = open_new_container();
        let id = Identifier::String("block1".into());
        let data1 = vec![42u8; 32];
        let meta1 = rmpv::Value::from(1);
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id,
                meta1,
                &data1,
            )
            .unwrap();
        // Replace with same-length data, should reuse space (empty_space stays empty)
        let data2 = vec![99u8; 32];
        let meta2 = rmpv::Value::from(2);
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id,
                meta2.clone(),
                &data2,
            )
            .unwrap();
        assert!(footer.empty_space.is_empty());
        let (block_metadata, read_back) = footer.get_block(&mut file, &id).unwrap();
        assert_eq!(&read_back, &data2);
        assert_eq!(block_metadata.clone(), meta2);
    }

    #[test]
    fn test_block_replacement_creates_hole_and_fills_it() {
        let (mut file, mut header, mut footer) = open_new_container();
        let id1 = Identifier::String("A".into());
        let id2 = Identifier::String("B".into());
        let data_a = vec![1u8; 64];
        let data_b = vec![2u8; 32];

        // Insert A, then B
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id1,
                rmpv::Value::Nil,
                &data_a,
            )
            .unwrap();
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id2,
                rmpv::Value::Nil,
                &data_b,
            )
            .unwrap();
        // Remove A (creates a hole)
        footer.delete_block(&mut file, &mut header, &id1).unwrap();
        assert!(footer.empty_space.values().any(|&v| v == 64));
        // Insert a block that fits in the hole
        let id3 = Identifier::String("C".into());
        let data_c = vec![7u8; 60];
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id3,
                rmpv::Value::Nil,
                &data_c,
            )
            .unwrap();
        // Should create a leftover hole of 4 bytes
        assert!(footer.empty_space.values().any(|&v| v == 4));
        let (_block_metadata, read_back) = footer.get_block(&mut file, &id3).unwrap();
        assert_eq!(&read_back, &data_c);
    }

    #[test]
    fn test_overallocation_policy() {
        let (mut file, mut header, mut footer) = open_new_container();
        let id = Identifier::String("GROW".into());
        let data = vec![0xAB; 16];
        // Request 50% overallocation, capped at 16 extra bytes
        let policy = OverallocationPolicy::PercentageCapped {
            percentage: 0.5,
            max_add_bytes: 16,
        };
        footer
            .insert_block(&mut file, &mut header, policy, &id, rmpv::Value::Nil, &data)
            .unwrap();
        let desc = &footer.blocks[&id];
        assert!(desc.allocated_length > data.len() as u64);
        assert!(desc.allocated_length <= data.len() as u64 + 16);
    }

    #[test]
    fn test_zero_length_block() {
        let (mut file, mut header, mut footer) = open_new_container();
        let id = Identifier::String("EMPTY".into());
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id,
                rmpv::Value::Nil,
                &[],
            )
            .unwrap();
        let (_block_metadata, read) = footer.get_block(&mut file, &id).unwrap();
        assert_eq!(read.len(), 0);
    }

    #[test]
    fn test_metadata_update() {
        let (mut file, mut header, mut footer) = open_new_container();
        let id = Identifier::U64(123);
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id,
                rmpv::Value::from(1),
                b"abc",
            )
            .unwrap();
        // Update metadata only
        footer
            .update_block_metadata(
                &mut file,
                &mut header,
                id.clone(),
                rmpv::Value::from("hello"),
            )
            .unwrap();
        let meta = &footer.blocks[&id].metadata;
        assert_eq!(meta, &rmpv::Value::from("hello"));
    }

    #[test]
    fn test_consolidate_empty_space() {
        let (mut file, mut header, mut footer) = open_new_container();
        // Insert and delete several adjacent blocks, check they consolidate
        for i in 0..5 {
            let id = Identifier::U64(i);
            let data = vec![0u8; 8];
            footer
                .insert_block(
                    &mut file,
                    &mut header,
                    OverallocationPolicy::None,
                    &id,
                    rmpv::Value::Nil,
                    &data,
                )
                .unwrap();
        }
        // Delete all
        for i in 0..5 {
            let id = Identifier::U64(i);
            footer.delete_block(&mut file, &mut header, &id).unwrap();
        }
        // Should be one large empty space
        assert_eq!(footer.empty_space.len(), 1);
        let (&FileOffset(_start), &len) = footer.empty_space.iter().next().unwrap();
        assert_eq!(len, 40); // 5*8
    }

    #[test]
    fn test_block_checksum_detection() {
        let (mut file, mut header, mut footer) = open_new_container();
        let id = Identifier::String("corrupt".into());
        let data = b"good data";
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id,
                rmpv::Value::Nil,
                data,
            )
            .unwrap();
        // Corrupt the block data
        let block_desc = footer.blocks[&id].clone();
        file.seek(SeekFrom::Start(block_desc.file_offset.0))
            .unwrap();
        file.write_all(b"bad data").unwrap();
        // Should fail on checksum mismatch
        let result = footer.get_block(&mut file, &id);
        assert!(matches!(result, Err(CogtainerError::BlockChecksumError(_))));
    }

    #[test]
    fn test_footer_checksum_detection() {
        let (mut file, mut header, mut footer) = open_new_container();
        let id = Identifier::String("footer".into());
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id,
                rmpv::Value::Nil,
                b"x",
            )
            .unwrap();
        // Corrupt footer bytes
        file.seek(SeekFrom::Start(header.footer_offset.0)).unwrap();
        let mut bad_footer = vec![0u8; header.footer_length as usize];
        file.read_exact(&mut bad_footer).unwrap();
        // Flip a byte
        bad_footer[0] ^= 0xFF;
        file.seek(SeekFrom::Start(header.footer_offset.0)).unwrap();
        file.write_all(&bad_footer).unwrap();
        // Should fail on footer checksum
        let result = ContainerFooter::read_from(&mut file, &header);
        assert!(matches!(result, Err(CogtainerError::FooterChecksumError)));
    }

    #[test]
    fn test_block_path_identifier() {
        let (mut file, mut header, mut footer) = open_new_container();
        let id = Identifier::Path(vec![
            Identifier::String("sheets".to_string()),
            Identifier::U64(0),
            Identifier::String("A1".to_string()),
        ]);
        let data = b"cell data";
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id,
                rmpv::Value::Nil,
                data,
            )
            .unwrap();
        let (_block_metadata, read_back) = footer.get_block(&mut file, &id).unwrap();
        assert_eq!(&read_back, data);
    }

    #[test]
    fn test_multi_block_roundtrip() {
        let (mut file, mut header, mut footer) = open_new_container();
        let data = (0..100)
            .map(|i| (Identifier::U64(i), vec![i as u8; 5]))
            .collect::<Vec<_>>();
        for (id, d) in &data {
            footer
                .insert_block(
                    &mut file,
                    &mut header,
                    OverallocationPolicy::None,
                    id,
                    rmpv::Value::Nil,
                    d,
                )
                .unwrap();
        }
        // Read all back
        for (id, d) in &data {
            let (_block_metadata, rb) = footer.get_block(&mut file, id).unwrap();
            assert_eq!(rb, *d);
        }
    }

    #[test]
    fn test_insert_splits_hole_correctly() {
        let (mut file, mut header, mut footer) = open_new_container();
        let id1 = Identifier::String("block1".into());
        let id2 = Identifier::String("block2".into());
        let data1 = vec![1u8; 100];
        let data2 = vec![2u8; 100];
        // Insert two blocks, then delete the first
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id1,
                rmpv::Value::Nil,
                &data1,
            )
            .unwrap();
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id2,
                rmpv::Value::Nil,
                &data2,
            )
            .unwrap();
        footer.delete_block(&mut file, &mut header, &id1).unwrap();
        let orig_hole_offset = footer.empty_space.keys().next().copied().unwrap();
        // Insert a smaller block
        let id3 = Identifier::String("block3".into());
        let data3 = vec![3u8; 40];
        footer
            .insert_block(
                &mut file,
                &mut header,
                OverallocationPolicy::None,
                &id3,
                rmpv::Value::Nil,
                &data3,
            )
            .unwrap();
        // The leftover hole should now be of size 60 at the expected offset
        let holes: Vec<_> = footer.empty_space.iter().collect();
        assert!(holes
            .iter()
            .any(|(&off, &len)| off.0 == orig_hole_offset.0 + 40 && len == 60));
    }
    #[test]
    fn test_delete_nonexistent_block_returns_error() {
        let (mut file, mut header, mut footer) = open_new_container();
        let result = footer.delete_block(
            &mut file,
            &mut header,
            &Identifier::String("missing".into()),
        );
        assert!(matches!(result, Err(CogtainerError::BlockNotFound(_))));
    }

    #[test]
    fn test_reserved_fields_nonzero() {
        let (mut file, mut header, _footer) = open_new_container();
        header.reserved = [1, 2, 3, 4];
        header.write_to(&mut file).unwrap();
        // Still able to read header/footer after
        let header2 = ContainerHeader::read_from(&mut file).unwrap();
        assert_eq!(header2.reserved, [1, 2, 3, 4]);
    }

    #[test]
    fn test_multiple_random_inserts_deletes() {
        let (mut file, mut header, mut footer) = open_new_container();
        for i in 0..20 {
            let id = Identifier::U64(i);
            let data = vec![i as u8; 10];
            footer
                .insert_block(
                    &mut file,
                    &mut header,
                    OverallocationPolicy::None,
                    &id,
                    rmpv::Value::from(i),
                    &data,
                )
                .unwrap();
            if i % 3 == 0 {
                footer.delete_block(&mut file, &mut header, &id).unwrap();
            }
        }
        // All non-deleted blocks should be readable and correct
        for i in 0..20 {
            let id = Identifier::U64(i);
            if i % 3 != 0 {
                let (_block_metadata, data) = footer.get_block(&mut file, &id).unwrap();
                assert_eq!(data, vec![i as u8; 10]);
            }
        }
    }
}
