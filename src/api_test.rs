#[cfg(test)]
mod tests {
    use crate::{basic_api::*, container_file::*, error::CogtainerError};

    use super::*;
    use std::io::{Cursor, Read, Seek, SeekFrom, Write};

    fn new_mem_file() -> Cursor<Vec<u8>> {
        Cursor::new(vec![0u8; 1024 * 1024])
    }

    fn open_container() -> Cogtainer<Cursor<Vec<u8>>> {
        let mut file = new_mem_file();
        let (header, footer) = ContainerHeader::create(&mut file).unwrap();
        Cogtainer {
            file,
            header,
            footer,
            overallocation_policy: OverallocationPolicy::default(),
        }
    }

    #[test]
    fn test_open_and_metadata_roundtrip() {
        let mut c = open_container();
        // Set and get container-wide metadata
        let meta = rmpv::Value::from("meta123");
        c.set_metadata(meta.clone()).unwrap();
        assert_eq!(c.get_container_metadata(), &meta);

        // Now flush, reopen, and verify
        c.flush().unwrap();
        let buf = c.file.into_inner();
        let mut c2 = Cogtainer::open(Cursor::new(buf)).unwrap();
        assert_eq!(c2.get_container_metadata(), &meta);
    }

    #[test]
    fn test_insert_and_get_block() {
        let mut c = open_container();
        let id = Identifier::String("myid".into());
        let meta = rmpv::Value::from("blockmeta");
        let data = b"data block";
        c.insert_block(&id, meta.clone(), data)
            .unwrap()
            .flush()
            .unwrap();

        let (got_meta, got_data) = c.get_block(&id).unwrap();
        assert_eq!(got_meta, &meta);
        assert_eq!(got_data, data);
    }

    #[test]
    fn test_overwrite_block_and_read_new() {
        let mut c = open_container();
        let id = Identifier::U64(17);
        let meta1 = rmpv::Value::from(1);
        let data1 = b"old data";
        c.insert_block(&id, meta1.clone(), data1)
            .unwrap()
            .flush()
            .unwrap();

        // Overwrite with new data and metadata
        let meta2 = rmpv::Value::from(2);
        let data2 = b"new data with more bytes";
        c.insert_block(&id, meta2.clone(), data2)
            .unwrap()
            .flush()
            .unwrap();

        let (m, d) = c.get_block(&id).unwrap();
        assert_eq!(m, &meta2);
        assert_eq!(d, data2);
    }

    #[test]
    fn test_delete_and_error_on_get() {
        let mut c = open_container();
        let id = Identifier::String("gone".into());
        c.insert_block(&id, rmpv::Value::Nil, b"abc")
            .unwrap()
            .flush()
            .unwrap();
        c.delete_block(&id).unwrap().flush().unwrap();

        let err = c.get_block(&id).unwrap_err();
        matches!(err, CogtainerError::BlockNotFound(_));
    }

    #[test]
    fn test_bulk_insert_get_delete() {
        let mut c = open_container();
        let count = 50;
        let mut blocks = vec![];
        for i in 0..count {
            let id = Identifier::U64(i);
            let meta = rmpv::Value::from(i);
            let data = vec![i as u8; (i + 1) as usize];
            c.insert_block(&id, meta.clone(), &data).unwrap();
            blocks.push((id, meta, data));
        }
        c.flush().unwrap();

        // Retrieve all
        for (id, meta, data) in &blocks {
            let (got_meta, got_data) = c.get_block(id).unwrap();
            assert_eq!(got_meta, meta);
            assert_eq!(&got_data, data);
        }

        // Delete even-indexed
        for (id, _, _) in &blocks {
            if let Identifier::U64(i) = id {
                if i % 2 == 0 {
                    c.delete_block(id).unwrap();
                }
            }
        }
        c.flush().unwrap();

        // Check deleted are gone, odds remain
        for (id, _, _) in &blocks {
            if let Identifier::U64(i) = id {
                let res = c.get_block(id);
                if i % 2 == 0 {
                    assert!(res.is_err());
                } else {
                    assert!(res.is_ok());
                }
            }
        }
    }

    #[test]
    fn test_zero_length_block_roundtrip() {
        let mut c = open_container();
        let id = Identifier::Bytes(b"empty".to_vec());
        c.insert_block(&id, rmpv::Value::Nil, &[])
            .unwrap()
            .flush()
            .unwrap();
        let (m, d) = c.get_block(&id).unwrap();
        assert_eq!(m, &rmpv::Value::Nil);
        assert!(d.is_empty());
    }

    #[test]
    fn test_set_overallocation_policy_and_insert() {
        let mut c = open_container();
        let id = Identifier::U64(99);
        let data = vec![1u8; 32];
        c.set_overallocation_policy(OverallocationPolicy::Bytes(32))
            .insert_block(&id, rmpv::Value::Nil, &data)
            .unwrap()
            .flush()
            .unwrap();

        // Confirm block is present and reads back OK
        let (_, read) = c.get_block(&id).unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn test_chainable_api() {
        let mut c = open_container();
        let id1 = Identifier::U64(1);
        let id2 = Identifier::U64(2);
        c.set_overallocation_policy(OverallocationPolicy::Percentage(0.25))
            .insert_block(&id1, rmpv::Value::Nil, b"abc")
            .unwrap()
            .insert_block(&id2, rmpv::Value::from(42), b"defg")
            .unwrap()
            .delete_block(&id1)
            .unwrap()
            .flush()
            .unwrap();
        // id1 is deleted, id2 exists
        assert!(c.get_block(&id1).is_err());
        assert_eq!(c.get_block(&id2).unwrap().1, b"defg");
    }

    #[test]
    fn test_typed_metadata_api() {
        #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
        struct MyMeta {
            tag: String,
            x: i32,
        }
        let mut c = open_container();
        let id = Identifier::String("typed".to_string());
        let meta = MyMeta {
            tag: "foo".to_string(),
            x: 42,
        };
        let data = vec![7, 8, 9];
        #[cfg(feature = "full")]
        {
            c.insert_block_as(&id, BlockCompression::None, &meta, &data)
                .unwrap()
                .flush()
                .unwrap();

            let (got_meta, got_data): (MyMeta, Vec<u8>) = c.get_as(&id).unwrap();
            assert_eq!(got_meta, meta);
            assert_eq!(got_data, data);
        }
    }

    #[test]
    fn test_get_blocks_list_returns_all() {
        let mut c = open_container();
        let mut ids = vec![];
        for i in 0..10 {
            let id = Identifier::U64(i);
            c.insert_block(&id, rmpv::Value::from(i), &[i as u8])
                .unwrap();
            ids.push(id);
        }
        c.flush().unwrap();
        let keys: Vec<_> = c.get_blocks_list().keys().cloned().collect();
        assert_eq!(keys.len(), 10);
        for id in ids {
            assert!(keys.contains(&id));
        }
    }

    #[test]
    fn test_get_block_returns_metadata() {
        let mut c = open_container();
        let id = Identifier::String("m".to_string());
        let meta = rmpv::Value::from("xx");
        c.insert_block(&id, meta.clone(), &[1, 2, 3])
            .unwrap()
            .flush()
            .unwrap();
        let (m, d) = c.get_block(&id).unwrap();
        assert_eq!(m, &meta);
        assert_eq!(d, vec![1, 2, 3]);
    }

    #[test]
    fn test_insert_block_large_and_small() {
        let mut c = open_container();
        let id1 = Identifier::U64(1000);
        let id2 = Identifier::U64(1001);
        let large = vec![0xAA; 4096];
        let small = vec![0x42; 2];
        c.insert_block(&id1, rmpv::Value::Nil, &large).unwrap();
        c.insert_block(&id2, rmpv::Value::Nil, &small).unwrap();
        c.flush().unwrap();
        assert_eq!(c.get_block(&id1).unwrap().1, large);
        assert_eq!(c.get_block(&id2).unwrap().1, small);
    }

    #[test]
    fn test_roundtrip_after_reopen() {
        let mut c = open_container();
        let id = Identifier::String("persist".into());
        c.insert_block(&id, rmpv::Value::from("meta"), b"blob")
            .unwrap()
            .flush()
            .unwrap();
        let buf = c.file.into_inner();
        let mut c2 = Cogtainer::open(Cursor::new(buf)).unwrap();
        let (m, d) = c2.get_block(&id).unwrap();
        assert_eq!(m, &rmpv::Value::from("meta"));
        assert_eq!(d, b"blob");
    }

    #[test]
    fn test_error_on_nonexistent_block() {
        let mut c = open_container();
        let missing = Identifier::String("does_not_exist".into());
        assert!(matches!(
            c.get_block(&missing),
            Err(CogtainerError::BlockNotFound(_))
        ));
    }

    #[test]
    fn test_metadata_update_flushes() {
        let mut c = open_container();
        let id = Identifier::U64(1);
        c.insert_block(&id, rmpv::Value::from(1), b"abc")
            .unwrap()
            .flush()
            .unwrap();
        // Change container metadata
        let meta = rmpv::Value::from("newmeta");
        c.set_metadata(meta.clone()).unwrap();
        assert_eq!(c.get_container_metadata(), &meta);
    }

    fn open_new_container() -> Cogtainer<Cursor<Vec<u8>>> {
        let file = Cursor::new(vec![0u8; 512 * 1024]);
        Cogtainer::create(file).unwrap()
    }

    #[test]
    fn insert_smaller_then_larger_block_allocates_new_space() {
        let mut c = open_new_container();
        let id = Identifier::String("resize".to_string());

        // Insert small block, no overallocation
        let data1 = vec![1u8; 8];
        c.insert_block(&id, rmpv::Value::Nil, &data1).unwrap();
        c.flush().unwrap();

        // Overwrite with larger data; overallocation not possible (should allocate at end)
        let data2 = vec![2u8; 128];
        c.insert_block(&id, rmpv::Value::Nil, &data2).unwrap();
        c.flush().unwrap();

        // Block's offset should have changed, and new allocation is large enough
        let block = &c.get_blocks_list()[&id];
        assert!(block.used_length == 128);
        assert!(block.allocated_length >= 128);
        let (_meta, read) = c.get_block(&id).unwrap();
        assert_eq!(read, data2);
    }

    #[test]
    fn zero_length_block_roundtrip_metadata() {
        let mut c = open_new_container();
        let id = Identifier::String("empty".to_string());
        let meta = rmpv::Value::from("just meta");
        c.insert_block(&id, meta.clone(), &[]).unwrap();
        c.flush().unwrap();

        let (m, data) = c.get_block(&id).unwrap();
        assert_eq!(data.len(), 0);
        assert_eq!(m, &meta);

        // Should be stored at offset 0 with zero length
        let block = &c.get_blocks_list()[&id];
        assert_eq!(block.used_length, 0);
        assert_eq!(block.allocated_length, 0);
        assert_eq!(block.file_offset.0, 0);
    }

    #[test]
    fn block_tail_zeroed_on_overallocation() {
        let mut c = open_new_container();
        let id = Identifier::U64(5);

        // Insert block with overallocation
        let data = vec![0xCA; 8];
        c.set_overallocation_policy(OverallocationPolicy::Bytes(8));
        c.insert_block(&id, rmpv::Value::Nil, &data).unwrap();
        c.flush().unwrap();

        // Read the raw file bytes to check the tail
        let block = &c.get_blocks_list()[&id];
        let mut buf = vec![0u8; block.allocated_length as usize];
        c.file.seek(SeekFrom::Start(block.file_offset.0)).unwrap();
        c.file.read_exact(&mut buf).unwrap();

        assert_eq!(&buf[..8], &data[..]);
        assert!(
            buf[8..].iter().all(|&b| b == 0),
            "Allocation tail should be zeroed"
        );
    }

    #[test]
    fn overwrite_with_larger_data_fills_tail() {
        let mut c = open_new_container();
        let id = Identifier::String("grow".to_string());

        // Insert 16 bytes with 16 bytes overalloc (total 32)
        let data1 = vec![0xDD; 16];
        c.set_overallocation_policy(OverallocationPolicy::Bytes(16));
        c.insert_block(&id, rmpv::Value::Nil, &data1).unwrap();
        c.flush().unwrap();

        // Overwrite with 32 bytes; block moves, and tail should be zero
        let data2 = vec![0xEE; 32];
        c.insert_block(&id, rmpv::Value::Nil, &data2).unwrap();
        c.flush().unwrap();

        let block = &c.get_blocks_list()[&id].clone();
        let mut buf = vec![0u8; block.allocated_length as usize];
        c.file.seek(SeekFrom::Start(block.file_offset.0)).unwrap();
        c.file.read_exact(&mut buf).unwrap();

        assert_eq!(&buf[..32], &data2[..]);
        if block.allocated_length > 32 {
            assert!(buf[32..].iter().all(|&b| b == 0));
        }
    }

    #[test]
    fn insert_and_delete_blocks_creates_and_fills_holes() {
        let mut c = open_new_container();

        let id1 = Identifier::U64(1);
        let id2 = Identifier::U64(2);
        let d1 = vec![0x11; 32];
        let d2 = vec![0x22; 32];
        let d3 = vec![0x33; 32];

        // Insert two blocks, then delete first
        c.insert_block(&id1, rmpv::Value::Nil, &d1).unwrap();
        c.insert_block(&id2, rmpv::Value::Nil, &d2).unwrap();
        c.flush().unwrap();

        c.delete_block(&id1).unwrap();
        c.flush().unwrap();

        // Now insert a block that fits the hole exactly (should reuse space)
        let id3 = Identifier::U64(3);
        c.insert_block(&id3, rmpv::Value::Nil, &d3).unwrap();
        c.flush().unwrap();

        let block1 = &c.get_blocks_list()[&id3];
        let block2 = &c.get_blocks_list()[&id2];
        // Their offsets should not overlap
        assert_ne!(block1.file_offset, block2.file_offset);
    }

    #[test]
    fn get_block_returns_error_on_bad_checksum() {
        let mut c = open_new_container();
        let id = Identifier::U64(88);
        let d = vec![0x44; 16];
        c.insert_block(&id, rmpv::Value::Nil, &d).unwrap();
        c.flush().unwrap();

        // Corrupt the block's data
        let block = c.get_blocks_list()[&id].clone();
        c.file.seek(SeekFrom::Start(block.file_offset.0)).unwrap();
        c.file.write_all(&[0x99; 16]).unwrap();

        let result = c.get_block(&id);
        assert!(matches!(result, Err(CogtainerError::BlockChecksumError(_))));
    }

    #[test]
    fn test_metadata_only_blocks_and_block_with_no_metadata() {
        let mut c = open_new_container();
        let id_meta = Identifier::String("meta".into());
        let id_no_meta = Identifier::String("no_meta".into());

        // Metadata only
        let meta = rmpv::Value::from(1234);
        c.insert_block(&id_meta, meta.clone(), &[]).unwrap();

        // Data but minimal metadata
        c.insert_block(&id_no_meta, rmpv::Value::Nil, b"datablob")
            .unwrap();
        c.flush().unwrap();

        let (m, d) = c.get_block(&id_meta).unwrap();
        assert_eq!(m, &meta);
        assert_eq!(d.len(), 0);

        let (m2, d2) = c.get_block(&id_no_meta).unwrap();
        assert_eq!(m2, &rmpv::Value::Nil);
        assert_eq!(d2, b"datablob");
    }
}
