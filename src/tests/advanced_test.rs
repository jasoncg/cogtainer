#[cfg(test)]
mod compression_block_tests {
    use crate::{basic_api::*, container_file::*};

    use std::io::Cursor;

    fn open_new_container() -> Cogtainer<Cursor<Vec<u8>>> {
        let file = Cursor::new(vec![0u8; 128 * 1024]);
        Cogtainer::create(file).unwrap()
    }

    #[test]
    fn insert_and_get_uncompressed_block() {
        let mut c = open_new_container();
        let id = Identifier::String("plain".into());
        let meta = "plain-meta".to_string();
        let data = vec![0x42; 128];
        c.insert_block_as(&id, BlockCompression::None, &meta, &data)
            .unwrap();

        let (metadata, actual): (String, Vec<u8>) = c.get_as(&id).unwrap();
        assert_eq!(metadata, meta);
        assert_eq!(actual, data);
    }

    #[test]
    fn insert_and_get_gzip_block() {
        let mut c = open_new_container();
        let id = Identifier::String("gz".into());
        let meta = "gzip-meta".to_string();
        let data = b"hello gzip world!".repeat(16);

        c.insert_block_as(&id, BlockCompression::Gzip(6), &meta, &data)
            .unwrap();

        let (metadata, actual): (String, Vec<u8>) = c.get_as(&id).unwrap();
        assert_eq!(metadata, meta);
        assert_eq!(actual, data);
    }

    #[test]
    fn block_compression_roundtrips_various_levels() {
        let mut c = open_new_container();
        let _id = Identifier::String("multi-level".into());
        let meta = "gzip-multilevel".to_string();
        let data = b"The quick brown fox jumps over the lazy dog. ".repeat(64);

        for level in 1..=9 {
            let id_level = Identifier::String(format!("level-{}", level));
            c.insert_block_as(&id_level, BlockCompression::Gzip(level), &meta, &data)
                .unwrap();

            let (metadata, actual): (String, Vec<u8>) = c.get_as(&id_level).unwrap();
            assert_eq!(metadata, meta);
            assert_eq!(
                actual, data,
                "Compression at level {} failed roundtrip",
                level
            );
        }
    }

    #[test]
    fn empty_block_and_empty_metadata() {
        let mut c = open_new_container();
        let id = Identifier::String("empty".into());
        let meta: Option<String> = None;
        let data: Vec<u8> = vec![];

        c.insert_block_as(&id, BlockCompression::None, &meta, &data)
            .unwrap();

        let (metadata, actual): (Option<String>, Vec<u8>) = c.get_as(&id).unwrap();
        assert_eq!(metadata, meta);
        assert_eq!(actual, data);
    }

    #[test]
    fn overwriting_compressed_block_with_uncompressed_and_vice_versa() {
        let mut c = open_new_container();
        let id = Identifier::String("toggle".into());
        let meta = "toggle-meta".to_string();
        let data1 = b"lots of data ".repeat(64);
        let data2 = b"other kind of data".repeat(8);

        // Write compressed
        c.insert_block_as(&id, BlockCompression::Gzip(1), &meta, &data1)
            .unwrap();
        let (_header, actual): (String, Vec<u8>) = c.get_as(&id).unwrap();
        assert_eq!(actual, data1);

        // Overwrite with uncompressed
        c.insert_block_as(&id, BlockCompression::None, &meta, &data2)
            .unwrap();
        let (_header, actual): (String, Vec<u8>) = c.get_as(&id).unwrap();
        assert_eq!(actual, data2);

        // And back to compressed
        c.insert_block_as(&id, BlockCompression::Gzip(9), &meta, &data1)
            .unwrap();
        let (_header, actual): (String, Vec<u8>) = c.get_as(&id).unwrap();
        assert_eq!(actual, data1);
    }

    #[test]
    fn test_error_on_corrupted_gzip_block() {
        let mut c = open_new_container();
        let id = Identifier::String("corrupt".into());
        let meta = "meta".to_string();
        let data = b"hello hello hello".to_vec();

        c.insert_block_as(&id, BlockCompression::Gzip(5), &meta, &data)
            .unwrap();

        // Tamper with the raw stored data in the container
        let block_desc = c.get_blocks_list().get(&id).unwrap();
        let offset = block_desc.file_offset.0;
        let len = block_desc.used_length as usize;

        let file = c.file.get_mut();
        file[offset as usize..offset as usize + len]
            .iter_mut()
            .for_each(|b| *b = 0xFF);

        // Now the block should fail to decompress (or fail block checksum first)
        let result: Result<(String, Vec<u8>), _> = c.get_as(&id);
        assert!(result.is_err());
    }

    #[test]
    fn get_as_returns_error_for_wrong_metadata_type() {
        let mut c = open_new_container();
        let id = Identifier::String("wrongmeta".into());
        let meta = "just a string".to_string();
        let data = b"123".to_vec();
        c.insert_block_as(&id, BlockCompression::None, &meta, &data)
            .unwrap();

        // Expect error when deserializing as BlockHeader<i32>
        let result: Result<(i32, Vec<u8>), _> = c.get_as(&id);
        assert!(result.is_err());
    }
}
