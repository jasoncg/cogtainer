#[cfg(test)]
mod defrag_tests {
    use crate::{basic_api::Cogtainer, container_file::*};

    use super::*;
    use std::io::{Cursor, Read, Seek, SeekFrom, Write};

    fn open_new_container() -> Cogtainer<Cursor<Vec<u8>>> {
        let file = Cursor::new(vec![0u8; 512 * 1024]);
        Cogtainer::create(file).unwrap()
    }

    #[test]
    fn defrag_no_empty_space_is_noop() {
        let mut c = open_new_container();
        let id = Identifier::String("block".into());
        c.insert_block(&id, rmpv::Value::Nil, b"hello").unwrap();
        let before = c.header.footer_offset.0;
        c.defragment().unwrap();
        assert_eq!(
            c.header.footer_offset.0, before,
            "Footer offset changed despite no empty space"
        );
    }

    #[test]
    fn defrag_single_hole_at_start_moves_block() {
        let mut c = open_new_container();
        // Insert A, B, C
        let ids: Vec<_> = (0..3).map(|i| i).collect();
        for id in &ids {
            c.insert_block(
                &Identifier::U64(*id),
                rmpv::Value::Nil,
                &vec![*id as u8 + 1; 32],
            )
            .unwrap();
        }
        // Delete A (creates hole at start)
        c.delete_block(&Identifier::U64(ids[0])).unwrap();
        let before_footer = c.header.footer_offset.0;
        c.defragment().unwrap();
        // The footer and blocks should be packed, no empty space before blocks.
        let block_offsets: Vec<u64> = ids[1..]
            .iter()
            .map(|id| c.footer.blocks[&Identifier::U64(*id)].file_offset.0)
            .collect();
        for (id, desc) in &c.footer.blocks {
            println!(
                "Block {:?}: offset={}, len={}",
                id, desc.file_offset.0, desc.allocated_length
            );
        }
        for (off, len) in &c.footer.empty_space {
            println!("Empty space at {} ({} bytes)", off.0, len);
        }
        println!("Footer at offset {}", c.header.footer_offset.0);
        assert_eq!(
            block_offsets[0],
            ContainerHeader::HEADER_SIZE as u64,
            "First block should be packed at file start"
        );
        assert!(
            c.footer.empty_space.is_empty(),
            "No empty space should remain"
        );
        assert!(
            c.header.footer_offset.0 < before_footer,
            "Footer should have moved left"
        );
    }

    #[test]
    fn defrag_multiple_holes_all_packed_and_single_gap_left() {
        let mut c = open_new_container();
        // Insert 5 blocks
        let ids: Vec<_> = (0..5).map(|i| i).collect();
        for id in &ids {
            c.insert_block(
                &Identifier::U64(*id),
                rmpv::Value::Nil,
                &vec![*id as u8; 16],
            )
            .unwrap();
        }
        // Delete 1 and 3 (creates two holes)
        c.delete_block(&Identifier::U64(ids[1])).unwrap();
        c.delete_block(&Identifier::U64(ids[3])).unwrap();
        // Verify two holes
        assert_eq!(c.footer.empty_space.len(), 2);
        c.defragment().unwrap();
        // All blocks should be packed, empty_space should be 0
        assert_eq!(c.footer.empty_space.len(), 0);
        let mut offsets: Vec<_> = c.footer.blocks.values().map(|b| b.file_offset.0).collect();
        offsets.sort();
        // Should be contiguous
        for w in offsets.windows(2) {
            let prev = w[0];
            let next = w[1];
            assert_eq!(next, prev + 16);
        }
    }

    #[test]
    fn defrag_blocks_of_varying_size() {
        let mut c = open_new_container();
        let ids: Vec<_> = (0..4).map(|i| i).collect();
        let sizes = [8, 64, 4, 128];
        for (id, size) in ids.iter().zip(sizes.iter()) {
            c.insert_block(
                &Identifier::U64(*id),
                rmpv::Value::Nil,
                &vec![0xA0 + *id as u8; *size],
            )
            .unwrap();
        }
        // Delete the largest (128), then the smallest (4)
        c.delete_block(&Identifier::U64(ids[3])).unwrap();
        c.delete_block(&Identifier::U64(ids[2])).unwrap();
        c.defragment().unwrap();
        // Check that the remaining blocks are packed tightly (8+64 bytes)
        let blocks = &c.footer.blocks;
        let block0 = blocks.get(&Identifier::U64(ids[0])).unwrap();
        let block1 = blocks.get(&Identifier::U64(ids[1])).unwrap();
        assert_eq!(block1.file_offset.0, block0.file_offset.0 + 8);
        // There should be no empty space
        assert_eq!(c.footer.empty_space.len(), 0);
    }

    #[test]
    fn defrag_with_metadata_only_blocks_and_zero_length() {
        let mut c = open_new_container();
        let meta_id = Identifier::String("meta".into());
        let zero_id = Identifier::String("zero".into());
        c.insert_block(&meta_id, rmpv::Value::from(42), &[])
            .unwrap();
        c.insert_block(&zero_id, rmpv::Value::Nil, &[]).unwrap();
        // Insert some data blocks
        let ids: Vec<_> = (0..3).map(|i| i).collect();
        for id in &ids {
            c.insert_block(
                &Identifier::U64(*id),
                rmpv::Value::Nil,
                &vec![*id as u8; 10],
            )
            .unwrap();
        }
        // Delete the middle data block
        c.delete_block(&Identifier::U64(ids[1])).unwrap();
        c.defragment().unwrap();
        // Metadata-only and zero-length blocks should not affect layout/defrag
        assert_eq!(c.footer.empty_space.len(), 0);
        for id in &[
            &meta_id,
            &zero_id,
            &Identifier::U64(ids[0]),
            &Identifier::U64(ids[2]),
        ] {
            let _ = c.get_block(id).unwrap();
        }
    }

    #[test]
    fn defrag_after_shrinking_overallocation_policy_empty_list_should_be_empty() {
        let mut c = open_new_container();
        // Insert with overallocation
        c.set_overallocation_policy(OverallocationPolicy::Bytes(32));
        let id = Identifier::U64(77);
        c.insert_block(&id, rmpv::Value::Nil, &[1, 2, 3, 4])
            .unwrap();
        // Delete the block, reinsert with no overallocation, but at new offset
        c.delete_block(&id).unwrap();
        c.set_overallocation_policy(OverallocationPolicy::None);
        c.insert_block(&id, rmpv::Value::Nil, &[1, 2, 3, 4])
            .unwrap();
        // Now delete and reinsert again, then defrag and ensure file is tight
        c.delete_block(&id).unwrap();
        c.defragment().unwrap();

        let empty_list_len = c.footer.empty_space.len();
        assert_eq!(
            empty_list_len, 0,
            "The empty tracker list should be empty after defragmenting "
        );
    }
    #[test]
    fn defrag_after_shrinking_overallocation_policy() {
        let mut c = open_new_container();
        // Insert with overallocation
        c.set_overallocation_policy(OverallocationPolicy::Bytes(32));
        let id = Identifier::U64(77);
        c.insert_block(&id, rmpv::Value::Nil, &[1, 2, 3, 4])
            .unwrap();
        // Delete the block, reinsert with no overallocation, but at new offset
        c.delete_block(&id).unwrap();
        c.set_overallocation_policy(OverallocationPolicy::None);
        c.insert_block(&id, rmpv::Value::Nil, &[1, 2, 3, 4])
            .unwrap();
        // Now delete and reinsert again, then defrag and ensure file is tight
        c.delete_block(&id).unwrap();
        c.defragment().unwrap();
        let last_offset = c.header.footer_offset.0;
        let expected = ContainerHeader::HEADER_SIZE as u64;
        assert_eq!(
            last_offset, expected,
            "Footer should be directly after header, no blocks present"
        );
    }

    #[test]
    fn defrag_does_not_affect_data_or_metadata() {
        let mut c = open_new_container();
        // Insert a bunch of blocks with unique data and metadata
        let mut expected = Vec::new();
        for i in 0..10 {
            let id = Identifier::U64(i);
            let data = vec![i as u8; 13 + (i % 5) as usize];
            let meta = rmpv::Value::from(format!("meta-{}", i));
            c.insert_block(&id, meta.clone(), &data).unwrap();
            expected.push((id, meta, data));
        }
        // Delete every other block
        for (i, (id, _, _)) in expected.iter().enumerate() {
            if i % 2 == 0 {
                c.delete_block(id).unwrap();
            }
        }
        c.defragment().unwrap();
        // Check all remaining data and metadata match
        for (i, (id, meta, data)) in expected.iter().enumerate() {
            if i % 2 == 1 {
                let (m2, d2) = c.get_block(id).unwrap();
                assert_eq!(meta, m2, "Metadata mismatch after defrag for id {:?}", id);
                assert_eq!(data, &d2, "Data mismatch after defrag for id {:?}", id);
            }
        }
    }

    #[test]
    fn defrag_does_not_move_blocks_when_already_packed() {
        let mut c = open_new_container();
        for i in 0..4 {
            c.insert_block(&Identifier::U64(i), rmpv::Value::Nil, &vec![i as u8; 8])
                .unwrap();
        }
        let orig_offsets: Vec<_> = (0..4)
            .map(|i| c.footer.blocks[&Identifier::U64(i)].file_offset.0)
            .collect();
        c.defragment().unwrap();
        let new_offsets: Vec<_> = (0..4)
            .map(|i| c.footer.blocks[&Identifier::U64(i)].file_offset.0)
            .collect();
        assert_eq!(
            orig_offsets, new_offsets,
            "Block offsets changed even though blocks were already packed"
        );
    }

    #[test]
    fn defrag_large_randomized_stress_test() {
        use rand::{seq::SliceRandom, Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xC0DEBEEF);
        let mut c = open_new_container();

        let mut ids = Vec::new();
        // Insert 100 blocks with random sizes and data
        for i in 0..100 {
            let id = Identifier::U64(i);
            let len = rng.gen_range(1..128);
            let data: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
            let meta = rmpv::Value::from(i as i64);
            c.insert_block(&id, meta.clone(), &data).unwrap();
            ids.push((id.clone(), meta, data));
        }
        // Randomly delete 40 blocks
        let mut to_delete = ids.clone();
        to_delete.shuffle(&mut rng);
        for (id, _, _) in to_delete.iter().take(40) {
            c.delete_block(id).unwrap();
        }
        // Defragment
        c.defragment().unwrap();
        // Check all remaining data matches
        for (id, meta, data) in ids.iter() {
            if c.footer.blocks.contains_key(id) {
                let (meta2, data2) = c.get_block(id).unwrap();
                assert_eq!(
                    meta, meta2,
                    "Metadata mismatch after stress defrag for id {:?}",
                    id
                );
                assert_eq!(
                    data, &data2,
                    "Data mismatch after stress defrag for id {:?}",
                    id
                );
            }
        }
        // There should be no holes
        assert_eq!(c.footer.empty_space.len(), 0);
    }

    // Add more pathological edge cases as you discover them!
}
