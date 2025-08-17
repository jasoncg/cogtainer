use crate::{basic_api::*, container_file::*};
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
fn internal_file_read_on_missing_block_errors_then_write_creates() {
    let mut c = open_container();
    let id = Identifier::String("nf".into());

    // Acquire file-like handle to a block that doesn't exist yet
    let mut f = c.get_block_as_file(&id);

    // Read should error with NotFound/InvalidInput (current impl uses InvalidInput)
    let mut buf = [0u8; 4];
    let err = f.read(&mut buf).unwrap_err();
    assert!(
        matches!(err.kind(), std::io::ErrorKind::InvalidInput)
            || matches!(err.kind(), std::io::ErrorKind::NotFound)
    );

    // Write should create the block and advance cursor
    let wrote = f.write(b"hello").unwrap();
    assert_eq!(wrote, 5);

    // EOF read at end
    let n = f.read(&mut buf).unwrap();
    assert_eq!(n, 0);

    // Drop `f` to release &mut borrow, then verify via high-level API
    drop(f);
    let (_m, data) = c.get_block(&id).unwrap();
    assert_eq!(data, b"hello");
}

#[test]
fn internal_file_seek_semantics_start_end_current() {
    let mut c = open_container();
    let id = Identifier::U64(1);
    c.insert_block(&id, rmpv::Value::Nil, b"abcdefghij")
        .unwrap(); // len 10
    c.flush().unwrap();

    let mut f = c.get_block_as_file(&id);

    // Seek to middle and read tail
    assert_eq!(f.seek(SeekFrom::Start(5)).unwrap(), 5);
    let mut buf = [0u8; 10];
    let n = f.read(&mut buf).unwrap();
    assert_eq!(n, 5);
    assert_eq!(&buf[..n], b"fghij");

    // Seek from end(0) => EOF, read => 0
    assert_eq!(f.seek(SeekFrom::End(0)).unwrap(), 10);
    assert_eq!(f.read(&mut buf).unwrap(), 0);

    // Seek from end(-3) then read
    assert_eq!(f.seek(SeekFrom::End(-3)).unwrap(), 7);
    let mut three = [0u8; 3];
    assert_eq!(f.read(&mut three).unwrap(), 3);
    assert_eq!(&three, b"hij");

    // Seek current negative past start should error and not change cursor
    assert_eq!(f.seek(SeekFrom::Start(2)).unwrap(), 2);
    let err = f.seek(SeekFrom::Current(-5)).unwrap_err();
    assert!(matches!(err.kind(), std::io::ErrorKind::InvalidInput));
    // Cursor unchanged (still 2)
    assert_eq!(f.seek(SeekFrom::Current(0)).unwrap(), 2);
}

#[test]
fn internal_file_read_past_eof_returns_zero() {
    let mut c = open_container();
    let id = Identifier::U64(2);
    c.insert_block(&id, rmpv::Value::Nil, b"xyz").unwrap();
    c.flush().unwrap();

    let mut f = c.get_block_as_file(&id);
    f.seek(SeekFrom::Start(100)).unwrap(); // way past EOF
    let mut buf = [0u8; 8];
    assert_eq!(f.read(&mut buf).unwrap(), 0);
}

#[test]
fn internal_file_in_place_write_within_allocation_zero_fills_gap_and_updates_used() {
    let mut c = open_container();
    let id = Identifier::U64(3);

    // Start with 8 bytes and 8 bytes overalloc (alloc >= 16)
    c.set_overallocation_policy(OverallocationPolicy::Bytes(8))
        .insert_block(&id, rmpv::Value::Nil, b"ABCDEFGH")
        .unwrap()
        .flush()
        .unwrap();

    // Write starting at position 12 (gap 8..12), which is within allocation
    {
        let mut f = c.get_block_as_file(&id);
        f.seek(SeekFrom::Start(12)).unwrap();
        f.write(b"ZZ").unwrap();
        f.flush().unwrap();
    } // drop f

    // Verify: used_length expanded to 14, gap zero-filled
    let block = c.get_blocks_list()[&id].clone();
    assert_eq!(block.used_length, 14);

    let (_m, data) = c.get_block(&id).unwrap();
    let mut expected = Vec::from(&b"ABCDEFGH"[..]); // 8
    expected.extend_from_slice(&[0u8; 4]); // gap [8..12]
    expected.extend_from_slice(b"ZZ"); // [12..14]
    assert_eq!(data, expected);
}

#[test]
fn internal_file_write_beyond_allocation_triggers_rebuild_and_extends() {
    let mut c = open_container();
    let id = Identifier::U64(4);

    // 8 bytes data with small overalloc (e.g., +4 -> alloc ~12)
    c.set_overallocation_policy(OverallocationPolicy::Bytes(4))
        .insert_block(&id, rmpv::Value::Nil, b"12345678")
        .unwrap()
        .flush()
        .unwrap();

    // Seek beyond current allocation (e.g., pos 20) and write 4 bytes
    {
        let mut f = c.get_block_as_file(&id);
        f.seek(SeekFrom::Start(20)).unwrap();
        f.write(b"WWWW").unwrap(); // should rebuild and extend
        f.flush().unwrap();
    }

    let block = c.get_blocks_list()[&id].clone();
    assert_eq!(block.used_length, 24);

    let (_m, data) = c.get_block(&id).unwrap();
    // Expect original 8, then zero gap [8..20], then "WWWW"
    let mut expected = Vec::from(&b"12345678"[..]);
    expected.extend_from_slice(&vec![0u8; 12]); // 8..20
    expected.extend_from_slice(b"WWWW");
    assert_eq!(data, expected);
}

#[test]
fn internal_file_cursor_advances_and_partial_reads() {
    let mut c = open_container();
    let id = Identifier::U64(5);
    c.insert_block(&id, rmpv::Value::Nil, b"short").unwrap();
    c.flush().unwrap();

    let mut f = c.get_block_as_file(&id);
    let mut buf = [0u8; 10];
    let n1 = f.read(&mut buf).unwrap();
    assert_eq!(n1, 5);
    assert_eq!(&buf[..n1], b"short");
    // Now at EOF
    let n2 = f.read(&mut buf).unwrap();
    assert_eq!(n2, 0);
}

#[test]
fn internal_file_flush_persists_changes_after_reopen() {
    let mut c = open_container();
    let id = Identifier::String("persist-if".into());

    {
        let mut f = c.get_block_as_file(&id);
        f.write(b"persist me").unwrap();
        f.flush().unwrap();
    }
    // Reopen container
    let buf = c.file.into_inner();
    let mut c2 = Cogtainer::open(Cursor::new(buf)).unwrap();

    let (m, data) = c2.get_block(&id).unwrap();
    assert_eq!(m, &rmpv::Value::Nil);
    assert_eq!(data, b"persist me");
}

#[test]
fn internal_file_write_empty_noop() {
    let mut c = open_container();
    let id = Identifier::U64(6);
    let mut f = c.get_block_as_file(&id);
    let n = f.write(&[]).unwrap();
    assert_eq!(n, 0);
    // Still no block created; subsequent read should error
    let mut buf = [0u8; 1];
    let err = f.read(&mut buf).unwrap_err();
    assert!(
        matches!(err.kind(), std::io::ErrorKind::InvalidInput)
            || matches!(err.kind(), std::io::ErrorKind::NotFound)
    );
}

#[test]
fn internal_file_overwrite_existing_region_in_place() {
    let mut c = open_container();
    let id = Identifier::U64(7);
    c.set_overallocation_policy(OverallocationPolicy::Bytes(16))
        .insert_block(&id, rmpv::Value::Nil, b"abcdefgh")
        .unwrap()
        .flush()
        .unwrap();

    {
        let mut f = c.get_block_as_file(&id);
        f.seek(SeekFrom::Start(2)).unwrap();
        f.write(b"ZZZ").unwrap(); // overwrite 'cde' -> 'ZZZ'
        f.flush().unwrap();
    }

    let (_m, data) = c.get_block(&id).unwrap();
    assert_eq!(&data, b"abZZZfgh");
}
