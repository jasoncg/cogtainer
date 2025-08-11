cogtainer is a file container format, similar to a zip file, written in Rust. It's designed to be used as a binary file format for applications needing to save "keyed" data that can be freely removed without having to regenerate the entire file.

# Key features
- Store many "blocks" of data with custom keys. This is similar to files with file paths, except the "paths" in this case can be strings or custom binary data.
- Delete blocks in place.
- Optional defragmententation. As blocks are deleted and new blocks created, they will attempt to fit inside empty space.
- Optional/configurable overprovision space for new blocks (decreasing the chance that changes to a block will grow the file and result in fragmentation).
- Block Identifier can be:
  - String
  - Unsigned 64 bit Integner
  - Bytes (Vec<u8>)
  - Path (Vector of Strings)
- Blocks can have arbitrary metadata

# Format Description

A Cogtainer file consists of:
- A fixed-size header
  - 4 byte magic number "DCCF"
  - 8 byte version
  - 8 byte offset location to file footer
  - 8 byte checksum of file footer
  - 8*4 bytes reserved
- The "chunks" making up the stored data.
- Footer
  - rmpv::Value serialized metadata (custom to application)
  - HashMap<Idenfiter, BlockDescriptor> listing all allocated blocks in the file
  - BTreeMap<Offset, Length> listing empty regions in the file


# License
Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
