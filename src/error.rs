use thiserror::Error;

use crate::container_file::Identifier;

#[derive(Debug)]
pub enum HeaderError {
    Magic,
    Version,
    FooterOffset,
    FooterLength,
    FooterChecksum,
    Other(String),
}

#[derive(Error, Debug)]
pub enum CogtainerError {
    #[error("header contains invalid data or is corrupt {0:?}")]
    InvalidHeader(HeaderError),

    #[error("footer contains invalid data or is corrupt")]
    FooterChecksumError,

    #[error("block {0:?} contains invalid data or is corrupt")]
    BlockChecksumError(Identifier),

    #[error("file io error {0}")]
    IOError(#[from] std::io::Error),

    #[error("block {0:?} not found")]
    BlockNotFound(Identifier),

    #[cfg(feature = "full")]
    #[error("Unable to serialize: `{0}`")]
    Serialize(#[from] rmp_serde::encode::Error),

    #[cfg(feature = "full")]
    #[error("Unable to serialize: `{0}`")]
    RmpvSerialize(#[from] rmpv::ext::Error),

    #[cfg(feature = "full")]
    #[error("Unable to deserialize: `{0}`")]
    Deserialize(#[from] rmp_serde::decode::Error),

    #[error("unknown error")]
    Unknown,
}
