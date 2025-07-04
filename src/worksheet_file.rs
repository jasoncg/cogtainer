use std::collections::{BTreeMap, HashMap, VecDeque};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum WorksheetFile {
    V1(WorksheetFileV1),
}

/// A (Column, Row) address
#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Address(u64, u64);

/// The Worksheet has 3 parts:
/// - Header: Magic Number, Version, Footer Offset
/// - Block Data
/// - Footer
///
/// The Magic Number is a fixed string defining the file format ("DCWS")
/// The version is a u64 number indicating the current version (1)
/// The Footer Offset isa u64 in bytes indicating the start of the footer
///
/// The rest of the data up to the Footer Offset is the block data. The particular format of this data
/// is defined in the footer.
///
/// The footer provides metadata about the worksheet, as well as the layout of the block data region.
/// This includes
/// - actively used blocks
/// - empty space that can be used for expansion/reallocation
///
///
///
#[derive(Serialize, Deserialize)]
pub struct WorksheetFileV1 {
    /// The first populated cell (top-left)
    pub first_cell: Address,
    /// the last populated cell (bottom-right)
    pub last_cell: Address,

    /// all blocks are fixed-size based on this value, e.g. 100x100
    pub block_size: u32,

    // If the user changes a column or row more than a few px, sets the custom size.
    pub column_widths: HashMap<u64, f64>,
    pub row_heights: HashMap<u64, f64>,

    /// Blocks are organized as fixed-sized grids. Address is the top-left address of the block.
    /// e.g. if BLOCK_SIZE is 100, then the first block is (0,0), the next block to the right is
    /// (100, 0), etc.
    ///
    /// The value is the offset location within the file where the block is stored. Blocks are
    /// not in any guaranteed order.
    pub blocks: HashMap<Address, BlockDescriptor>,

    /// When a block is removed (or moved to the end if it's too big), its
    /// space is merged into the empty_space list for use when another block is needed or
    /// to ease defragmenting. (neighboring BlockDescriptors are merged together)
    pub empty_space: Vec<BlockDescriptor>,

    pub history: VecDeque<SheetAction>,
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct BlockDescriptor {
    pub file_offset: u64,
    pub length: u32,
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct Reference {
    /// worksheet 0 is this sheet. The others are based on connected inputs.
    pub worksheet: u64,
    pub address: Address,
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum CellValue {
    Formula {
        formula: String,

        /// If a refernced cell is changed, and this cell is not currently in scope,
        /// the dirty flag will be set instead of running the formula instantly.
        dirty: bool,
        /// caches the computed value of the formula, or the resulting error
        result: Result<rmpv::Value, String>,
    },
    Text(String),

    /// Simply mirrors whatever is at the referenced location.
    /// (This is always checked when the cell enters scope, so no need for a
    /// dirty flag)
    Reference(Reference),
}
#[derive(Serialize, Deserialize, PartialEq)]
pub struct Cell {
    pub value: CellValue,
    /// Used for recalculations. When the value of this cell changes,
    /// the other cells in this list will be updated
    pub referenced_by: Vec<Reference>,
    //style: Style, // todo/future work
}

#[derive(Serialize, Deserialize, PartialEq)]
pub struct WorksheetBlockDataSparse {
    pub cells: BTreeMap<Address, Cell>,
}

#[derive(Serialize, Deserialize, PartialEq)]
pub struct WorksheetBlockDataDense {
    /// It's a Vec, but will always be exactly block_size*block_size values
    /// (serde struggles with arrays)
    pub cells: Vec<Option<Cell>>,
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum WorksheetBlockData {
    Sparse(WorksheetBlockDataSparse),
    Dense(WorksheetBlockDataDense),
}

#[derive(Serialize, Deserialize)]
pub enum SheetAction {
    ColumnResize {
        column: u64,
        old_value: f64,
        new_value: f64,
    },
    RowResize {
        row: u64,
        old_value: f64,
        new_value: f64,
    },
    Cell {
        address: Address,
        old_value: CellValue,
        new_value: CellValue,
    },
}
