//! Stores (`docs/architecture/storage.md`): each store is a separate RocksDB instance, each table a
//! column family. Modules describe their tables with [`Table`]; this crate does not know their
//! keys and values.

use std::path::Path;

use rocksdb::{BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, DB, IteratorMode, Options};

/// Tuning profiles of `docs/architecture/storage.md`, "Engine".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Bloom filter.
    PointLookup,
    /// Blob files.
    LargeValues,
    Sequential,
}

pub trait Table {
    const NAME: &'static str;
    const PROFILE: Profile;
    type Key;
    type Value;

    fn encode_key(key: &Self::Key) -> Vec<u8>;
    fn decode_key(bytes: &[u8]) -> Result<Self::Key, DecodeError>;
    fn encode_value(value: &Self::Value) -> Vec<u8>;
    fn decode_value(bytes: &[u8]) -> Result<Self::Value, DecodeError>;
}

pub type Entry<T> = (<T as Table>::Key, <T as Table>::Value);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableSpec {
    pub name: &'static str,
    pub profile: Profile,
}

impl TableSpec {
    pub fn of<T: Table>() -> Self {
        Self {
            name: T::NAME,
            profile: T::PROFILE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DecodeError(pub String);

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Engine(#[from] rocksdb::Error),
    #[error("table {0} was not opened with the store")]
    NoTable(&'static str),
    #[error("cannot decode an entry of table {table}: {error}")]
    Decode {
        table: &'static str,
        error: DecodeError,
    },
}

pub struct Store {
    db: DB,
}

impl Store {
    /// Creates the directory and the tables if missing.
    pub fn open(path: &Path, tables: &[TableSpec]) -> Result<Self, StoreError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let tables = tables
            .iter()
            .map(|table| ColumnFamilyDescriptor::new(table.name, tuning(table.profile)));
        let db = DB::open_cf_descriptors(&options, path, tables)?;
        Ok(Self { db })
    }

    pub fn put<T: Table>(&self, key: &T::Key, value: &T::Value) -> Result<(), StoreError> {
        let table = self.table::<T>()?;
        self.db
            .put_cf(table, T::encode_key(key), T::encode_value(value))?;
        Ok(())
    }

    /// Deleting a missing key is not an error.
    pub fn delete<T: Table>(&self, key: &T::Key) -> Result<(), StoreError> {
        let table = self.table::<T>()?;
        self.db.delete_cf(table, T::encode_key(key))?;
        Ok(())
    }

    pub fn load_all<T: Table>(&self) -> Result<Vec<Entry<T>>, StoreError> {
        let table = self.table::<T>()?;
        self.db
            .iterator_cf(table, IteratorMode::Start)
            .map(|entry| {
                let (key, value) = entry?;
                let decode = |error| StoreError::Decode {
                    table: T::NAME,
                    error,
                };
                let key = T::decode_key(&key).map_err(decode)?;
                let value = T::decode_value(&value).map_err(decode)?;
                Ok((key, value))
            })
            .collect()
    }

    fn table<T: Table>(&self) -> Result<&ColumnFamily, StoreError> {
        self.db
            .cf_handle(T::NAME)
            .ok_or(StoreError::NoTable(T::NAME))
    }
}

fn tuning(profile: Profile) -> Options {
    let mut options = Options::default();
    match profile {
        Profile::PointLookup => {
            let mut table = BlockBasedOptions::default();
            table.set_bloom_filter(10.0, false);
            options.set_block_based_table_factory(&table);
        }
        Profile::LargeValues => options.set_enable_blob_files(true),
        Profile::Sequential => {}
    }
    options
}
