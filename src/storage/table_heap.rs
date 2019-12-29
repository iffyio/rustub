use crate::buffer::buffer_pool_manager::BufferPoolError;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::replacer::PageState;
use crate::storage::disk_manager::DiskManager;
use crate::storage::page::Tuple;
use crate::storage::page::PAGE_DATA_SIZE;
use crate::storage::page::SLOT_SIZE_BYTES;
use crate::storage::page::{TupleId, PAGE_SIZE};
use crate::storage::page_directory::PageDirectory;
use crate::storage::page_directory::FreeSpaceDirectory;
use crate::storage::page_store::DiskPageStore;
use crate::storage::page_store::PageStore;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::error::Error;
use std::io::{Error as IOError, ErrorKind};
use std::path::Path;

#[derive(Debug)]
pub enum TableError {
    TupleNotFound(TupleId),
    BufferPoolError(Box<dyn Error>),
}

impl Error for TableError {
    fn description(&self) -> &str {
        match self {
            Self::TupleNotFound(_) => "Tuple was not found",
            Self::BufferPoolError(_) => "The buffer pool manager encountered an error",
        }
    }
}
impl std::fmt::Display for TableError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::TupleNotFound(tid) => write!(f, "tuple was not found {:?}", tid),
            Self::BufferPoolError(err) => write!(f, "{}", err),
        }
    }
}

impl From<BufferPoolError> for TableError {
    fn from(error: BufferPoolError) -> Self {
        Self::BufferPoolError(Box::new(error))
    }
}

pub struct TableHeap<S> {
    buffer_pool: BufferPoolManager<S>,
}

impl<S> TableHeap<S>
where
    S: PageStore,
{
    pub fn new(buffer_pool: BufferPoolManager<S>) -> Result<TableHeap<S>, Box<dyn Error>> {
        Ok(TableHeap { buffer_pool })
    }

    pub fn from_page_file<P: AsRef<Path>>(
        page_file_path: &P,
        buffer_pool_size: usize,
    ) -> Result<TableHeap<DiskPageStore>, Box<dyn Error>> {
        let mut disk = DiskManager::new(page_file_path, PAGE_SIZE)?;

        // Load initial page directory info from page file.
        struct PageInfo {
            free_space: u16,
            offset: u64,
        }
        let directory = disk.visit_pages(
            |page, offset, mut acc| match acc.entry(page.get_id()) {
                Entry::Vacant(entry) => {
                    entry.insert(PageInfo {
                        free_space: page.free_space(),
                        offset,
                    });
                    Ok(acc)
                }
                Entry::Occupied(_) => Err(Box::new(IOError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "duplicate page id {:?} found in file {:?} ",
                        page.get_id(),
                        page_file_path.as_ref()
                    ),
                ))),
            },
            HashMap::new(),
        )?;

        let offsets = directory
            .iter()
            .map(|(page_id, &PageInfo { offset, .. })| (page_id.clone(), offset))
            .collect();

        let initial_unused_bytes = directory
            .iter()
            .map(|(page_id, &PageInfo { free_space, .. })| (page_id.clone(), free_space))
            .collect();

        TableHeap::new(BufferPoolManager::new(
            buffer_pool_size,
            DiskPageStore::new(PageDirectory::new(offsets), disk),
            FreeSpaceDirectory::new(initial_unused_bytes),
        ))
    }

    /// Finds a page with enough space and inserts the tuple in that page.
    pub fn insert_tuple(&mut self, tuple: &[u8]) -> Result<TupleId, TableError> {
        let tuple_size = tuple.len();
        if tuple_size > (PAGE_DATA_SIZE - SLOT_SIZE_BYTES) as usize {
            panic!(
                "Too large tuple ({} bytes) - should be less than {} bytes",
                tuple_size,
                std::u16::MAX
            );
        }

        let reserved_bytes = tuple.len() as u16 + SLOT_SIZE_BYTES;
        let page = self.buffer_pool.assign_page(reserved_bytes)?;

        // Unwrapping is safe here because we made sure that the page
        // had enough space for the tuple.
        let tid = page
            .insert_tuple(tuple)
            .expect("Unable to insert page into tuple");

        // Mark the page as dirty and notify we're done with it.
        let page_id = page.get_id();
        self.buffer_pool.unpin_page(page_id, PageState::DIRTY)?;

        Ok(tid)
    }

    /// Reads a tuple from the buffer pool or storage.
    pub fn get_tuple(&mut self, tid: TupleId) -> Result<Tuple, TableError> {
        let page = self.buffer_pool.fetch_page(tid.page_id.clone())?;
        let tuple = match page.get_tuple(tid.clone()) {
            Some(tuple) => Ok(tuple),
            None => Err(TableError::TupleNotFound(tid)),
        };

        // Notify we're done with the page.
        let page_id = page.get_id();
        self.buffer_pool.unpin_page(page_id, PageState::CLEAN)?;

        tuple
    }

    /// Deletes a tuple.
    /// The delete is durable when the page gets written out to storage.
    pub fn delete_tuple(&mut self, tid: TupleId) -> Result<(), TableError> {
        let page = self.buffer_pool.fetch_page(tid.page_id.clone())?;

        let tuple_exists = page.remove(tid.clone());
        if !tuple_exists {
            self.buffer_pool
                .unpin_page(tid.page_id.clone(), PageState::CLEAN)?;
            return Err(TableError::TupleNotFound(tid));
        }

        self.buffer_pool.unpin_page(tid.page_id, PageState::DIRTY)?;

        Ok(())
    }

    /// Closes the table heap and its resources.
    pub fn close(self) -> Result<(), TableError> {
        self.buffer_pool.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::TableHeap;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::storage::page::Page;
    use crate::storage::page::PageId;
    use crate::storage::page::PAGE_DATA_SIZE;
    use crate::storage::page::SLOT_SIZE_BYTES;
    use crate::storage::page_directory::FreeSpaceDirectory;
    use crate::storage::page_store;
    use crate::storage::page_store::PageStore;
    use crate::storage::page_store::StorageError;
    use std::collections::HashMap;

    #[test]
    fn insert_and_get_tuple() -> Result<(), Box<dyn std::error::Error>> {
        struct MockPageStore {
            next_page_id: u32,
            storage: HashMap<PageId, Page>,
        }
        impl MockPageStore {
            fn new(storage: HashMap<PageId, Page>) -> Self {
                let next_page_id = storage
                    .keys()
                    .max_by_key(|page_id| page_id.0)
                    .map(|page_id| page_id.0 + 1)
                    .unwrap_or(0);

                MockPageStore {
                    storage,
                    next_page_id,
                }
            }
        }
        impl PageStore for MockPageStore {
            fn allocate_page(&mut self) -> page_store::Result<PageId> {
                let page_id = self.next_page_id;
                self.next_page_id += 1;
                Ok(PageId(page_id))
            }

            fn fetch_page(&mut self, page_id: PageId) -> page_store::Result<Page> {
                match self.storage.remove(&page_id) {
                    Some(page) => Ok(page),
                    None => Err(StorageError::PageNotExist(page_id)),
                }
            }
        }

        let unused_bytes = FreeSpaceDirectory::new(HashMap::new());
        let buffer_pool =
            BufferPoolManager::new(3, MockPageStore::new(HashMap::new()), unused_bytes);

        let mut table = TableHeap::new(buffer_pool)?;

        let t1 = [0xfa, 0xce];
        let t2 = [0xfa, 0xce, 0xef, 0xc0, 0xed];
        let t3 = [0xbe; (PAGE_DATA_SIZE - SLOT_SIZE_BYTES) as usize];
        let tid1 = table.insert_tuple(&t1)?;
        let tid2 = table.insert_tuple(&t2)?;
        let tid3 = table.insert_tuple(&t3)?;
        let read_t1 = table.get_tuple(tid1.clone())?;
        let read_t2 = table.get_tuple(tid2.clone())?;
        let read_t3 = table.get_tuple(tid3.clone())?;

        assert_eq!(&t1, read_t1.as_ref());
        assert_eq!(&t2, read_t2.as_ref());
        assert_eq!(&t3[..], read_t3.as_ref());

        assert_eq!(&tid1.page_id, &tid2.page_id);
        assert_eq!(PageId(tid1.page_id.0 + 1), tid3.page_id);

        Ok(())
    }
}
