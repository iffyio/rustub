use crate::buffer::replacer::PageState;
use crate::storage::page::PAGE_SIZE;
use crate::storage::page::SLOT_SIZE_BYTES;
use crate::storage::page::{Page, PageId};
use crate::storage::page_directory::FreeSpaceDirectory;
use crate::storage::page_store::{PageStore, StorageError};
use log;
use std::collections::hash_map;
use std::collections::HashMap;
use std::error::Error;

type Result<T> = std::result::Result<T, BufferPoolError>;

#[derive(Debug)]
pub enum BufferPoolError {
    PageNotExist(PageId),
    NoSpaceInPool,
    PageNotPinned(PageId),
    PageNotInPool(PageId),
    StorageError(Box<dyn Error>),
}

impl Error for BufferPoolError {
    fn description(&self) -> &str {
        match self {
            Self::PageNotExist(_) => "Page does not exist",
            Self::NoSpaceInPool => "Buffer pool is full",
            Self::PageNotPinned(_) => "Page is not currently pinned so it cannot be unpinned",
            Self::PageNotInPool(_) => "Page is not in the buffer pool",
            Self::StorageError(_) => "The underlying storage encountered an error",
        }
    }
}
impl std::fmt::Display for BufferPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::PageNotExist(page_id) => write!(f, "page {:?} does not exist", page_id),
            Self::NoSpaceInPool => write!(f, "Buffer pool is full"),
            Self::PageNotPinned(page_id) => write!(
                f,
                "Page {:?} is not currently pinned so it cannot be unpinned",
                page_id
            ),
            Self::PageNotInPool(page_id) => {
                write!(f, "Page {:?} is not in the buffer pool", page_id)
            }
            Self::StorageError(err) => write!(f, "{}", err),
        }
    }
}

impl From<StorageError> for BufferPoolError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::PageNotExist(page_id) => Self::PageNotExist(page_id),
            error @ _ => Self::StorageError(Box::new(error)),
        }
    }
}

struct Frame {
    // next is the index of the next frame in the free list.
    next: Option<usize>,
    page: Page,
}

impl Default for Frame {
    fn default() -> Self {
        Frame {
            next: None,
            page: Page::default(),
        }
    }
}

struct PageResidence {
    frame_index: usize,
    pins: usize,
    state: PageState,
}

pub struct BufferPoolManager<S> {
    pool: Vec<Frame>,
    page_table: HashMap<PageId, PageResidence>,
    free_space_directory: FreeSpaceDirectory,
    free_list: Option<usize>,
    storage: Box<S>,
}

/// Iterate through all pages in the pool.
struct PoolIterator<'a, S> {
    page_residences: hash_map::Values<'a, PageId, PageResidence>,
    buffer_pool: &'a BufferPoolManager<S>,
}

impl<'a, S> Iterator for PoolIterator<'a, S> {
    type Item = &'a Page;

    fn next(&mut self) -> Option<Self::Item> {
        self.page_residences
            .next()
            .map(|residence| residence.frame_index)
            .map(|frame_index| &self.buffer_pool.pool[frame_index].page)
    }
}

impl<S: PageStore> BufferPoolManager<S> {
    pub fn new(
        size: usize,
        storage: S,
        free_space_directory: FreeSpaceDirectory,
    ) -> BufferPoolManager<S> {
        let mut pool = Vec::with_capacity(size);
        pool.resize_with(size, Frame::default);

        // Initially all frames are in the free list.
        for (i, frame) in pool.iter_mut().enumerate().rev() {
            frame.next = if i == size - 1 { None } else { Some(i + 1) };
        }

        BufferPoolManager {
            pool,
            page_table: HashMap::new(),
            free_space_directory,
            free_list: Some(0),
            storage: Box::new(storage),
        }
    }

    /// Returns the requested page from the pool, retrieving it
    /// from the underlying storage if necessary.
    /// The page is pinned by the caller and must be unpinned at
    /// some point in the future.
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<&mut Page> {
        let page_meta = match self.page_table.get_mut(&page_id) {
            // The page is in the buffer pool.
            Some(page_meta) => page_meta,
            None => {
                // The page is not in the buffer pool. Fetch it from storage
                // and insert it into the pool.
                let page = self.storage.fetch_page(page_id)?;
                self.insert_page(page)?
            }
        };

        // Mark page as being used by the caller.
        page_meta.pins += 1;

        let frame_index = page_meta.frame_index;
        Ok(&mut self.pool[frame_index].page)
    }

    /// Creates a new page in the pool.
    pub fn new_page(&mut self) -> Result<&mut Page> {
        let page_id = self.storage.allocate_page()?;
        let page_meta = self.insert_page(Page::new(page_id))?;
        let frame_index = page_meta.frame_index;
        Ok(&mut self.pool[frame_index].page)
    }

    /// Returns a page with enough space for a tuple of the specified size.
    /// The space is immediately claimed for the tuple within the page.
    /// The page returned page is pinned.
    pub fn assign_page(&mut self, tuple_size: u16) -> Result<&mut Page> {
        let reserved_bytes = tuple_size as u16 + SLOT_SIZE_BYTES;
        match self.free_space_directory.acquire_bytes(reserved_bytes) {
            // Note: calling fetch_page at the end of both branches pins the page.
            Some(page_id) => self.fetch_page(page_id),
            None => {
                let page_id = {
                    // We're ignoring the new page and re-fetching it outside this block
                    // to avoid borrowing `&mut self` multiple times.
                    let page = self.new_page()?;
                    page.get_id().clone()
                };
                self.free_space_directory
                    .add_page(page_id.clone(), PAGE_SIZE - reserved_bytes);
                self.fetch_page(page_id)
            }
        }
    }

    /// Marks the page as no longer in-use by one less party.
    pub fn unpin_page(&mut self, page_id: PageId, modified: PageState) -> Result<()> {
        match self.page_table.get_mut(&page_id) {
            Some(page_meta) if page_meta.pins > 0 => {
                // The page is currently pinned by someone so we can remove a pin.
                page_meta.pins -= 1;

                // Mark the page as such if it has been updated.
                if modified == PageState::DIRTY {
                    page_meta.state = modified
                }

                // Update the free space directory in case that has changed
                // for the page.
                let frame_index = page_meta.frame_index;
                let free_space = self.pool[frame_index].page.free_space();
                self.free_space_directory.update(page_id, free_space);

                Ok(())
            }
            // The page is not pinned so it cannot be unpinned.
            Some(_) => Err(BufferPoolError::PageNotPinned(page_id)),

            // We don't have the page in the pool.
            // And we currently don't swap a page out while it is
            // still pinned so this must be an error.
            None => Err(BufferPoolError::PageNotInPool(page_id)),
        }
    }

    /// Writes a page to the underlying storage.
    /// Does nothing if the page has not been modified or the page is currently pinned.
    /// Returns true if the operation was successful
    pub fn flush_page(&mut self, page_id: PageId) -> Result<bool> {
        if let Some(page_meta) = self.page_table.get_mut(&page_id) {
            let frame = &mut self.pool[page_meta.frame_index];

            if page_meta.pins == 0 && page_meta.state == PageState::DIRTY {
                self.storage.write_page(&frame.page)?;
                // Reset the page state to clean now that we have written it out.
                page_meta.state = PageState::CLEAN;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Err(BufferPoolError::PageNotInPool(page_id))
        }
    }

    /// Flushes all unpinned, dirty pages to the underlying storage.
    fn flush_all_pages(&mut self) -> Result<()> {
        let mut pinned_pages = Vec::new();
        let mut num_pinned_pages = 0;
        let mut num_pages_written = 0;

        for (page_id, page_meta) in self.page_table.iter() {
            if page_meta.pins == 0 {
                if page_meta.state == PageState::DIRTY {
                    self.storage
                        .write_page(&self.pool[page_meta.frame_index].page)?;
                    num_pages_written += 1;
                }
            } else {
                num_pinned_pages += 1;
                if pinned_pages.len() < 10 {
                    // Just show up to ten page ids to give a picture.
                    pinned_pages.push(page_id);
                }
            }
        }

        if num_pages_written > 0 {
            log::info!("Flushed {} pages", num_pages_written);
        } else {
            log::info!("There were no pages to flush");
        }
        if num_pinned_pages > 0 {
            log::warn!(
                "{} pages were still pinned and were not written {:?}",
                num_pinned_pages,
                pinned_pages
            );
        }
        Ok(())
    }

    /// Shutdown the buffer pool and free its resources.
    pub fn close(mut self) -> Result<()> {
        self.flush_all_pages()
    }

    /// Inserts a page into the pool.
    fn insert_page(&mut self, page: Page) -> Result<&mut PageResidence> {
        match self.free_list {
            // We have a free frame at the head of the free list
            Some(index) => {
                // Update the head of the free list to the next free frame if any.
                let frame = &mut self.pool[index];
                self.free_list = frame.next.clone();

                // Add the page to the frame
                let page_id = page.get_id();
                frame.page = page;

                // Add the page meta to the page table.
                self.page_table.insert(
                    page_id.clone(),
                    PageResidence {
                        pins: 0,
                        frame_index: index,
                        state: PageState::CLEAN,
                    },
                );

                // unwrap is safe here since we just inserted the page.
                Ok(self.page_table.get_mut(&page_id).expect(
                    format!("BUG: Missing entry in page table that was just inserted").as_str(),
                ))
            }
            None => Err(BufferPoolError::NoSpaceInPool), // TODO evict
        }
    }

    fn iter(&self) -> PoolIterator<S> {
        PoolIterator {
            page_residences: self.page_table.values(),
            buffer_pool: self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BufferPoolManager;
    use crate::storage::page::{Page, PageId};
    use crate::storage::page_directory::FreeSpaceDirectory;
    use crate::storage::page_store::{self, PageStore, StorageError};
    use std::collections::HashMap;

    struct MockPageStore {
        storage: HashMap<PageId, Page>,
    }
    impl MockPageStore {
        fn new(storage: HashMap<PageId, Page>) -> Self {
            MockPageStore { storage }
        }
    }

    #[test]
    fn initial_free_list() {
        impl PageStore for MockPageStore {
            fn allocate_page(&mut self) -> page_store::Result<PageId> {
                Err(StorageError::NoAvailableSpace)
            }

            fn fetch_page(&mut self, page_id: PageId) -> page_store::Result<Page> {
                match self.storage.remove(&page_id) {
                    Some(page) => Ok(page),
                    None => Err(StorageError::PageNotExist(page_id)),
                }
            }
        }

        let b = BufferPoolManager::new(
            10,
            MockPageStore::new(HashMap::new()),
            FreeSpaceDirectory::new(HashMap::new()),
        );
        let free_list = Some(0);
        assert_eq!(b.free_list, free_list);

        let mut free_list_size = 0;
        let mut curr = &free_list;
        while let Some(index) = curr {
            free_list_size += 1;
            curr = &b.pool[*index as usize].next;
        }

        assert_eq!(free_list_size, 10);
    }

    #[test]
    fn fetch_page() {
        let mut pages = HashMap::new();
        pages.insert(PageId(1), Page::new(PageId(1)));
        pages.insert(PageId(2), Page::new(PageId(2)));

        let mut b = BufferPoolManager::new(
            10,
            MockPageStore::new(pages),
            FreeSpaceDirectory::new(HashMap::new()),
        );

        assert!(b.fetch_page(PageId(0)).is_err());
        // Fetch from storage.
        assert_eq!((*b.fetch_page(PageId(1)).unwrap()).get_id(), PageId(1));
        assert_eq!((*b.fetch_page(PageId(2)).unwrap()).get_id(), PageId(2));
        // Fetch from page cache.
        assert_eq!((*b.fetch_page(PageId(1)).unwrap()).get_id(), PageId(1));
        assert_eq!((*b.fetch_page(PageId(2)).unwrap()).get_id(), PageId(2));
    }

    #[test]
    fn allocate_page() {
        use std::collections::HashSet;
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

        let mut b = BufferPoolManager::new(
            3,
            MockPageStore::new(HashMap::new()),
            FreeSpaceDirectory::new(HashMap::new()),
        );

        let page_ids = vec![
            b.new_page().unwrap().get_id(),
            b.new_page().unwrap().get_id(),
            b.new_page().unwrap().get_id(),
        ];

        // Check unique page ids were returned.
        assert_eq!(
            page_ids
                .iter()
                .map(|page_id| page_id.clone().0)
                .collect::<HashSet<u32>>()
                .len(),
            page_ids.len()
        );

        // Check that the pages are now in the cache.
        page_ids
            .iter()
            .map(|page_id| page_id.clone())
            .for_each(|page_id| {
                assert_eq!((*b.fetch_page(page_id.clone()).unwrap()).get_id(), page_id);
            });
    }
}
