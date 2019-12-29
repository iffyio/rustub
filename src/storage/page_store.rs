use crate::storage::disk_manager::DiskManager;
use crate::storage::page::{Page, PageId};
use crate::storage::page_directory::PageDirectory;
use std::error::Error;
use std::io::Error as IOError;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    PageNotExist(PageId),
    NoAvailableSpace,
    IOError(IOError),
}

impl Error for StorageError {
    fn description(&self) -> &str {
        match self {
            Self::PageNotExist(_) => "Page does not exist",
            Self::NoAvailableSpace => "No space available in storage",
            Self::IOError(_) => "An I/O error occurred",
        }
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::PageNotExist(page_id) => write!(f, "page {:?} does not exist", page_id),
            Self::NoAvailableSpace => write!(f, "no space available in storage"),
            Self::IOError(error) => write!(f, "{}", error),
        }
    }
}

impl From<IOError> for StorageError {
    fn from(error: IOError) -> Self {
        Self::IOError(error)
    }
}

/// Handle page allocation using some underlying storage.
pub trait PageStore {
    /// Allocate a new page and return its page id.
    fn allocate_page(&mut self) -> Result<PageId>;

    /// Retrieve the page with the given page id.
    fn fetch_page(&mut self, page_id: PageId) -> Result<Page>;

    /// Release any resources for the page with the given id.
    fn deallocate_page(&mut self) {}

    /// Write the given page out to the underlying storage.
    fn write_page(&mut self, _page: &Page) -> Result<()> {
        Ok(())
    }
}

/// Manages pages backed by disk.
pub struct DiskPageStore {
    page_directory: PageDirectory,
    disk: DiskManager,
}

impl DiskPageStore {
    pub(crate) fn new(page_directory: PageDirectory, disk: DiskManager) -> Self {
        DiskPageStore {
            page_directory,
            disk,
        }
    }
}

impl PageStore for DiskPageStore {
    fn allocate_page(&mut self) -> Result<PageId> {
        Ok(self.page_directory.new_page_id())
    }

    fn fetch_page(&mut self, page_id: PageId) -> Result<Page> {
        let offset = self.page_directory.get_or_create_offset(&page_id);
        let page = self.disk.read_page(offset)?;
        Ok(page)
    }

    fn deallocate_page(&mut self) {
        unimplemented!()
    }

    fn write_page(&mut self, page: &Page) -> Result<()> {
        let page_id = page.get_id();

        let offset = self.page_directory.get_or_create_offset(&page_id);

        self.disk.write_page(page, offset)?;
        Ok(())
    }
}
