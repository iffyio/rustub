use crate::storage::page::Page;
use log;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Error as IOError, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

type PageOffset = u64;

/// Back pages with underlying storage using a single file on disk.
pub struct DiskManager {
    page_file: File,
    page_size: u16,
}

impl DiskManager {
    pub fn new<P: AsRef<Path>>(page_file_path: &P, page_size: u16) -> Result<Self, IOError> {
        let path = Path::new(page_file_path.as_ref().as_os_str());
        if path.exists() && !path.is_file() {
            return Err(IOError::new(
                ErrorKind::Other,
                format!(
                    "page file {:?} is not a regular file",
                    page_file_path.as_ref()
                ),
            ));
        }

        let page_file_size_bytes = if path.exists() {
            path.metadata()?.len()
        } else {
            0
        };
        if page_file_size_bytes % page_size as u64 != 0 {
            return Err(IOError::new(
                ErrorKind::InvalidData,
                format!(
                    "Invalid page file: file size is {} bytes while page size is {} bytes",
                    page_file_size_bytes, page_size
                ),
            ));
        }

        if page_file_size_bytes == 0 {
            log::trace!(
                "Creating new page file {:#}",
                page_file_path.as_ref().to_str().unwrap()
            );
        } else {
            log::trace!(
                "Page file found {:#} ({} bytes) containing {} pages ",
                page_file_path.as_ref().to_str().unwrap(),
                page_file_size_bytes,
                page_file_size_bytes / page_size as u64
            );
        }

        let page_file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(page_file_path)?;

        Ok(DiskManager {
            page_file,
            page_size,
        })
    }

    pub fn write_page(&mut self, page: &Page, offset: PageOffset) -> Result<(), IOError> {
        assert_eq!(
            offset % self.page_size as u64,
            0,
            "{}",
            format!("invalid page offset {} at which to write", offset)
        );

        log::trace!("writing page {:?} at offset {:?}", page.get_id(), offset);

        // TODO: Maybe re-use the page buffer so we don't allocate on every read/write to disk.
        let mut page_buffer = Vec::with_capacity(self.page_size as usize);
        page_buffer.resize(self.page_size as usize, 0);

        Page::serialize_into(page, &mut page_buffer[..])?;
        self.page_file.seek(SeekFrom::Start(offset))?;
        let num_bytes_written = self.page_file.write(&page_buffer[..])?;

        if num_bytes_written != page_buffer.len() {
            return Err(IOError::new(
                ErrorKind::BrokenPipe,
                format!(
                    "Expected to write {} bytes, wrote {} bytes instead",
                    page_buffer.len(),
                    num_bytes_written
                ),
            ));
        }

        self.page_file.flush()?;

        Ok(())
    }

    pub fn read_page(&mut self, offset: PageOffset) -> Result<Page, IOError> {
        assert_eq!(
            offset % self.page_size as u64,
            0,
            "{}",
            format!("invalid page offset {} from which to read", offset)
        );

        log::trace!("reading page at offset {:?}", offset);

        let mut page_buffer = Vec::with_capacity(self.page_size as usize);
        page_buffer.resize(self.page_size as usize, 0);
        let mut page = Page::default();

        // Seek to the page's offset
        let _offset_from_start = self.page_file.seek(SeekFrom::Start(offset))?;

        // Read the next page.
        let bytes_read = self.page_file.read(&mut page_buffer[..])?;
        if bytes_read != self.page_size as usize {
            return Err(IOError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "read {}bytes, expected to read {}bytes",
                    bytes_read, self.page_size
                ),
            ));
        }

        // Deserialize page.
        Page::deserialize_into(&mut page, &page_buffer)?;
        Ok(page)
    }

    /// Calls the reducer function for all pages in storage.
    pub fn visit_pages<F, U>(&mut self, mut reduce: F, mut acc: U) -> Result<U, Box<dyn Error>>
    where
        F: FnMut(&Page, u64, U) -> Result<U, Box<dyn Error>>,
    {
        let page_file_size = self.page_file.metadata()?.len();

        // Reuse the same buffer and page instance to iterate the file.
        let mut page_buffer = Vec::with_capacity(self.page_size as usize);
        page_buffer.resize(self.page_size as usize, 0);
        let mut page = Page::default();

        let mut curr_offset = 0;
        while curr_offset < page_file_size {
            self.page_file.seek(SeekFrom::Start(curr_offset))?;

            // Read the next page.
            let bytes_read = self.page_file.read(&mut page_buffer[..])?;
            if bytes_read != self.page_size as usize {
                return Err(Box::new(IOError::new(
                    ErrorKind::UnexpectedEof,
                    format!(
                        "read {}bytes, expected to read {}bytes",
                        bytes_read, self.page_size
                    ),
                )));
            }

            // Deserialize page.
            Page::deserialize_into(&mut page, &page_buffer)?;
            acc = match reduce(&page, curr_offset, acc) {
                Ok(acc) => acc,
                err @ Err(_) => return err,
            };

            curr_offset += self.page_size as u64;
        }

        Ok(acc)
    }
}
