use crate::storage::page::PageId;
use crate::storage::page::PAGE_SIZE;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

type PageOffset = u64;

/// Tracks pages location within the underlying storage.
pub struct PageDirectory {
    offsets: HashMap<PageId, PageOffset>,
    // The next unique available id for a new page.
    next_page_id: u32,
    // The next offset in the file to append a new page.
    next_append_offset: PageOffset,
}

impl PageDirectory {
    /// Creates a new instance based on the provided pages' data.
    pub fn new(initial_offsets: HashMap<PageId, PageOffset>) -> Self {
        let next_page_id = match initial_offsets.keys().max_by_key(|page_id| page_id.0) {
            Some(ref max_page_id) => max_page_id.0 + 1, // Just use increasing sequence numbers as page ids
            None => 0,
        };
        // TODO: This next offset doesn't consider deleted pages within
        //  the page file - those offsets can be reused.
        let next_append_offset = match initial_offsets.values().max() {
            Some(highest_offset) => highest_offset + PAGE_SIZE as u64,
            None => 0,
        };

        PageDirectory {
            offsets: initial_offsets,
            next_page_id,
            next_append_offset,
        }
    }

    /// Allocates a unique page id.
    pub fn new_page_id(&mut self) -> PageId {
        // Just use increasing sequence numbers as page ids
        let id = self.next_page_id;
        self.next_page_id += 1;
        PageId(id)
    }

    pub fn get_or_create_offset(&mut self, page_id: &PageId) -> PageOffset {
        match self.offsets.get(page_id) {
            Some(offset) => offset.clone(),
            None => {
                // If we don't yet have an offset then this is a new page.
                // Assign to it, the next page offset for the file.
                let offset = self.next_append_offset;
                self.next_append_offset += PAGE_SIZE as u64;
                offset
            }
        }
    }
}

type FreeSpace = u16;

/// Tracks unused space within pages.
pub struct FreeSpaceDirectory {
    free_space: HashMap<PageId, FreeSpace>,
}

impl FreeSpaceDirectory {
    /// Creates a new instance based on the provided pages' data.
    pub fn new(initial_unused_bytes: HashMap<PageId, FreeSpace>) -> Self {
        FreeSpaceDirectory {
            free_space: initial_unused_bytes,
        }
    }

    /// Finds a page large enough to accommodate the requested bytes and
    /// reserves those bytes within that page.
    pub fn acquire_bytes(&mut self, bytes_to_reserve: u16) -> Option<PageId> {
        // TODO: Avoid scanning all pages to find space since the pages can be
        //  many and we're calling this for every tuple insert. We could keep a
        //  heap of (page_id, free_space), sorted by space DESC.
        for (page_id, unused_byte) in &mut self.free_space {
            if *unused_byte >= bytes_to_reserve {
                *unused_byte -= bytes_to_reserve;
                return Some(page_id.clone());
            }
        }
        None
    }

    /// Update the free space available within a page.
    pub fn update(&mut self, page_id: PageId, free_space: FreeSpace) {
        if free_space == 0 {
            // If the page has run out of space then no need to track it.
            self.free_space.remove(&page_id);
        } else {
            self.free_space.insert(page_id, free_space);
        }
    }

    /// Adds a newly created page to the directory.
    pub fn add_page(&mut self, page_id: PageId, initial_unused_bytes: FreeSpace) {
        match self.free_space.entry(page_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(initial_unused_bytes);
            }
            Entry::Occupied(_) => unreachable!("Page already exists {:?}", page_id),
        }
    }
}
