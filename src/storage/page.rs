use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::convert::AsRef;
use std::io;
use std::mem;

/**!
 *! Slotted page format (size in bytes):
 *!  ---------------------------------------------------------------------------------------------
 *!  | id (4) | slot_count (2) | free_space_offset (2) | ... SLOTS ... | ... INSERTED TUPLES ... |
 *!  ---------------------------------------------------------------------------------------------
 *!                                                                    ^
 *!                                                                    free_space_offset
 *!  Slot format (size in bytes):
 *!  -------------------------------------------------------
 *!  | tuple_offset (2)| tuple_size (2)| occupied_flag (1) |
 *!  -------------------------------------------------------
 *!
 */

/// The total size of a page.
pub const PAGE_SIZE: u16 = 1 * 1024;

/// The size of a page's slot array + tuple data.
//  8 is the space taken up by the id, tuple_count and free_space_offset.
pub const PAGE_DATA_SIZE: u16 = PAGE_SIZE - 8;

#[derive(Debug, Eq, PartialEq)]
pub struct Tuple(Vec<u8>);

/// A tuple can be a slice of bytes.
impl AsRef<[u8]> for Tuple {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

mod slot {
    use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

    /// A slot in a page's slot array, pointing to a tuple in the page.
    /// A slot is a wrapper over a 5byte slice, consisting of the offset (2bytes)
    /// within the page, the size (2bytes) of the tuple being pointed to and whether
    /// or not the tuple is a tombstone (1byte).
    pub struct Slot<'a>(pub &'a mut [u8]);
    pub struct ImmutableSlot<'a>(pub &'a [u8]);

    /// Size of a slot in bytes.
    pub const SIZE_OF: u16 = 5;

    impl<'a> Slot<'a> {
        pub fn set_offset(&mut self, offset: u16) {
            (&mut self.0[..])
                .write_u16::<BigEndian>(offset)
                .ok()
                .unwrap();
        }

        pub fn set_size(&mut self, size: u16) {
            (&mut self.0[2..])
                .write_u16::<BigEndian>(size)
                .ok()
                .unwrap();
        }

        pub fn mark_occupied(&mut self) {
            (&mut self.0[4..]).write_u8(0xff).ok().unwrap();
        }

        pub fn mark_free(&mut self) {
            (&mut self.0[4..]).write_u8(0x00).ok().unwrap();
        }

        pub fn get_offset(&self) -> u16 {
            ImmutableSlot(self.0).get_offset()
        }

        pub fn get_size(&self) -> u16 {
            ImmutableSlot(self.0).get_size()
        }

        pub fn is_occupied(&self) -> bool {
            ImmutableSlot(self.0).is_occupied()
        }
    }

    impl<'a> ImmutableSlot<'a> {
        pub fn get_offset(&self) -> u16 {
            (&self.0[..]).read_u16::<BigEndian>().unwrap()
        }

        pub fn get_size(&self) -> u16 {
            (&self.0[2..]).read_u16::<BigEndian>().unwrap()
        }

        pub fn is_occupied(&self) -> bool {
            (&self.0[4..]).read_u8().unwrap() == 0xff
        }
    }

    #[cfg(test)]
    mod tests {
        use super::Slot;

        #[test]
        fn slot() {
            let mut buf: Vec<u8> = vec![0; 6];
            buf[4] = 0xff;
            buf[5] = 0xef;

            let mut slot = Slot(&mut buf[..]);
            slot.set_offset(0xface);
            slot.set_size(0xcafe);

            assert_eq!(slot.get_offset(), 0xface);
            assert_eq!(slot.get_size(), 0xcafe);
            assert_eq!(buf[4..], [0xff, 0xef]);
        }

        #[test]
        fn slot_occupancy() {
            let mut buf: Vec<u8> = vec![0; 6];
            buf[4] = 0x00;
            buf[5] = 0xef;

            let mut slot = Slot(&mut buf[..]);
            slot.set_offset(0xface);
            slot.set_size(0xcafe);

            assert!(!slot.is_occupied());
            slot.mark_occupied();
            assert!(slot.is_occupied());
            slot.mark_free();
            assert!(!slot.is_occupied());

            assert_eq!(slot.get_offset(), 0xface);
            assert_eq!(slot.get_size(), 0xcafe);
            assert_eq!(buf[4..], [0x00, 0xef]);
        }
    }
}

pub use self::slot::SIZE_OF as SLOT_SIZE_BYTES;
use self::slot::{ImmutableSlot, Slot};

#[derive(PartialEq, Eq, Debug, Clone, Hash)]
pub struct PageId(pub u32);

pub struct Page {
    // Page id.
    id: PageId,
    /// The number of slots in the slot array.
    slot_count: u16,
    /// Offset, in the page data, to prepend a new tuple.
    free_space_offset: u16,
    /// Contains the slot array and tuple area.
    data: [u8; PAGE_DATA_SIZE as usize],
}

pub struct PageIterator<'a> {
    page: &'a Page,
    curr_slot: u16,
}

pub struct IterTuple<'a>(&'a [u8]);

impl<'a> Iterator for PageIterator<'a> {
    type Item = IterTuple<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.curr_slot < self.page.slot_count {
            let slot = self.curr_slot;
            self.curr_slot += 1;

            let slot = ImmutableSlot(&self.page.data[(slot * slot::SIZE_OF) as usize..]);

            if slot.is_occupied() {
                let tuple_offset = slot.get_offset() as usize;
                let tuple_size = slot.get_size() as usize;

                return Some(IterTuple(
                    &self.page.data[tuple_offset..tuple_offset + tuple_size],
                ));
            }
        }
        None
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TupleId {
    pub page_id: PageId,
    slot_index: u16,
}

impl Default for Page {
    fn default() -> Self {
        Page::new(PageId(std::u32::MAX))
    }
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Page {
            id,
            slot_count: 0,
            free_space_offset: PAGE_DATA_SIZE - 1,
            data: [0; PAGE_DATA_SIZE as usize],
        }
    }

    /// Returns the id of this page
    pub fn get_id(&self) -> PageId {
        self.id.clone()
    }

    /// Inserts a tuple into this page.
    /// Returns a read id to the insert tuple if successful (i.e the page had enough space).
    pub fn insert_tuple(&mut self, tuple: &[u8]) -> Option<TupleId> {
        let tuple_size = tuple.len() as u16;

        if tuple_size == 0 {
            panic!("Empty tuple insertion");
        }

        // Find a free (previously occupied) slot that we can reuse for this tuple.
        let mut free_slot_index = None;
        let mut curr_slot_index = 0;
        while curr_slot_index < self.slot_count {
            let slot = Slot(&mut self.data[(curr_slot_index * slot::SIZE_OF) as usize..]);

            if !slot.is_occupied() {
                free_slot_index = Some(curr_slot_index);
                break;
            }

            curr_slot_index += 1;
        }

        // Do we need to allocate a new slot for this tuple?
        let allocated_slot_size = match free_slot_index {
            Some(_) => 0,
            None => slot::SIZE_OF,
        };

        // Calculate the number of bytes we need to newly allocate to fit this tuple.
        // i.e the space for the tuple plus any newly allocated slot.
        let allocated_tuple_size = allocated_slot_size + tuple_size;

        let free_space_begin_offset = self.slot_count * slot::SIZE_OF;

        if self.free_space_offset < free_space_begin_offset {
            // Page is full.
            return None;
        }

        let free_space_size = self.free_space_offset - free_space_begin_offset + 1;
        if allocated_tuple_size > free_space_size {
            // Not enough space in the page to fit this tuple.
            return None;
        }

        let tuple_begin_offset = self.free_space_offset - tuple_size + 1;

        // Mark the assigned slot with the tuple's info.
        let slot_index = match free_slot_index {
            Some(index) => index,
            None => {
                self.slot_count += 1;
                self.slot_count - 1
            }
        };
        let mut slot = Slot(&mut self.data[(slot_index * slot::SIZE_OF) as usize..]);
        slot.mark_occupied();
        slot.set_offset(tuple_begin_offset);
        slot.set_size(tuple_size as u16);

        // Copy over the tuple into the assigned space.
        let copy_start = tuple_begin_offset as usize;
        let copy_end = copy_start + tuple_size as usize;
        self.data[copy_start..copy_end].copy_from_slice(tuple);

        self.free_space_offset = tuple_begin_offset - 1;

        Some(TupleId {
            page_id: self.id.clone(),
            slot_index,
        })
    }

    /// Fetches the tuple with the given read id.
    pub fn get_tuple(&self, tid: TupleId) -> Option<Tuple> {
        let TupleId {
            page_id,
            slot_index,
        } = tid;

        if page_id != self.id || slot_index >= self.slot_count {
            return None;
        }

        let slot = ImmutableSlot(&self.data[(slot_index * slot::SIZE_OF) as usize..]);
        if !slot.is_occupied() {
            return None;
        }

        let tuple_offset = slot.get_offset() as usize;
        let tuple_size = slot.get_size() as usize;

        let mut tuple = Vec::with_capacity(tuple_size);
        tuple.resize_with(tuple_size, || 0);
        tuple[0..tuple_size].copy_from_slice(&self.data[tuple_offset..tuple_offset + tuple_size]);

        Some(Tuple(tuple))
    }

    /// Marks the tuple with the given read id as deleted.
    /// Returns true if the tuple was marked as deleted (i.e if it existed).
    pub fn mark_delete(&mut self, tid: TupleId) -> bool {
        let TupleId {
            page_id,
            slot_index,
        } = tid;

        if page_id != self.id || slot_index >= self.slot_count {
            return false;
        }

        let mut slot = Slot(&mut self.data[(slot_index * slot::SIZE_OF) as usize..]);
        if !slot.is_occupied() {
            return false;
        }

        slot.mark_free();
        true
    }

    /// Removes the tuple with the given read id.
    /// Returns true if the tuple was removed (i.e if it existed).
    pub fn remove(&mut self, tid: TupleId) -> bool {
        let TupleId {
            page_id,
            slot_index,
        } = tid;

        if page_id != self.id || slot_index >= self.slot_count {
            return false;
        }

        let mut slot = Slot(&mut self.data[(slot_index * slot::SIZE_OF) as usize..]);
        if !slot.is_occupied() {
            return false;
        }

        slot.mark_free();

        // Compact the tuple area.
        let removed_tuple_offset = slot.get_offset();
        let removed_tuple_size = slot.get_size();

        struct MoveSlot {
            slot_index: u16,
            offset: u16,
            size: u16,
        }
        let mut slots_to_move = Vec::new();
        for slot_index in 0..self.slot_count {
            let slot = ImmutableSlot(&self.data[(slot_index * slot::SIZE_OF) as usize..]);
            if slot.get_offset() < removed_tuple_offset {
                slots_to_move.push(MoveSlot {
                    slot_index,
                    offset: slot.get_offset(),
                    size: slot.get_size(),
                })
            }
        }

        let total_size_to_relocate: u16 = slots_to_move.iter().map(|slot| slot.size).sum();

        let mut temp_space = Vec::with_capacity(total_size_to_relocate as usize);
        temp_space.resize_with(total_size_to_relocate as usize, || 0);

        // Index into the temp space.
        let mut temp_free_space_end = temp_space.len();
        // Index into the page's data space.
        let mut data_free_space_end = removed_tuple_offset + removed_tuple_size;

        for mv in slots_to_move {
            let old_offset = mv.offset as usize;
            let tuple_size = mv.size as usize;
            temp_space[temp_free_space_end - tuple_size..temp_free_space_end]
                .copy_from_slice(&self.data[old_offset..old_offset + tuple_size]);

            temp_free_space_end -= tuple_size;
            data_free_space_end -= tuple_size as u16;

            let mut slot = Slot(&mut self.data[(mv.slot_index * slot::SIZE_OF) as usize..]);
            slot.set_offset(data_free_space_end)
        }

        assert_eq!(
            temp_free_space_end, 0,
            "BUG: Exactly all tuples should be relocated into the new space"
        );

        // Index into the page, from where we will copy over new space.
        let copy_space_end = removed_tuple_offset + removed_tuple_size;
        let copy_space_begin = copy_space_end - total_size_to_relocate;
        (&mut self.data[copy_space_begin as usize..copy_space_end as usize])
            .copy_from_slice(&temp_space[..]);

        self.free_space_offset = copy_space_begin - 1;

        true
    }

    /// Iterate through all tuples in the page.
    pub fn iter(&self) -> PageIterator {
        PageIterator {
            page: self,
            curr_slot: 0,
        }
    }

    /// Returns the number of bytes that are free for tuples (including unoccupied
    /// slots in the slot array)
    pub fn free_space(&self) -> u16 {
        // TODO: Maybe track the free space in a variable and manually
        //  update it every time we insert/delete tuples? instead of recomputing
        //  e.g even when it hasn't changed - though the recompute is bounded by the
        //  number of slots which is in the hundreds in the worst cases.
        let occupied_slot_space: u16 = (0..self.slot_count)
            .filter_map(|slot_index| {
                let slot = ImmutableSlot(&self.data[(slot::SIZE_OF * slot_index) as usize..]);
                if slot.is_occupied() {
                    Some(slot::SIZE_OF + slot.get_size())
                } else {
                    None
                }
            })
            .sum();

        PAGE_DATA_SIZE - occupied_slot_space
    }
}

// Static helper methods
impl Page {
    /// Writes the page into they slice.
    pub fn serialize_into(page: &Page, bytes: &mut [u8]) -> Result<(), io::Error> {
        if bytes.len() != PAGE_SIZE as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid page in bytes: bytes size is {}bytes while page size is {}bytes",
                    bytes.len(),
                    PAGE_SIZE
                ),
            ));
        }

        let size_page_id = mem::size_of::<PageId>();
        (&mut bytes[..size_page_id]).write_u32::<BigEndian>(page.id.0)?;
        (&mut bytes[size_page_id..]).write_u16::<BigEndian>(page.slot_count)?;
        (&mut bytes[size_page_id + 2..]).write_u16::<BigEndian>(page.free_space_offset)?;
        bytes[size_page_id + 4..].copy_from_slice(&page.data);

        Ok(())
    }

    /// Translate the contents of the slice into the page.
    pub fn deserialize_into(page: &mut Page, bytes: &[u8]) -> Result<(), io::Error> {
        if bytes.len() != PAGE_SIZE as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid page in bytes: bytes size is {}bytes while page size is {}bytes",
                    bytes.len(),
                    PAGE_SIZE
                ),
            ));
        }

        let size_page_id = mem::size_of::<PageId>();
        page.id = PageId((&bytes[..size_page_id]).read_u32::<BigEndian>().unwrap());
        page.slot_count = (&bytes[size_page_id..]).read_u16::<BigEndian>().unwrap();
        page.free_space_offset = (&bytes[size_page_id + 2..])
            .read_u16::<BigEndian>()
            .unwrap();
        page.data.copy_from_slice(&bytes[size_page_id + 4..]);
        Ok(())
    }

    // Test only.
    fn assert_equal(p1: &Page, p2: &Page) {
        assert_eq!(p1.id, p2.id);
        assert_eq!(p1.slot_count, p2.slot_count);
        assert_eq!(p1.free_space_offset, p2.free_space_offset);
        assert_eq!(p1.data[..], p2.data[..]);
    }
}

#[cfg(test)]
mod tests {
    use super::{slot as SLOT, Page, PageId, Tuple, TupleId, PAGE_DATA_SIZE, PAGE_SIZE};

    fn sort_tuples(tuples: &mut Vec<&[u8]>) {
        use std::cmp::Ordering;
        tuples.sort_by(|t1, t2| {
            let mut i = 0;
            let mut j = 0;
            while i < t1.len() && j < t2.len() {
                match t1[i].cmp(&t2[j]) {
                    Ordering::Equal => {
                        i += 1;
                        j += 1;
                    }
                    order => return order,
                }
            }

            if i < t1.len() {
                Ordering::Less
            } else if j < t2.len() {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        })
    }

    #[test]
    fn new_page() {
        let p = Page::new(PageId(19));

        use std::mem;
        assert_eq!(1024, mem::size_of::<Page>());
        assert_eq!(PAGE_SIZE as usize, mem::size_of::<Page>());

        assert_eq!(p.slot_count, 0);
        assert_eq!(p.free_space_offset, PAGE_DATA_SIZE - 1);
        assert_eq!(&p.data[..], &[0; PAGE_DATA_SIZE as usize][..]);

        assert!(p.iter().next().is_none());
    }

    #[test]
    fn insert_single_tuple() {
        let mut p = Page::new(PageId(19));
        let t1: Vec<u8> = vec![0xca, 0xfe, 0xba, 0xbe];
        assert_eq!(
            p.insert_tuple(&t1),
            Some(TupleId {
                page_id: PageId(19),
                slot_index: 0
            })
        );

        let mut iter = p.iter();
        assert_eq!(iter.next().expect("Missing inserted tuple").0, &t1[..]);
        assert!(iter.next().is_none());
    }

    #[test]
    fn insert_multiple_tuples() {
        let mut p = Page::new(PageId(19));
        let t1 = [0xca, 0xfe, 0xba, 0xbe];
        assert_eq!(
            p.insert_tuple(&t1),
            Some(TupleId {
                page_id: PageId(19),
                slot_index: 0
            })
        );

        let mut iter = p.iter();
        assert_eq!(iter.next().expect("Missing inserted tuple").0, &t1[..]);
        assert!(iter.next().is_none());

        let t2 = [0xde, 0xad, 0xbe, 0xef, 0xfa];
        assert_eq!(
            p.insert_tuple(&t2),
            Some(TupleId {
                page_id: PageId(19),
                slot_index: 1
            })
        );

        let t3 = [0xce];
        assert_eq!(
            p.insert_tuple(&t3),
            Some(TupleId {
                page_id: PageId(19),
                slot_index: 2
            })
        );

        let mut tuples = p.iter().map(|t| t.0).collect::<Vec<&[u8]>>();

        sort_tuples(&mut tuples);

        assert_eq!(tuples, vec![&t1[..], &t3[..], &t2[..]]);
    }

    #[test]
    fn insert_no_space() {
        let mut p = Page::new(PageId(19));
        let t1 = [0xca, 0xfe, 0xba, 0xbe];
        assert_eq!(
            p.insert_tuple(&t1),
            Some(TupleId {
                page_id: PageId(19),
                slot_index: 0
            })
        );

        // Request just over the available space remaining,
        // t2 size = all space - (2 slots + t1.size (4B)) + 1
        let t2 = [0xba; PAGE_DATA_SIZE as usize - (2 * SLOT::SIZE_OF as usize) - 4 + 1];
        assert_eq!(p.insert_tuple(&t2), None);

        let mut tuples = p.iter().map(|t| t.0).collect::<Vec<&[u8]>>();
        sort_tuples(&mut tuples);
        assert_eq!(tuples, vec![&t1[..]]);

        let t2 = [0xba; PAGE_DATA_SIZE as usize - (2 * SLOT::SIZE_OF as usize) - 4];
        assert_eq!(
            p.insert_tuple(&t2),
            Some(TupleId {
                page_id: PageId(19),
                slot_index: 1
            })
        );

        // Page should now be full.
        let t3 = [0xba];
        assert_eq!(p.insert_tuple(&t3), None);

        let mut tuples = p.iter().map(|t| t.0).collect::<Vec<&[u8]>>();
        sort_tuples(&mut tuples);
        assert_eq!(tuples, vec![&t2[..], &t1[..]]);
    }

    #[test]
    fn get_tuples() {
        let mut p = Page::new(PageId(19));
        let t1 = [0xca, 0xfe, 0xba, 0xbe];
        let t2 = [0xbe, 0xef];
        let tid1 = p.insert_tuple(&t1).expect("Failed to insert tuple");
        let tid2 = p.insert_tuple(&t2).expect("Failed to insert tuple");

        assert_eq!(
            p.get_tuple(TupleId { ..tid1 }).expect("Cannot find tuple"),
            Tuple(t1.to_vec())
        );
        assert_eq!(
            p.get_tuple(TupleId { ..tid2.clone() })
                .expect("Cannot find tuple"),
            Tuple(t2.to_vec())
        );
        assert_eq!(
            p.get_tuple(TupleId {
                page_id: PageId(18),
                ..tid2.clone()
            }),
            None
        );
        assert_eq!(
            p.get_tuple(TupleId {
                slot_index: 2,
                ..tid2.clone()
            }),
            None
        );
    }

    #[test]
    fn mark_deleted() {
        let mut p = Page::new(PageId(19));
        let t1 = [0xca, 0xfe, 0xba, 0xbe];
        let t2 = [0xbe, 0xef];
        let t3 = [0xfa, 0xce];
        p.insert_tuple(&t1).expect("Failed to insert tuple");
        let tid2 = p.insert_tuple(&t2).expect("Failed to insert tuple");
        p.insert_tuple(&t3).expect("Failed to insert tuple");

        assert!(!p.mark_delete(TupleId {
            page_id: PageId(20),
            ..tid2.clone()
        }));
        assert!(!p.mark_delete(TupleId {
            slot_index: 8,
            ..tid2.clone()
        }));
        assert!(p.mark_delete(tid2.clone()));
        let t4 = [0xe9, 0xc4];
        p.insert_tuple(&t4).expect("Failed to insert tuple");

        let mut tuples = p.iter().map(|t| t.0).collect::<Vec<&[u8]>>();
        sort_tuples(&mut tuples);
        assert_eq!(tuples, vec![&t1[..], &t4[..], &t3[..]]);
    }

    #[test]
    fn remove() {
        let mut p = Page::new(PageId(19));
        let t1 = [0xca, 0xfe, 0xba, 0xbe];
        let t2 = [0xbe, 0xef];
        let t3 = [0xfa, 0xce];
        assert!(p.insert_tuple(&t1).is_some());
        let tid2 = p.insert_tuple(&t2).expect("Failed to insert tuple");
        assert!(p.insert_tuple(&t3).is_some());

        assert!(!p.remove(TupleId {
            page_id: PageId(20),
            ..tid2.clone()
        }));
        assert!(!p.remove(TupleId {
            slot_index: 8,
            ..tid2.clone()
        }));
        assert!(p.remove(tid2.clone()));
        let t4 = [0xe9, 0xc4];
        assert!(p.insert_tuple(&t4).is_some());

        // Verify that we did a compaction to free up the removed tuple's space.
        let t5 = [0xfb; PAGE_DATA_SIZE as usize - (4 * SLOT::SIZE_OF as usize) - 8 + 1];
        assert!(p.insert_tuple(&t5).is_none());

        let t5 = [0xfb; PAGE_DATA_SIZE as usize - (4 * SLOT::SIZE_OF as usize) - 8];
        assert!(p.insert_tuple(&t5).is_some());

        let mut tuples = p.iter().map(|t| t.0).collect::<Vec<&[u8]>>();
        sort_tuples(&mut tuples);
        assert_eq!(tuples, vec![&t1[..], &t4[..], &t3[..], &t5[..]]);
    }

    #[test]
    fn free_space() {
        let mut p = Page::new(PageId(19));
        assert_eq!(p.free_space(), PAGE_DATA_SIZE);

        let t1 = [0xca, 0xfe, 0xba, 0xbe];
        assert!(p.insert_tuple(&t1).is_some());
        assert_eq!(p.free_space(), PAGE_DATA_SIZE - (1 * SLOT::SIZE_OF + 4));

        let t2 = [0xbe, 0xef];
        let tid2 = p.insert_tuple(&t2).unwrap();
        assert_eq!(p.free_space(), PAGE_DATA_SIZE - (2 * SLOT::SIZE_OF + 6));

        let t3 = [0xfa];
        assert!(p.insert_tuple(&t3).is_some());
        assert_eq!(p.free_space(), PAGE_DATA_SIZE - (3 * SLOT::SIZE_OF + 7));

        // Does not count unused slots
        p.remove(tid2);
        assert_eq!(p.free_space(), PAGE_DATA_SIZE - (2 * SLOT::SIZE_OF + 5));

        let t4 = [0xde, 0xaf];
        assert!(p.insert_tuple(&t4).is_some());
        assert_eq!(p.free_space(), PAGE_DATA_SIZE - (3 * SLOT::SIZE_OF + 7));
    }

    #[test]
    fn serde() {
        let mut p = Page::new(PageId(19));
        let t1 = [0xca, 0xfe, 0xba, 0xbe];
        let t2 = [0xbe, 0xef];
        let t3 = [0xfa, 0xce];
        assert!(p.insert_tuple(&t1).is_some());
        assert!(p.insert_tuple(&t2).is_some());
        assert!(p.insert_tuple(&t3).is_some());

        let mut bytes = [0; PAGE_SIZE as usize];
        assert!(Page::serialize_into(&p, &mut bytes).is_ok());
        let mut result = Page::default();
        assert!(Page::deserialize_into(&mut result, &bytes).is_ok());

        assert_eq!(result.get_id(), p.get_id());
        Page::assert_equal(&p, &result);
    }
}
