use rustub::storage;
use rustub::storage::page::PAGE_DATA_SIZE;
use rustub::storage::page::SLOT_SIZE_BYTES;
use rustub::storage::page_store::DiskPageStore;
use simple_logger;
use std::error::Error;

mod common;

#[test]
fn insert_fetch_tuples() -> Result<(), Box<dyn Error>> {
    let _ = simple_logger::init();

    // A test that writes inserted tuples to disk and reads them back.
    common::with_page_file("store_tuples", |page_file_path| {
        let mut table =
            storage::table_heap::TableHeap::<DiskPageStore>::from_page_file(&page_file_path, 2)?;

        // Page 1.
        let t1 = [0xfa, 0xce];
        let t2 = [0xfa, 0xce, 0xef, 0xc0, 0xed];
        // Page 2.
        let t3 = [0xbe; (PAGE_DATA_SIZE - SLOT_SIZE_BYTES) as usize];

        let tid1 = table.insert_tuple(&t1)?;
        let tid2 = table.insert_tuple(&t2)?;
        let tid3 = table.insert_tuple(&t3)?;

        // Flush file
        table.close()?;

        // Restart so that the pages are no longer in memory
        let mut table =
            storage::table_heap::TableHeap::<DiskPageStore>::from_page_file(&page_file_path, 2)?;

        // Force page 1 to be fetched from memory into the buffer pool
        let read_t1 = table.get_tuple(tid1.clone())?;
        let read_t2 = table.get_tuple(tid2.clone())?;
        // Force page 2 to be fetched from memory into the buffer pool
        let read_t3 = table.get_tuple(tid3.clone())?;

        assert_eq!(&t1, read_t1.as_ref());
        assert_eq!(&t2, read_t2.as_ref());
        assert_eq!(&t3[..], read_t3.as_ref());

        Ok(())
    })?;

    Ok(())
}

#[test]
fn delete_tuples() -> Result<(), Box<dyn Error>> {
    let _ = simple_logger::init();

    // A test that writes inserted tuples to disk and deletes them.
    common::with_page_file("delete_tuples", |page_file_path| {
        let mut table =
            storage::table_heap::TableHeap::<DiskPageStore>::from_page_file(&page_file_path, 2)?;

        let t1 = [0xfa, 0xce];
        let t2 = [0xfa, 0xce, 0xef, 0xc0, 0xed];
        let t3 = [0xbe; (PAGE_DATA_SIZE - SLOT_SIZE_BYTES) as usize];

        // Page 1.
        let tid1 = table.insert_tuple(&t1)?;
        let tid2 = table.insert_tuple(&t2)?;

        // Page 2.
        let tid3 = table.insert_tuple(&t3)?;

        // Delete t2 - which so far has been in memory only.
        table.delete_tuple(tid2.clone())?;

        // Check that all tuples but t2 exist in memory.
        let _read_t1 = table.get_tuple(tid1.clone())?;
        let _read_t3 = table.get_tuple(tid3.clone())?;
        table
            .get_tuple(tid2.clone())
            .expect_err(format!("tuple {:?} was not deleted in memory", tid2.clone()).as_str());

        // Flush file
        table.close()?;

        // Restart.
        let mut table =
            storage::table_heap::TableHeap::<DiskPageStore>::from_page_file(&page_file_path, 2)?;

        // Verify delete of t2 (memory-bound).
        // Check that all tuples but t2 are found from storage.
        let read_t1 = table.get_tuple(tid1.clone())?;
        let read_t3 = table.get_tuple(tid3.clone())?;
        table.get_tuple(tid2.clone()).expect_err(
            format!(
                "deleted tuple {:?} was written out to storage",
                tid2.clone()
            )
            .as_str(),
        );
        assert_eq!(&t1, read_t1.as_ref());
        assert_eq!(&t3[..], read_t3.as_ref());

        // Insert a new tuple. Since we only have two pages in the pool
        // it probably should occupy the old t2 slot.
        let t4 = [0xdf, 0xfa, 0x11, 0x08];
        let tid4 = table.insert_tuple(&t4)?;

        // Delete t1 - which is in memory but also in storage.
        table.delete_tuple(tid1.clone())?;

        // Flush file
        table.close()?;

        // Restart.
        let mut table =
            storage::table_heap::TableHeap::<DiskPageStore>::from_page_file(&page_file_path, 2)?;

        // Verify delete of t1 (storage-bound).
        // Check that all tuples but t1 are found from storage.
        let read_t3 = table.get_tuple(tid3.clone())?;
        let read_t4 = table.get_tuple(tid4.clone())?;
        table.get_tuple(tid1.clone()).expect_err(
            format!(
                "deleted tuple {:?} was not removed from storage",
                tid1.clone()
            )
            .as_str(),
        );
        assert_eq!(&t3[..], read_t3.as_ref());
        assert_eq!(&t4, read_t4.as_ref());

        Ok(())
    })?;

    Ok(())
}
