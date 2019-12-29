use std::convert::AsRef;
use std::error::Error;
use std::fs::{create_dir, remove_file};
use std::io::{Error as IOError, ErrorKind};
use std::path::Path;

pub const TEST_DIR: &'static str = "/tmp/rustub_test";

fn ensure_test_dir() -> Result<(), IOError> {
    match create_dir(TEST_DIR) {
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
        response => response,
    }
}

pub fn ensure_removal<P: AsRef<Path>>(path: P) {
    match remove_file(&path) {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => panic!(
            "Failed to remove file {:?}: {:?}",
            path.as_ref().to_str().unwrap(),
            e
        ),
    };
}

pub fn with_page_file<P, F, U>(file_name: P, test: F) -> Result<U, Box<dyn Error>>
where
    F: FnOnce(&dyn AsRef<Path>) -> Result<U, Box<dyn Error>>,
    P: AsRef<Path>,
{
    ensure_test_dir()?;

    let file_path = Path::new(TEST_DIR).join(file_name.as_ref());

    // If the file exists from a previous execution remove it.
    ensure_removal(&file_path);
    let res = test(&file_path);
    ensure_removal(file_path);

    Ok(res?)
}
