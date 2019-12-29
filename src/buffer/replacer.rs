// TODO: Add a page replacement policy to the buffer pool

/// Represents whether or not a page has been modified.
/// i.e whether or not tuples have been added or removed from it.
#[derive(Eq, PartialEq)]
pub enum PageState {
    /// The page has not been updated.
    CLEAN,
    /// The page has been updated.
    DIRTY,
}
