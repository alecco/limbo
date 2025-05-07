use crate::fast_lock::SpinLock;
use crate::result::LimboResult;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseStorage;
use crate::storage::sqlite3_ondisk::{self, DatabaseHeader, PageContent, PageType};
use crate::storage::wal::{CheckpointResult, Wal};
use crate::{Buffer, LimboError, Result};
use parking_lot::RwLock;
use std::cell::{RefCell, UnsafeCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::trace;

use super::page_cache::{CacheError, DumbLruPageCache, PageCacheKey};
use super::wal::{CheckpointMode, CheckpointStatus};

pub struct PageInner {
    pub flags: AtomicUsize,
    pub contents: Option<PageContent>,
    pub id: usize,
}

#[derive(Debug)]
pub struct Page {
    pub inner: UnsafeCell<PageInner>,
}

// Concurrency control of pages will be handled by the pager, we won't wrap Page with RwLock
// because that is bad bad.
pub type PageRef = Arc<Page>;

/// Page is up-to-date.
const PAGE_UPTODATE: usize = 0b001;
/// Page is locked for I/O to prevent concurrent access.
const PAGE_LOCKED: usize = 0b010;
/// Page had an I/O error.
const PAGE_ERROR: usize = 0b100;
/// Page is dirty. Flush needed.
const PAGE_DIRTY: usize = 0b1000;
/// Page's contents are loaded in memory.
const PAGE_LOADED: usize = 0b10000;

impl Page {
    pub fn new(id: usize) -> Self {
        Self {
            inner: UnsafeCell::new(PageInner {
                flags: AtomicUsize::new(0),
                contents: None,
                id,
            }),
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get(&self) -> &mut PageInner {
        unsafe { &mut *self.inner.get() }
    }

    pub fn get_contents(&self) -> &mut PageContent {
        self.get().contents.as_mut().unwrap()
    }

    pub fn is_uptodate(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_UPTODATE != 0
    }

    pub fn set_uptodate(&self) {
        self.get().flags.fetch_or(PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn clear_uptodate(&self) {
        self.get().flags.fetch_and(!PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn is_locked(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_LOCKED != 0
    }

    pub fn set_locked(&self) {
        self.get().flags.fetch_or(PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn clear_locked(&self) {
        self.get().flags.fetch_and(!PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn is_error(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_ERROR != 0
    }

    pub fn set_error(&self) {
        self.get().flags.fetch_or(PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn clear_error(&self) {
        self.get().flags.fetch_and(!PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn is_dirty(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_DIRTY != 0
    }

    pub fn set_dirty(&self) {
        tracing::debug!("set_dirty(page={})", self.get().id);
        self.get().flags.fetch_or(PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn clear_dirty(&self) {
        tracing::debug!("clear_dirty(page={})", self.get().id);
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn is_loaded(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_LOADED != 0
    }

    pub fn set_loaded(&self) {
        self.get().flags.fetch_or(PAGE_LOADED, Ordering::SeqCst);
    }

    pub fn clear_loaded(&self) {
        tracing::debug!("clear loaded {}", self.get().id);
        self.get().flags.fetch_and(!PAGE_LOADED, Ordering::SeqCst);
    }

    pub fn is_index(&self) -> bool {
        match self.get_contents().page_type() {
            PageType::IndexLeaf | PageType::IndexInterior => true,
            PageType::TableLeaf | PageType::TableInterior => false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum FlushState {
    Start,
    WaitAppendFrames,
    SyncWal,
    Checkpoint,
    SyncDbFile,
    WaitSyncDbFile,
}

#[derive(Clone, Debug, Copy)]
enum CheckpointState {
    Checkpoint,
    SyncDbFile,
    WaitSyncDbFile,
    CheckpointDone,
}

/// This will keep track of the state of current cache flush in order to not repeat work
struct FlushInfo {
    state: FlushState,
    /// Number of writes taking place. When in_flight gets to 0 we can schedule a fsync.
    in_flight_writes: Rc<RefCell<usize>>,
}

/// The pager interface implements the persistence layer by providing access
/// to pages of the database file, including caching, concurrency control, and
/// transaction management.
pub struct Pager {
    /// Source of the database pages.
    pub db_file: Arc<dyn DatabaseStorage>,
    /// The write-ahead log (WAL) for the database.
    wal: Option<Rc<RefCell<dyn Wal>>>,
    /// A page cache for the database.
    page_cache: Arc<RwLock<DumbLruPageCache>>,
    /// Buffer pool for temporary data storage.
    buffer_pool: Rc<BufferPool>,
    /// I/O interface for input/output operations.
    pub io: Arc<dyn crate::io::IO>,
    dirty_pages: Rc<RefCell<HashSet<usize>>>,
    pub db_header: Arc<SpinLock<DatabaseHeader>>,

    flush_info: RefCell<FlushInfo>,
    checkpoint_state: RefCell<CheckpointState>,
    checkpoint_inflight: Rc<RefCell<usize>>,
    syncing: Rc<RefCell<bool>>,
}

impl From<CacheError> for LimboError {
    fn from(error: CacheError) -> Self {
        match error {
            CacheError::Full => LimboError::CacheFull,
            CacheError::Locked => LimboError::InternalError(
                format!("Cache operation failed: Page was locked")
            ),
            CacheError::Dirty => LimboError::InternalError(
                format!("Cache operation failed: Page was dirty")
            ),
            CacheError::ActiveRefs => LimboError::InternalError(
                format!("Cache operation failed: Page had active references")
            ),
            CacheError::KeyExists => LimboError::InternalError(
                format!("There is already a different page with this key in cache")
            ),
            CacheError::InternalError(s) => LimboError::InternalError(
                format!("Cache internal error: {}", s)
            ),
        }
    }
}

impl Pager {
    /// Begins opening a database by reading the database header.
    pub fn begin_open(db_file: Arc<dyn DatabaseStorage>) -> Result<Arc<SpinLock<DatabaseHeader>>> {
        sqlite3_ondisk::begin_read_database_header(db_file)
    }

    /// Completes opening a database by initializing the Pager with the database header.
    pub fn finish_open(
        db_header_ref: Arc<SpinLock<DatabaseHeader>>,
        db_file: Arc<dyn DatabaseStorage>,
        wal: Option<Rc<RefCell<dyn Wal>>>,
        io: Arc<dyn crate::io::IO>,
        page_cache: Arc<RwLock<DumbLruPageCache>>,
        buffer_pool: Rc<BufferPool>,
    ) -> Result<Self> {
        Ok(Self {
            db_file,
            wal,
            page_cache,
            io,
            dirty_pages: Rc::new(RefCell::new(HashSet::new())),
            db_header: db_header_ref.clone(),
            flush_info: RefCell::new(FlushInfo {
                state: FlushState::Start,
                in_flight_writes: Rc::new(RefCell::new(0)),
            }),
            syncing: Rc::new(RefCell::new(false)),
            checkpoint_state: RefCell::new(CheckpointState::Checkpoint),
            checkpoint_inflight: Rc::new(RefCell::new(0)),
            buffer_pool,
        })
    }

    pub fn btree_create(&self, flags: &CreateBTreeFlags) -> Result<u32> {
        let page_type = match flags {
            _ if flags.is_table() => PageType::TableLeaf,
            _ if flags.is_index() => PageType::IndexLeaf,
            _ => unreachable!("Invalid flags state"),
        };
        let page = self.do_allocate_page(page_type, 0)?;
        let id = page.get().id;
        Ok(id as u32)
    }

    /// Allocate a new overflow page.
    /// This is done when a cell overflows and new space is needed.
    pub fn allocate_overflow_page(&self) -> Result<PageRef> {
        let page = self.allocate_page()?;
        tracing::debug!("Pager::allocate_overflow_page(id={})", page.get().id);

        // setup overflow page
        let contents = page.get().contents.as_mut().unwrap();
        let buf = contents.as_ptr();
        buf.fill(0);

        Ok(page)
    }

    /// Allocate a new page to the btree via the pager.
    /// This marks the page as dirty and writes the page header.
    pub fn do_allocate_page(&self, page_type: PageType, offset: usize) -> Result<PageRef> {
        let page = self.allocate_page()?;
        crate::btree_init_page(&page, page_type, offset, self.usable_space() as u16);
        tracing::debug!(
            "do_allocate_page(id={}, page_type={:?})",
            page.get().id,
            page.get_contents().page_type()
        );
        Ok(page)
    }

    /// The "usable size" of a database page is the page size specified by the 2-byte integer at offset 16
    /// in the header, minus the "reserved" space size recorded in the 1-byte integer at offset 20 in the header.
    /// The usable size of a page might be an odd number. However, the usable size is not allowed to be less than 480.
    /// In other words, if the page size is 512, then the reserved space size cannot exceed 32.
    pub fn usable_space(&self) -> usize {
        let db_header = self.db_header.lock();
        (db_header.page_size - db_header.reserved_space as u16) as usize
    }

    #[inline(always)]
    pub fn begin_read_tx(&self) -> Result<LimboResult> {
        if let Some(wal) = &self.wal {
            return wal.borrow_mut().begin_read_tx();
        }

        Ok(LimboResult::Ok)
    }

    #[inline(always)]
    pub fn begin_write_tx(&self) -> Result<LimboResult> {
        if let Some(wal) = &self.wal {
            return wal.borrow_mut().begin_write_tx();
        }

        Ok(LimboResult::Ok)
    }

    pub fn end_tx(&self) -> Result<CheckpointStatus> {
        if let Some(wal) = &self.wal {
            let checkpoint_status = self.cacheflush()?;
            return match checkpoint_status {
                CheckpointStatus::IO => Ok(checkpoint_status),
                CheckpointStatus::Done(_) => {
                    wal.borrow().end_write_tx()?;
                    wal.borrow().end_read_tx()?;
                    Ok(checkpoint_status)
                }
            };
        }

        Ok(CheckpointStatus::Done(CheckpointResult::default()))
    }

    pub fn end_read_tx(&self) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.borrow().end_read_tx()?;
        }
        Ok(())
    }

    /// Reads a page from the database.
    pub fn read_page(&self, page_idx: usize) -> Result<PageRef, LimboError> {
        tracing::trace!("read_page(page_idx = {})", page_idx);
        let mut page_cache = self.page_cache.write();
        let max_frame = match &self.wal {
            Some(wal) => wal.borrow().get_max_frame(),
            None => 0,
        };
        let page_key = PageCacheKey::new(page_idx, Some(max_frame));
        if let Some(page) = page_cache.get(&page_key) {
            tracing::trace!("read_page(page_idx = {}) = cached", page_idx);
            return Ok(page.clone());
        }

        // Not in cache
        let page = Arc::new(Page::new(page_idx));
        // Try to insert in cache before reading
        match page_cache.insert(page_key, page.clone())?;
        page.set_locked();

        if let Some(wal) = &self.wal {
            if let Some(frame_id) = wal.borrow().find_frame(page_idx as u64)? {
                wal.borrow()
                    .read_frame(frame_id, page.clone(), self.buffer_pool.clone())?;
                {
                    page.set_uptodate();
                }
                return Ok(page);
            }
        }
        sqlite3_ondisk::begin_read_page(
            self.db_file.clone(),
            self.buffer_pool.clone(),
            page.clone(),
            page_idx,
        )?;
        Ok(page)
    }

    /// Loads pages if not loaded
    pub fn load_page(&self, page: PageRef) -> Result<(), LimboError> {
        let id = page.get().id;
        trace!("load_page(page_idx = {})", id);
        let mut page_cache = self.page_cache.write();
        page.set_locked();
        let max_frame = match &self.wal {
            Some(wal) => wal.borrow().get_max_frame(),
            None => 0,
        };
        let page_key = PageCacheKey::new(id, Some(max_frame));
        // Insert checks for duplicates
        page_cache.insert(page_key, page.clone())?;
        if let Some(wal) = &self.wal {
            if let Some(frame_id) = wal.borrow().find_frame(id as u64)? {
                wal.borrow()
                    .read_frame(frame_id, page.clone(), self.buffer_pool.clone())?;
                {
                    page.set_uptodate();
                }
                return Ok(());
            }
        }
        sqlite3_ondisk::begin_read_page(
            self.db_file.clone(),
            self.buffer_pool.clone(),
            page.clone(),
            id,
        )?;

        Ok(())
    }

    /// Writes the database header.
    pub fn write_database_header(&self, header: &DatabaseHeader) {
        sqlite3_ondisk::begin_write_database_header(header, self).expect("failed to write header");
    }

    /// Changes the size of the page cache.
    // FIXME: handle no room in page cache
    pub fn change_page_cache_size(&self, capacity: usize) {
        let mut page_cache = self.page_cache.write();
        page_cache.resize(capacity)
    }

    pub fn add_dirty(&self, page_id: usize) {
        // TODO: check duplicates?
        let mut dirty_pages = RefCell::borrow_mut(&self.dirty_pages);
        dirty_pages.insert(page_id);
    }

    pub fn cacheflush(&self) -> Result<CheckpointStatus> {
        let mut checkpoint_result = CheckpointResult::default();
        loop {
            let state = self.flush_info.borrow().state;
            trace!("cacheflush {:?}", state);
            match state {
                FlushState::Start => {
                    let db_size = self.db_header.lock().database_size;
                    let max_frame = match &self.wal {
                        Some(wal) => wal.borrow().get_max_frame(),
                        None => 0,
                    };
                    for page_id in self.dirty_pages.borrow().iter() {
                        let mut cache = self.page_cache.write();
                        let page_key = PageCacheKey::new(*page_id, Some(max_frame));
                        if let Some(wal) = &self.wal {
                            let page = cache.get(&page_key).expect("we somehow added a page to dirty list but we didn't mark it as dirty, causing cache to drop it.");
                            let page_type = page.get().contents.as_ref().unwrap().maybe_page_type();
                            trace!("cacheflush(page={}, page_type={:?}", page_id, page_type);
                            wal.borrow_mut().append_frame(
                                page.clone(),
                                db_size,
                                self.flush_info.borrow().in_flight_writes.clone(),
                            )?;
                        }
                        // This page is no longer valid.
                        // For example:
                        // We took page with key (page_num, max_frame) -- this page is no longer valid for that max_frame
                        // so it must be invalidated. There shouldn't be any active refs.
                        cache.delete(page_key).map_err(|e| {LimboError::InternalError(format!("Failed to delete page {:?} from cache during flush: {:?}. Might be actively referenced.", page_id, e))})?;
                    }
                    self.dirty_pages.borrow_mut().clear();
                    self.flush_info.borrow_mut().state = FlushState::WaitAppendFrames;
                    return Ok(CheckpointStatus::IO);
                }
                FlushState::WaitAppendFrames => {
                    let in_flight = *self.flush_info.borrow().in_flight_writes.borrow();
                    if in_flight == 0 {
                        self.flush_info.borrow_mut().state = FlushState::SyncWal;
                    } else {
                        return Ok(CheckpointStatus::IO);
                    }
                }
                FlushState::SyncWal => {
                    let wal = self.wal.clone().ok_or(LimboError::InternalError(
                        "SyncWal was called without a existing wal".to_string(),
                    ))?;
                    match wal.borrow_mut().sync() {
                        Ok(CheckpointStatus::IO) => return Ok(CheckpointStatus::IO),
                        Ok(CheckpointStatus::Done(res)) => checkpoint_result = res,
                        Err(e) => return Err(e),
                    }

                    let should_checkpoint = wal.borrow().should_checkpoint();
                    if should_checkpoint {
                        self.flush_info.borrow_mut().state = FlushState::Checkpoint;
                    } else {
                        self.flush_info.borrow_mut().state = FlushState::Start;
                        break;
                    }
                }
                FlushState::Checkpoint => {
                    match self.checkpoint()? {
                        CheckpointStatus::Done(res) => {
                            checkpoint_result = res;
                            self.flush_info.borrow_mut().state = FlushState::SyncDbFile;
                        }
                        CheckpointStatus::IO => return Ok(CheckpointStatus::IO),
                    };
                }
                FlushState::SyncDbFile => {
                    sqlite3_ondisk::begin_sync(self.db_file.clone(), self.syncing.clone())?;
                    self.flush_info.borrow_mut().state = FlushState::WaitSyncDbFile;
                }
                FlushState::WaitSyncDbFile => {
                    if *self.syncing.borrow() {
                        return Ok(CheckpointStatus::IO);
                    } else {
                        self.flush_info.borrow_mut().state = FlushState::Start;
                        break;
                    }
                }
            }
        }
        Ok(CheckpointStatus::Done(checkpoint_result))
    }

    pub fn checkpoint(&self) -> Result<CheckpointStatus> {
        let mut checkpoint_result = CheckpointResult::default();
        loop {
            let state = *self.checkpoint_state.borrow();
            trace!("pager_checkpoint(state={:?})", state);
            match state {
                CheckpointState::Checkpoint => {
                    let in_flight = self.checkpoint_inflight.clone();
                    let wal = self.wal.clone().ok_or(LimboError::InternalError(
                        "Checkpoint was called without a existing wal".to_string(),
                    ))?;
                    match wal
                        .borrow_mut()
                        .checkpoint(self, in_flight, CheckpointMode::Passive)?
                    {
                        CheckpointStatus::IO => return Ok(CheckpointStatus::IO),
                        CheckpointStatus::Done(res) => {
                            checkpoint_result = res;
                            self.checkpoint_state.replace(CheckpointState::SyncDbFile);
                        }
                    };
                }
                CheckpointState::SyncDbFile => {
                    sqlite3_ondisk::begin_sync(self.db_file.clone(), self.syncing.clone())?;
                    self.checkpoint_state
                        .replace(CheckpointState::WaitSyncDbFile);
                }
                CheckpointState::WaitSyncDbFile => {
                    if *self.syncing.borrow() {
                        return Ok(CheckpointStatus::IO);
                    } else {
                        self.checkpoint_state
                            .replace(CheckpointState::CheckpointDone);
                    }
                }
                CheckpointState::CheckpointDone => {
                    return if *self.checkpoint_inflight.borrow() > 0 {
                        Ok(CheckpointStatus::IO)
                    } else {
                        self.checkpoint_state.replace(CheckpointState::Checkpoint);
                        Ok(CheckpointStatus::Done(checkpoint_result))
                    };
                }
            }
        }
    }

    // WARN: used for testing purposes
    pub fn clear_page_cache(&self) -> CheckpointResult {
        let checkpoint_result: CheckpointResult;
        loop {
            match self.wal.clone().unwrap().borrow_mut().checkpoint(
                self,
                Rc::new(RefCell::new(0)),
                CheckpointMode::Passive,
            ) {
                Ok(CheckpointStatus::IO) => {
                    let _ = self.io.run_once();
                }
                Ok(CheckpointStatus::Done(res)) => {
                    checkpoint_result = res;
                    break;
                }
                Err(err) => panic!("error while clearing cache {}", err),
            }
        }
        // TODO: only clear cache of things that are really invalidated
        self.page_cache
            .write()
            .clear()
            .expect("Failed to clear page cache");
        checkpoint_result
    }

    // Providing a page is optional, if provided it will be used to avoid reading the page from disk.
    // This is implemented in accordance with sqlite freepage2() function.
    pub fn free_page(&self, page: Option<PageRef>, page_id: usize) -> Result<()> {
        const TRUNK_PAGE_HEADER_SIZE: usize = 8;
        const LEAF_ENTRY_SIZE: usize = 4;
        const RESERVED_SLOTS: usize = 2;

        const TRUNK_PAGE_NEXT_PAGE_OFFSET: usize = 0; // Offset to next trunk page pointer
        const TRUNK_PAGE_LEAF_COUNT_OFFSET: usize = 4; // Offset to leaf count

        if page_id < 2 || page_id > self.db_header.lock().database_size as usize {
            return Err(LimboError::Corrupt(format!(
                "Invalid page number {} for free operation",
                page_id
            )));
        }

        let page = match page {
            Some(page) => {
                assert_eq!(page.get().id, page_id, "Page id mismatch");
                page
            }
            None => self.read_page(page_id)?,
        };

        self.db_header.lock().freelist_pages += 1;

        let trunk_page_id = self.db_header.lock().freelist_trunk_page;

        if trunk_page_id != 0 {
            // Add as leaf to current trunk
            let trunk_page = self.read_page(trunk_page_id as usize)?;
            let trunk_page_contents = trunk_page.get().contents.as_ref().unwrap();
            let number_of_leaf_pages = trunk_page_contents.read_u32(TRUNK_PAGE_LEAF_COUNT_OFFSET);

            // Reserve 2 slots for the trunk page header which is 8 bytes or 2*LEAF_ENTRY_SIZE
            let max_free_list_entries = (self.usable_size() / LEAF_ENTRY_SIZE) - RESERVED_SLOTS;

            if number_of_leaf_pages < max_free_list_entries as u32 {
                trunk_page.set_dirty();
                self.add_dirty(trunk_page_id as usize);

                trunk_page_contents
                    .write_u32(TRUNK_PAGE_LEAF_COUNT_OFFSET, number_of_leaf_pages + 1);
                trunk_page_contents.write_u32(
                    TRUNK_PAGE_HEADER_SIZE + (number_of_leaf_pages as usize * LEAF_ENTRY_SIZE),
                    page_id as u32,
                );
                page.clear_uptodate();
                page.clear_loaded();

                return Ok(());
            }
        }

        // If we get here, need to make this page a new trunk
        page.set_dirty();
        self.add_dirty(page_id);

        let contents = page.get().contents.as_mut().unwrap();
        // Point to previous trunk
        contents.write_u32(TRUNK_PAGE_NEXT_PAGE_OFFSET, trunk_page_id);
        // Zero leaf count
        contents.write_u32(TRUNK_PAGE_LEAF_COUNT_OFFSET, 0);
        // Update page 1 to point to new trunk
        self.db_header.lock().freelist_trunk_page = page_id as u32;
        // Clear flags
        page.clear_uptodate();
        page.clear_loaded();
        Ok(())
    }

    /*
        Gets a new page that increasing the size of the page or uses a free page.
        Currently free list pages are not yet supported.
    */
    // FIXME: handle no room in page cache
    #[allow(clippy::readonly_write_lock)]
    pub fn allocate_page(&self) -> Result<PageRef> {
        let header = &self.db_header;
        let mut header = header.lock();
        header.database_size += 1;
        {
            // update database size
            // read sync for now
            loop {
                let first_page_ref = self.read_page(1)?;
                if first_page_ref.is_locked() {
                    self.io.run_once()?;
                    continue;
                }
                first_page_ref.set_dirty();
                self.add_dirty(1);

                let contents = first_page_ref.get().contents.as_ref().unwrap();
                contents.write_database_header(&header);
                break;
            }
        }

        // FIXME: should reserve page cache entry before modifying the database
        let page = allocate_page(header.database_size as usize, &self.buffer_pool, 0);
        {
            // setup page and add to cache
            page.set_dirty();
            self.add_dirty(page.get().id);
            let max_frame = match &self.wal {
                Some(wal) => wal.borrow().get_max_frame(),
                None => 0,
            };

            let page_key = PageCacheKey::new(page.get().id, Some(max_frame));
            let mut cache = self.page_cache.write();
            match cache.insert(page_key, page.clone()) {
                Err(CacheError::Full) => return Err(LimboError::CacheFull),
                Err(_) => {
                    return Err(LimboError::InternalError(
                        "Unknown error inserting page to cache".into(),
                    ))
                }
                Ok(_) => return Ok(page),
            }
        }
    }

    pub fn put_loaded_page(&self, id: usize, page: PageRef) -> Result<(), LimboError> {
        let mut cache = self.page_cache.write();
        let max_frame = match &self.wal {
            Some(wal) => wal.borrow().get_max_frame(),
            None => 0,
        };
        let page_key = PageCacheKey::new(id, Some(max_frame));

        // FIXME: is this correct? Why would there be another version of the page?
        // Check if there's an existing page at this key and remove it
        // This can happen when a page is being updated during a transaction
        // or when WAL frames are being managed
        if let Some(_existing_page_ref) = cache.get(&page_key) {
            cache.delete(page_key.clone()).map_err(|e| {
                LimboError::InternalError(format!(
                    "put_loaded_page failed to remove old version of page {:?}: {:?}.",
                    page_key, e
                ))
            })?;
        }

        cache.insert(page_key, page.clone()).map_err(|e| {
            LimboError::InternalError(format!(
                "Failed to insert loaded page {} into cache: {:?}",
                id, e
            ))
        })?;

        page.set_loaded();
        Ok(())
    }

    pub fn usable_size(&self) -> usize {
        let db_header = self.db_header.lock();
        (db_header.page_size - db_header.reserved_space as u16) as usize
    }
}

pub fn allocate_page(page_id: usize, buffer_pool: &Rc<BufferPool>, offset: usize) -> PageRef {
    let page = Arc::new(Page::new(page_id));
    {
        let buffer = buffer_pool.get();
        let bp = buffer_pool.clone();
        let drop_fn = Rc::new(move |buf| {
            bp.put(buf);
        });
        let buffer = Arc::new(RefCell::new(Buffer::new(buffer, drop_fn)));
        page.set_loaded();
        page.get().contents = Some(PageContent::new(offset, buffer));
    }
    page
}

#[derive(Debug)]
pub struct CreateBTreeFlags(pub u8);
impl CreateBTreeFlags {
    pub const TABLE: u8 = 0b0001;
    pub const INDEX: u8 = 0b0010;

    pub fn new_table() -> Self {
        Self(CreateBTreeFlags::TABLE)
    }

    pub fn new_index() -> Self {
        Self(CreateBTreeFlags::INDEX)
    }

    pub fn is_table(&self) -> bool {
        (self.0 & CreateBTreeFlags::TABLE) != 0
    }

    pub fn is_index(&self) -> bool {
        (self.0 & CreateBTreeFlags::INDEX) != 0
    }

    pub fn get_flags(&self) -> u8 {
        self.0
    }
}

#[cfg(test)]
pub mod tests {
    use crate::io::{MemoryIO, OpenFlags, IO};
    use crate::storage::database::DatabaseFile;
    use crate::storage::page_cache::DumbLruPageCache;
    use crate::storage::page_cache::PageCacheKey;
    use crate::storage::sqlite3_ondisk::PageType;
    use crate::storage::wal::CheckpointStatus;
    use crate::storage::wal::{WalFile, WalFileShared};
    use crate::{btree_init_page, BufferPool, DatabaseHeader, Page, Pager, SpinLock};
    use parking_lot::RwLock;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    pub(crate) fn run_pager_io(pager: &Pager) {
        for _ in 0..9 {
            let _ = pager.io.run_once();
        }
    }


    pub(crate) const SQLITE_ROOT_PAGE_ID: usize = 1;

    pub(crate) fn create_test_pager(cache_size: usize) -> (Rc<Pager>, usize) {
        let db_header = DatabaseHeader::default();
        let page_size = db_header.page_size as usize;
        let db_header_arc = Arc::new(SpinLock::new(db_header.clone()));

        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let io_file = io.open_file("test.db", OpenFlags::Create, false).unwrap();
        let db_file = Arc::new(DatabaseFile::new(io_file));

        let buffer_pool = Rc::new(BufferPool::new(page_size));
        let wal_shared = WalFileShared::open_shared(&io, "test.wal", page_size as u16).unwrap();
        let wal_file = WalFile::new(io.clone(), page_size, wal_shared, buffer_pool.clone());
        let wal = Rc::new(RefCell::new(wal_file));

        // XXX let page_cache = Arc::new(RwLock::new(DumbLruPageCache::new(cache_size)));
        let page_cache = Arc::new(parking_lot::RwLock::new(DumbLruPageCache::new(cache_size)));
        let pager = Pager::finish_open(
            db_header_arc,
            db_file,
            Some(wal),
            io,
            page_cache,
            buffer_pool,
        )
        .unwrap();
        let pager = Rc::new(pager);

        let root_page_ref = pager.read_page(SQLITE_ROOT_PAGE_ID).unwrap();
        root_page_ref.get_contents()
            .write_database_header(&pager.db_header.lock());
        root_page_ref.set_dirty();
        pager.add_dirty(root_page_ref.get().id);
        drop(root_page_ref);

        let page_ref = pager.allocate_page().unwrap();
        let page_id = page_ref.get().id;
        btree_init_page(&page_ref, PageType::TableLeaf, 0, 4096);
        pager.add_dirty(page_id);
        drop(page_ref);

        cache_flush(&pager);
        run_pager_io(&pager);

        (pager, page_id)
    }

    pub fn cache_flush(pager: &Rc<Pager>) -> CheckpointStatus {
        const FLUSH_IO_LIMIT: usize = 10;

        let mut status = pager.cacheflush().unwrap();
        let mut io_count = 0;
        while matches!(status, CheckpointStatus::IO) {
            io_count += 1;
            assert!(
                io_count <= FLUSH_IO_LIMIT,
                "Too many IO iterations ({}), maximum allowed is {}",
                io_count,
                FLUSH_IO_LIMIT
            );
            run_pager_io(&pager);
            status = pager.cacheflush().unwrap();
        }
        status
    }

    // TODO: move to page_cache.rs
    // Ensure cache can be shared between threads
    #[test]
    fn test_shared_cache() {
        let cache = Arc::new(RwLock::new(DumbLruPageCache::new(10)));

        let thread = {
            let cache = cache.clone();
            std::thread::spawn(move || {
                let mut cache = cache.write();
                let key1 = PageCacheKey::new(1, None);
                assert!(cache.insert(key1, Arc::new(Page::new(1))).is_ok());
            })
        };

        let _ = thread.join();
        let mut cache = cache.write();
        let key1 = PageCacheKey::new(1, None);
        let key2 = PageCacheKey::new(2, None);
        assert!(cache.insert(key2.clone(), Arc::new(Page::new(2))).is_ok());

        let page_ref1 = cache.get(&key1).expect("Page 1 should be in cache");
        assert_eq!(page_ref1.get().id, 1);
        let page_ref2 = cache.get(&key2).expect("Page 2 should be in cache");
        assert_eq!(page_ref2.get().id, 2);
    }

    #[test]
    fn test_read_page_simple() {
        let (pager, data_page_id) = create_test_pager(10);
        let page_ref = pager.read_page(data_page_id).unwrap();
        run_pager_io(&pager);
        assert_eq!(page_ref.get().id, data_page_id);
        assert!(page_ref.is_loaded(), "Page should be loaded");
        assert!(
            !page_ref.is_locked(),
            "Page should not be locked after read"
        );
    }

    #[test]
    fn test_read_page_cached() {
        let (pager, data_page_id) = create_test_pager(10);
        let page_ref_first = pager.read_page(data_page_id).unwrap();
        run_pager_io(&pager);
        assert!(page_ref_first.is_loaded());
        let page_ref_second = pager.read_page(data_page_id).unwrap();
        run_pager_io(&pager);
        assert!(
            Arc::ptr_eq(&page_ref_first, &page_ref_second),
            "Should get the same PageRef from cache"
        );
        assert!(page_ref_second.is_loaded(), "Cached page should be loaded");
    }

    #[test]
    fn test_read_page_wal() {
        let (pager, data_page_id) = create_test_pager(10);
        let wal = pager.wal.as_ref().unwrap();

        let page_ref = pager.read_page(data_page_id).unwrap();
        run_pager_io(&pager);
        page_ref.set_dirty();
        pager.add_dirty(data_page_id);
        drop(page_ref);
        let first_frame = wal.borrow().get_max_frame_in_wal();

        cache_flush(&pager);
        let page_ref_wal = pager.read_page(data_page_id).unwrap();
        assert!(
            page_ref_wal.is_loaded(),
            "Page read from WAL should be loaded"
        );
        assert!(
            page_ref_wal.is_uptodate(),
            "Page read from WAL should be up-to-date"
        );
        assert_eq!(
            wal.borrow().get_max_frame_in_wal(),
            first_frame + 1,
            "WAL should have one frame"
        );
    }

    #[test]
    fn test_load_page_not_loaded() {
        let (pager, _) = create_test_pager(10);
        let page_ref = pager.allocate_page().unwrap();
        assert!(
            page_ref.is_loaded(),
            "Page allocated should initially be marked loaded"
        );
        assert!(pager.load_page(page_ref.clone()).is_ok());
        run_pager_io(&pager);
        assert!(
            page_ref.is_loaded(),
            "Page should remain loaded after load_page call"
        );
    }

    #[test]
    fn test_cacheflush_dirty_page() {
        const POS: usize = 1;
        const VAL: u32 = 1234;
        let (pager, data_page_id) = create_test_pager(10);

        let page_ref = pager.read_page(data_page_id).unwrap();
        run_pager_io(&pager);
        {
            let contents = page_ref.get_contents();
            contents.write_u32(POS, VAL);
        }
        page_ref.set_dirty();
        pager.add_dirty(data_page_id);
        assert_eq!(pager.dirty_pages.borrow().len(), 1);
        let first_frame = pager.wal.as_ref().unwrap().borrow().get_max_frame_in_wal();
        drop(page_ref);

        cache_flush(&pager);
        assert_eq!(
            pager.dirty_pages.borrow().len(),
            0,
            "Dirty pages should be cleared after flush"
        );
        assert_eq!(
            pager.wal.as_ref().unwrap().borrow().get_max_frame_in_wal(),
            first_frame + 1,
            "WAL frame count should increase"
        );
        let page_ref_after = pager.read_page(data_page_id).unwrap();
        run_pager_io(&pager);
        assert!(
            !page_ref_after.is_dirty(),
            "Page should not be dirty after flush"
        );
        assert!(page_ref_after.is_loaded());
        {
            let contents = page_ref_after.get_contents();
            assert_eq!(
                contents.read_u32(POS),
                VAL,
                "Read back page should have modified user_version"
            );
        }
    }

    #[test]
    fn test_cacheflush_no_dirty_pages() {
        let (pager, _) = create_test_pager(10);
        let initial_dirty_pages = pager.dirty_pages.borrow().len();
        let initial_max_frame = pager.wal.as_ref().unwrap().borrow().get_max_frame();
        let status = cache_flush(&pager);
        assert!(
            matches!(status, CheckpointStatus::Done(_)),
            "Flush should eventually complete with Done status"
        );
        assert_eq!(
            pager.dirty_pages.borrow().len(),
            initial_dirty_pages,
            "Dirty pages should remain empty"
        );
        assert_eq!(
            pager.wal.as_ref().unwrap().borrow().get_max_frame(),
            initial_max_frame,
            "WAL frame count should not change"
        );
    }

    #[test]
    fn test_allocate_page() {
        let (pager, data_page_id) = create_test_pager(10);

        let page_ref = pager.allocate_page().unwrap();
        run_pager_io(&pager);
        let page_id = data_page_id + 1;
        assert_eq!(page_ref.get().id, page_id);
        assert!(page_ref.is_loaded());
        assert!(page_ref.is_dirty());
        assert!(pager.dirty_pages.borrow().contains(&page_id));
        assert_eq!(
            pager.db_header.lock().database_size,
            3,
            "Database size should increase"
        );
    }
}
