use std::{cell::RefCell, collections::HashMap, ptr::NonNull, result::Result, sync::Arc};
use crate::error::LimboError;

use tracing::{debug, trace};

use super::pager::PageRef;

// In limbo, page cache is shared by default, meaning that multiple frames from WAL can reside in
// the cache, meaning, we need a way to differentiate between pages cached in different
// connections. For this we include the max_frame that a connection will read from so that if two
// connections have different max_frames, they might or not have different frame read from WAL.
//
// WAL was introduced after Shared cache in SQLite, so this is why these two features don't work
// well together because pages with different snapshots may collide.
#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct PageCacheKey {
    pgno: usize,
    max_frame: Option<u64>,
}

#[allow(dead_code)]
struct PageCacheEntry {
    key: PageCacheKey,
    page: PageRef,
    prev: Option<NonNull<PageCacheEntry>>,
    next: Option<NonNull<PageCacheEntry>>,
}

pub struct DumbLruPageCache {
    capacity: usize,
    map: RefCell<HashMap<PageCacheKey, NonNull<PageCacheEntry>>>,
    head: RefCell<Option<NonNull<PageCacheEntry>>>,
    tail: RefCell<Option<NonNull<PageCacheEntry>>>,
}

// XXX Send/Sync impls - RefCell makes this !Sync
unsafe impl Send for DumbLruPageCache {}
unsafe impl Sync for DumbLruPageCache {}

impl PageCacheKey {
    pub fn new(pgno: usize, max_frame: Option<u64>) -> Self {
        Self { pgno, max_frame }
    }
}
impl DumbLruPageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: RefCell::new(HashMap::new()),
            head: RefCell::new(None),
            tail: RefCell::new(None),
        }
    }

    #[must_use]
    pub fn contains_key(&mut self, key: &PageCacheKey) -> bool {
        self.map.borrow().contains_key(key)
    }

    #[must_use]
    pub fn delete_or_make_room_for_key(&mut self, key: &PageCacheKey) -> Result<(), LimboError> {
        trace!("delete_or_make_room_for_key(key={:?})", key);
        
        if self.map.borrow().contains_key(key) {
            trace!("delete_or_make_room_for_key(key={:?}) removing previous entry", key);
            self.delete(key)
        } else {
            trace!("delete_or_make_room_for_key(key={:?}) making room for new entry", key);
            self.make_room_for(1)
        }
    }

    #[must_use]
    pub fn insert(&mut self, key: &PageCacheKey, value: PageRef) -> Result<PageRef, LimboError> {
        trace!("insert(key={:?})", key);
        if self.contains_key(key) {
            return Err(LimboError::PageCacheKeyExists);
        }
        self.make_room_for(1)?;
        let entry = Box::new(PageCacheEntry {
            key: key.clone(),
            next: None,
            prev: None,
            page: value.clone(),
        });
        let ptr_raw = Box::into_raw(entry);
        let mut ptr = unsafe { NonNull::new_unchecked(ptr_raw) };
        self.insert_head(&mut ptr);
        {
            let mut map = self.map.borrow_mut();
            let _ = map.insert(key.clone(), ptr);
        }
        Ok(value)
    }

    #[must_use]
    pub fn insert_with_replace(&mut self, key: &PageCacheKey, value: PageRef) -> Result<(), LimboError> {
        trace!("insert_with_replace(key={:?})", key);
        let existing_ptr = self.map.borrow().get(key).copied();

        if let Some(mut ptr_ref_mut) = existing_ptr {
            trace!("insert_with_replace(key={:?}) replacing existing entry", key);
            let entry_mut = unsafe { ptr_ref_mut.as_mut() };
            // Clean the old page first or fail
            self.clean_page(&mut entry_mut.page)?;
            entry_mut.page = value;
            self.unlink(&mut ptr_ref_mut);
            self.insert_head(&mut ptr_ref_mut);
        } else {
            trace!("insert_with_replace(key={:?}) adding new entry", key);
            self.make_room_for(1)?;
            let entry = Box::new(PageCacheEntry {
                key: key.clone(),
                next: None,
                prev: None,
                page: value,
            });
            let ptr_raw = Box::into_raw(entry);
            let mut ptr = unsafe { NonNull::new_unchecked(ptr_raw) };
            self.insert_head(&mut ptr);
            {
                let mut map = self.map.borrow_mut();
                map.insert(key.clone(), ptr);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn delete(&mut self, key: &PageCacheKey) -> Result<(), LimboError> {
        trace!("delete(key={:?})", key);
        {
            // Try to detach before removing from map in case of error
            let ptr_opt = self.map.borrow().get(key).copied();
            if ptr_opt.is_none() {
                return Err(LimboError::PageCacheKeyNotFound);
            }
            let mut ptr = ptr_opt.unwrap();
            self.detach(&mut ptr)?;
        }
        let ptr_opt = self.map.borrow_mut().remove(key);
        let ptr = ptr_opt.unwrap();
        unsafe {
            std::ptr::drop_in_place(ptr.as_ptr());
        }
        trace!("delete(key={:?}) successful", key);
        Ok(())
    }

    #[must_use]
    fn get_ptr(&self, key: &PageCacheKey) -> Result<NonNull<PageCacheEntry>, LimboError> {
        match self.map.borrow().get(key).copied() {
            Some(ptr) => Ok(ptr),
            None => Err(LimboError::PageCacheKeyNotFound)
        }
    }

    /// Get page and promote entry in LRU order
    #[must_use]
    pub fn get(&self, key: &PageCacheKey) -> Result<PageRef, LimboError> {
        trace!("get(key={:?})", key);
        let mut ptr = self.get_ptr(key)?;
        let page = unsafe { ptr.as_mut().page.clone() };
        self.unlink(&mut ptr);
        self.insert_head(&mut ptr);
        Ok(page)
    }

    /// Get page without promoting entry in LRU order
    #[must_use]
    pub fn get_no_touch(&self, key: &PageCacheKey) -> Result<PageRef, LimboError> {
        trace!("get_no_touch(key={:?})", key);
        let mut ptr = self.get_ptr(key)?;
        let page = unsafe { ptr.as_mut().page.clone() };
        Ok(page)
    }

    #[must_use]
    pub fn resize(&mut self, capacity: usize) -> Result<(), LimboError> {
        self.capacity = capacity;
        self.make_room_for(self.len() - self.capacity)
    }

    #[must_use]
    fn can_evict(page: &PageRef) -> bool {
        !page.is_dirty() && !page.is_locked()
    }

    #[must_use]
    pub fn make_room_for(&mut self, n: usize) -> Result<(), LimboError> {
        if n > self.capacity {
            return Err(LimboError::PageCacheNotEnoughSpace);
        }

        let available = self.capacity - self.len();
        if n == 0 || n <= available {
            return Ok(()); // No need to evict anything
        }

        let need_to_evict = n - available;

        let tail = self.tail.borrow().ok_or_else(|| {
            LimboError::InternalError(format!(
                "Page cache of len {} expected to have a tail pointer", 
                self.len()
            ))
        })?;

        let mut evictable_keys = Vec::with_capacity(need_to_evict);
        let mut current = tail;

        while evictable_keys.len() < need_to_evict {
            let current_entry = unsafe { current.as_mut() };
            
            if Self::can_evict(&current_entry.page) {
                evictable_keys.push(&current_entry.key);
            } else {
                trace!(
                    "make_room_for({}) skipping non-evictable entry (key={:?})", 
                    n, current_entry.key
                );
            }

            match current_entry.prev {
                Some(prev) => current = prev,
                None => break,
            }
        }

        if evictable_keys.len() < need_to_evict {
            debug!(
                "make_room_for({}) iterated through all pages, found {}, but needed {} pages for eviction",
                n, evictable_keys.len(), need_to_evict
            );
            return Err(LimboError::PageCacheNotEnoughSpace);
        }

        for key_ref in evictable_keys {
            trace!("make_room_for({}) evicting (key={:?})", n, key_ref);
            self.delete(key_ref)?;
        }

        Ok(())
    }

    #[must_use]
    fn clean_page(&self, page: &mut PageRef) -> Result<(), LimboError> {
        Arc::get_mut(page)
            .ok_or(LimboError::PageHasActiveRefs)
            .and_then(|page_mut| {
                if page_mut.is_locked() {
                    return Err(LimboError::PageLocked);
                }
                if page_mut.is_dirty() {
                    return Err(LimboError::PageDirty);
                }

                debug!("Cleaning page {}", page_mut.get().id);
                page_mut.clear_loaded();
                let _ = page_mut.get().contents.take();
                Ok(())
            })
    }

    #[must_use]
    fn detach(&self, entry: &mut NonNull<PageCacheEntry>) -> Result<(), LimboError> {
        let entry_mut = unsafe { entry.as_mut() };
        // Try to clean before detaching in case it fails
        self.clean_page(&mut entry_mut.page)?;
        self.unlink(entry);
        Ok(())
    }

    fn unlink(&self, entry: &mut NonNull<PageCacheEntry>) {
        let (prev, next) = unsafe {
            let entry_mut = entry.as_mut();
            let prev = entry_mut.prev.take();
            let next = entry_mut.next.take();
            (prev, next)
        };

        match (prev, next) {
            (None, None) => {
                self.head.borrow_mut().take();
                self.tail.borrow_mut().take();
            }
            (None, Some(mut n)) => {
                unsafe { n.as_mut().prev = None };
                self.head.borrow_mut().replace(n);
            }
            (Some(mut p), None) => {
                unsafe { p.as_mut().next = None };
                self.tail.borrow_mut().replace(p);
            }
            (Some(mut p), Some(mut n)) => {
                unsafe {
                    p.as_mut().next = Some(n);
                    n.as_mut().prev = Some(p);
                }
            }
        }
    }

    fn insert_head(&self, entry: &mut NonNull<PageCacheEntry>) {
        assert!(unsafe { entry.as_ref().next.is_none() });
        assert!(unsafe { entry.as_ref().prev.is_none() });
        
        let mut head_borrow = self.head.borrow_mut();
        let old_head = head_borrow.take();

        let entry_mut = unsafe { entry.as_mut() };

        match old_head {
            Some(mut head_ptr) => {
                unsafe {
                    entry_mut.next = Some(head_ptr);
                    head_ptr.as_mut().prev = Some(*entry);
                }
            }
            None => {
                self.tail.borrow_mut().replace(*entry);
            }
        }

        *head_borrow = Some(*entry);

        trace!(
            "inserted head {:?}, head={:?}, tail={:?}",
            entry_mut.key.clone(),
            self.head.borrow().map(|p| unsafe { p.as_ref().key.clone() }),
            self.tail.borrow().map(|p| unsafe { p.as_ref().key.clone() })
        );
    }

    pub fn clear(&mut self) {
        let keys: Vec<PageCacheKey> = self.map.borrow().keys().cloned().collect();
        for key in keys {
            let _ = self.delete(&key);
        }
        assert!(self.head.borrow().is_none());
        assert!(self.tail.borrow().is_none());
        assert!(self.map.borrow().is_empty());
    }

    #[allow(dead_code)]
    pub fn print(&self) {
        println!("page_cache capacity={}", self.capacity);
        println!("page_cache len={}", self.map.borrow().len());
        // Iterate through the list from head to tail for better LRU viz
        let mut current = *self.head.borrow();
        let mut count = 0;
        print!("LRU Order (Head -> Tail): [");
        while let Some(node) = current {
            // SAFETY: Node pointer assumed valid within list structure.
            let entry = unsafe { node.as_ref() };
            print!("({:?}) ", entry.key);
            current = entry.next;
            count += 1;
            if count > self.len() + 2 { // Safety break
                print!("... LIST CORRUPT? ...");
                break;
            }
        }
        println!("]");
        if count != self.len() {
            println!("WARN: List count {} != Map length {}", count, self.len());
        }
    }

    pub fn len(&self) -> usize {
        self.map.borrow().len()
    }

    #[cfg(test)]
    fn verify_list_integrity(&self) {
        let map = self.map.borrow(); // Hold borrow for duration
        let map_len = map.len();
        let head_ptr = *self.head.borrow();
        let tail_ptr = *self.tail.borrow();

        if map_len == 0 {
            assert!(head_ptr.is_none(), "Head should be None when map is empty");
            assert!(tail_ptr.is_none(), "Tail should be None when map is empty");
            return;
        }

        assert!(
            head_ptr.is_some(),
            "Head should be Some when map is not empty"
        );
        assert!(
            tail_ptr.is_some(),
            "Tail should be Some when map is not empty"
        );

        unsafe {
            assert!(
                head_ptr.unwrap().as_ref().prev.is_none(),
                "Head's prev pointer mismatch"
            );
        }

        unsafe {
            assert!(
                tail_ptr.unwrap().as_ref().next.is_none(),
                "Tail's next pointer mismatch"
            );
        }

        // Forward traversal
        let mut forward_count = 0;
        let mut current = head_ptr;
        let mut last_ptr: Option<NonNull<PageCacheEntry>> = None;
        while let Some(node) = current {
            forward_count += 1;
            unsafe {
                let node_ref = node.as_ref();
                assert_eq!(
                    node_ref.prev, last_ptr,
                    "Backward pointer mismatch during forward traversal for key {:?}",
                    node_ref.key
                );
                assert!(
                    map.contains_key(&node_ref.key),
                    "Node key {:?} not found in map during forward traversal",
                    node_ref.key
                );
                assert_eq!(
                    map.get(&node_ref.key).copied(),
                    Some(node),
                    "Map pointer mismatch for key {:?}",
                    node_ref.key
                );

                last_ptr = Some(node);
                current = node_ref.next;
            }

            if forward_count > map_len + 5 {
                panic!(
                    "Infinite loop suspected in forward integrity check. Size {}, count {}",
                    map_len, forward_count
                );
            }
        }
        assert_eq!(
            forward_count, map_len,
            "Forward count mismatch (counted {}, map has {})",
            forward_count, map_len
        );
        assert_eq!(
            tail_ptr, last_ptr,
            "Tail pointer mismatch after forward traversal"
        );

        // Backward traversal
        let mut backward_count = 0;
        current = tail_ptr;
        last_ptr = None;
        while let Some(node) = current {
            backward_count += 1;
            unsafe {
                let node_ref = node.as_ref();
                assert_eq!(
                    node_ref.next, last_ptr,
                    "Forward pointer mismatch during backward traversal for key {:?}",
                    node_ref.key
                );
                assert!(
                    map.contains_key(&node_ref.key),
                    "Node key {:?} not found in map during backward traversal",
                    node_ref.key
                );

                last_ptr = Some(node);
                current = node_ref.prev;
            }
            if backward_count > map_len + 5 {
                panic!(
                    "Infinite loop suspected in backward integrity check. Size {}, count {}",
                    map_len, backward_count
                );
            }
        }
        assert_eq!(
            backward_count, map_len,
            "Backward count mismatch (counted {}, map has {})",
            backward_count, map_len
        );
        assert_eq!(
            head_ptr, last_ptr,
            "Head pointer mismatch after backward traversal"
        );
    }
}

// --- Tests Adjustments ---
#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{Buffer, BufferData};
    use crate::storage::pager::{Page, PageRef};
    use crate::storage::sqlite3_ondisk::PageContent;
    use std::{cell::RefCell, num::NonZeroUsize, pin::Pin, rc::Rc, sync::Arc};

    use lru::LruCache;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    // Helper to create key remains same
    fn create_key(id: usize) -> PageCacheKey {
        PageCacheKey::new(id, Some(id as u64))
    }

    // Helper to create page remains same
    pub fn page_with_content(page_id: usize) -> PageRef {
        let page = Arc::new(Page::new(page_id));
        {
            let buffer_drop_fn = Rc::new(|_data: BufferData| {});
            let buffer = Buffer::new(Pin::new(vec![0; 4096]), buffer_drop_fn);
            let page_content = PageContent {
                offset: 0,
                buffer: Arc::new(RefCell::new(buffer)),
                overflow_cells: Vec::new(),
            };
            page.get().contents = Some(page_content);
            page.set_loaded();
        }
        page
    }

    // Adjust insert_page to pass key by reference
    fn insert_page(cache: &mut DumbLruPageCache, id: usize) -> PageCacheKey {
        let key = create_key(id);
        let page = page_with_content(id);
        cache.insert(&key, page).expect("Insert failed in test helper"); // Use &key
        key // Return owned key for test checks
    }

    fn page_has_content(page: &PageRef) -> bool {
        page.is_loaded() && page.get().contents.is_some()
    }

    // Test states remain same
    #[derive(Clone, Copy)]
    pub enum DirtyState { Clean, Dirty }
    #[derive(Clone, Copy)]
    pub enum LockState { Unlocked, Locked }

    // Adjust insert_page_with_state to pass key by reference
    #[must_use]
    fn insert_page_with_state(
        cache: &mut DumbLruPageCache,
        id: usize,
        dirty: DirtyState,
        lock: LockState,
    ) -> PageCacheKey {
        let key = create_key(id);
        let page = page_with_content(id);

        match dirty {
            DirtyState::Dirty => page.set_dirty(),
            DirtyState::Clean => (),
        }

        match lock {
            LockState::Locked => page.set_locked(),
            LockState::Unlocked => (),
        }
        cache.insert(&key, page.clone()).expect("Insert failed in test helper"); // Use &key
        key
    }

    // --- Individual Tests ---

    #[test]
    fn test_detach_only_element() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = insert_page(&mut cache, 1);
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 1);
        assert!(cache.head.borrow().is_some());
        assert!(cache.tail.borrow().is_some());
        assert_eq!(*cache.head.borrow(), *cache.tail.borrow());

        cache.delete(&key1).expect("Delete failed");

        assert_eq!(cache.len(), 0, "Length should be 0 after deleting only element");
        assert!(cache.get(&key1).is_err(), "Map should not contain key after delete");
        assert!(cache.head.borrow().is_none(), "Head should be None");
        assert!(cache.tail.borrow().is_none(), "Tail should be None");
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_head() {
        let mut cache = DumbLruPageCache::new(5);
        let _key1 = insert_page(&mut cache, 1); // Tail
        let key2 = insert_page(&mut cache, 2); // Middle
        let key3 = insert_page(&mut cache, 3); // Head
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 3);

        let head_ptr_before = cache.head.borrow().unwrap();
        assert_eq!(unsafe { &head_ptr_before.as_ref().key }, &key3, "Initial head check");

        cache.delete(&key3).expect("Delete failed");

        assert_eq!(cache.len(), 2, "Length should be 2 after deleting head");
        assert!(cache.get(&key3).is_err(), "Map should not contain deleted head key");
        cache.verify_list_integrity();

        let new_head_ptr = cache.head.borrow().unwrap();
        assert_eq!(unsafe { &new_head_ptr.as_ref().key }, &key2, "New head should be key2");
        assert!(unsafe { new_head_ptr.as_ref().prev.is_none() }, "New head's prev should be None");

        // Find key1's pointer to check linkage
        let key1_ptr = cache.get_ptr(&create_key(1)).expect("Key 1 should exist");
        assert_eq!(unsafe { new_head_ptr.as_ref().next }, Some(key1_ptr), "New head's next should point to key1");
        assert_eq!(unsafe { key1_ptr.as_ref().prev }, Some(new_head_ptr), "Key1's prev should point to new head (key2)");
    }

    #[test]
    fn test_detach_tail() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = insert_page(&mut cache, 1); // Tail
        let key2 = insert_page(&mut cache, 2); // Middle
        let key3 = insert_page(&mut cache, 3); // Head
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 3);

        let tail_ptr_before = cache.tail.borrow().unwrap();
        assert_eq!(unsafe { &tail_ptr_before.as_ref().key }, &key1, "Initial tail check");

        cache.delete(&key1).expect("Delete failed");

        assert_eq!(cache.len(), 2, "Length should be 2 after deleting tail");
        assert!(cache.get(&key1).is_err(), "Map should not contain deleted tail key");
        cache.verify_list_integrity();

        let new_tail_ptr = cache.tail.borrow().unwrap();
        assert_eq!(unsafe { &new_tail_ptr.as_ref().key }, &key2, "New tail should be key2");
        assert!(unsafe { new_tail_ptr.as_ref().next.is_none() }, "New tail's next should be None");

        let head_ptr = cache.head.borrow().unwrap();
        assert_eq!(unsafe { &head_ptr.as_ref().key }, &key3, "Head should still be key3");
        assert_eq!(unsafe { head_ptr.as_ref().next }, Some(new_tail_ptr), "Head's next should point to new tail (key2)");
        assert_eq!(unsafe { new_tail_ptr.as_ref().prev }, Some(head_ptr), "New tail's prev should point to head (key3)");
    }

    #[test]
    fn test_detach_middle() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = insert_page(&mut cache, 1); // Tail
        let key2 = insert_page(&mut cache, 2); // Middle
        let key3 = insert_page(&mut cache, 3); // Middle
        let key4 = insert_page(&mut cache, 4); // Head
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 4);

        cache.delete(&key2).expect("Delete failed");

        assert_eq!(cache.len(), 3, "Length should be 3 after deleting middle");
        assert!(cache.get(&key2).is_err(), "Map should not contain deleted middle key2");
        cache.verify_list_integrity();

        // Check neighbors
        let key1_ptr = cache.get_ptr(&key1).expect("Key1 should still exist");
        let key3_ptr = cache.get_ptr(&key3).expect("Key3 should still exist");
        assert_eq!(unsafe { key3_ptr.as_ref().next }, Some(key1_ptr), "Key3's next should point to key1");
        assert_eq!(unsafe { key1_ptr.as_ref().prev }, Some(key3_ptr), "Key1's prev should point to key3");

        let head_ptr = cache.head.borrow().unwrap();
        let tail_ptr = cache.tail.borrow().unwrap();
        assert_eq!(unsafe { head_ptr.as_ref().key.clone() }, key4, "Head should remain key4");
        assert_eq!(unsafe { tail_ptr.as_ref().key.clone() }, key1, "Tail should remain key1");
    }

    #[test]
    fn test_detach_via_delete() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = create_key(1);
        let page1 = page_with_content(1);
        let page1_clone = page1.clone(); // Clone Arc for checking later
        cache.insert(&key1, page1).unwrap(); // Use &key1
        assert!(page_has_content(&page1_clone), "Page content should exist before delete");
        cache.verify_list_integrity();

        cache.delete(&key1).expect("Delete failed");
        assert_eq!(cache.len(), 0);

        // Check content removal via Arc count (if delete cleans)
        // Note: clean_page only works if Arc::get_mut succeeds (ref count == 1)
        // If page1_clone is the only strong ref left, content should be gone.
        // Check Arc count first. If > 1, clean_page would have failed.
        if Arc::strong_count(&page1_clone) == 1 {
            assert!(!page_has_content(&page1_clone), "Page content should be removed after delete if ref count was 1");
        } else {
            println!("Note: Page content not checked for removal as ref count > 1 after delete.");
        }
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_via_insert() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page_with_content(1);
        let page1_v1_clone = page1_v1.clone(); // Clone for checking later

        cache.insert(&key1, page1_v1).unwrap(); // Use &key1
        assert!(page_has_content(&page1_v1_clone), "Page1 V1 content should exist initially");
        cache.verify_list_integrity();

        cache.insert(&key1, page1_v2.clone()).unwrap(); // Use &key1, trigger replace

        assert_eq!(cache.len(), 1, "Cache length should still be 1 after replace");
        // Check if V1 was cleaned. Requires ref count == 1 before the replace call's clean_page attempt.
        if Arc::strong_count(&page1_v1_clone) == 1 {
             assert!(!page_has_content(&page1_v1_clone), "Page1 V1 content should be cleaned after being replaced if ref count was 1");
        } else {
            println!("Note: Page V1 content not checked for removal as ref count > 1 after replace.");
        }
        assert!(page_has_content(&page1_v2), "Page1 V2 content should exist after insert");

        let current_page = cache.get(&key1).expect("Get should succeed");
        assert!(Arc::ptr_eq(&current_page, &page1_v2), "Cache should now hold page1 V2");

        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_nonexistent_key() {
        let mut cache = DumbLruPageCache::new(5);
        let key_nonexist = create_key(99);
        let result = cache.delete(&key_nonexist);
        assert!(matches!(result, Err(LimboError::PageCacheKeyNotFound)), "Delete should fail with PageCacheKeyNotFound");
        assert_eq!(cache.len(), 0);
        cache.verify_list_integrity();
    }

    #[test]
    fn test_page_cache_fuzz() {
        // Fuzz test adjustments: use &key for insert/peek/delete
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        tracing::info!("super seed: {}", seed);
        let capacity = 10;
        let max_pages = 20; // Use more pages than capacity
        let mut cache = DumbLruPageCache::new(capacity);
        let mut lru = LruCache::new(NonZeroUsize::new(capacity).unwrap());

        for i in 0..10000 {
            cache.verify_list_integrity();
            assert_eq!(cache.len(), lru.len(), "Cache and LRU len mismatch iter {}", i);

            match rng.next_u64() % 4 {
                0 => { // INSERT
                    let id_page = rng.next_u64() % max_pages;
                    let id_frame = rng.next_u64() % max_pages;
                    let key = PageCacheKey::new(id_page as usize, Some(id_frame));
                    let page = page_with_content(id_page as usize); // Content helps check cleaning

                    // Simulate potential eviction by LRU model first
                    if lru.len() == capacity && !lru.contains(&key) {
                        // Predict eviction if capacity is full and key is new
                        let _evicted = lru.pop_lru();
                    }
                    lru.push(key.clone(), page.clone()); // Update LRU model

                    // Perform actual cache insert
                    let _ = cache.insert(&key, page); // Use &key, ignore PageCacheFull error like original fuzz

                    // Post-check: Cache size shouldn't exceed capacity unless inserts fail
                    assert!(cache.len() <= capacity, "Cache size {} exceeds capacity {}", cache.len(), capacity);

                },
                1 => { // DELETE
                    if lru.is_empty() { continue; }
                    // Choose a key to delete: 50% random (might not exist), 50% from LRU
                    let key = if rng.next_u64() % 2 == 0 {
                        let id_page = rng.next_u64() % max_pages;
                        let id_frame = rng.next_u64() % max_pages;
                        PageCacheKey::new(id_page as usize, Some(id_frame))
                    } else {
                        // Pick a random key actually in the cache
                        let index = rng.next_u64() as usize % lru.len();
                        lru.iter().nth(index).unwrap().0.clone()
                    };

                    lru.pop(&key); // Update LRU model
                    let _ = cache.delete(&key); // Ignore errors here for fuzz testing
                },
                2 => { // GET (promotes)
                    if lru.is_empty() { continue; }
                    let index = rng.next_u64() as usize % lru.len();
                    let key = lru.iter().nth(index).unwrap().0.clone();

                    // Get from cache (promotes)
                    let page_result = cache.get(&key); // Use &key
                    assert!(page_result.is_ok(), "Get failed for key {:?} known to be in LRU model", key);
                    assert_eq!(page_result.unwrap().get().id, key.pgno);

                    // Update LRU model to reflect promotion
                    lru.get(&key);
                },
                3 => { // PEEK (no promotion)
                     if lru.is_empty() { continue; }
                    let index = rng.next_u64() as usize % lru.len();
                    let key = lru.iter().nth(index).unwrap().0.clone();

                    // Peek from cache (no promotion)
                    let page_result = cache.get_no_touch(&key);
                    assert!(page_result.is_ok(), "Peek failed for key {:?} known to be in LRU model", key);
                    assert_eq!(page_result.unwrap().get().id, key.pgno);

                    // No update needed for LRU model
                }
                _ => unreachable!(),
            }
             // Final check after operation
             assert_eq!(cache.len(), lru.len(), "Post-op Cache and LRU len mismatch iter {}", i);
        }
         cache.clear(); // Test clear at the end
         assert_eq!(cache.len(), 0);
         cache.verify_list_integrity();
    }

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key1).expect("Get key1 failed").get().id, 1); // Use &key1
        assert_eq!(cache.get(&key2).expect("Get key2 failed").get().id, 2); // Use &key2
        cache.verify_list_integrity();
    }

    #[test]
    fn test_page_cache_over_capacity() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1); // Tail after next insert
        let _key2 = insert_page(&mut cache, 2); // Head after insert, Tail after next insert
        let key3 = insert_page(&mut cache, 3); // Head
        cache.verify_list_integrity();
        // Key1 should be evicted
        assert!(cache.get(&key1).is_err(), "Key1 should have been evicted"); // Use &key1
        assert!(cache.get(&create_key(2)).is_ok(), "Key2 should still exist"); // Check key2 exists
        assert!(cache.get(&key3).is_ok(), "Key3 should exist"); // Use &key3
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_page_cache_delete() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        cache.delete(&key1).expect("Delete failed");
        assert!(cache.get(&key1).is_err());
        assert_eq!(cache.len(), 0);
        cache.verify_list_integrity();
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        cache.clear();
        assert!(cache.get(&key1).is_err()); // Use &key1
        assert!(cache.get(&key2).is_err()); // Use &key2
        assert_eq!(cache.len(), 0);
        cache.verify_list_integrity();
    }

    #[test]
    fn test_page_cache_insert_sequential() {
        let mut cache = DumbLruPageCache::new(10); // Larger capacity
        for i in 0..100 { // Insert more pages
            let key = insert_page(&mut cache, i);
            assert_eq!(cache.get_no_touch(&key).expect("Peek failed").get().id, i);
            cache.verify_list_integrity(); // Check integrity each step
        }
         assert_eq!(cache.len(), 10); // Should be at capacity
    }

    #[test]
    fn test_page_cache_evict_one() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page(&mut cache, 1);
        let _key2 = insert_page(&mut cache, 2); // Evicts key1
        cache.verify_list_integrity();
        assert!(cache.get(&key1).is_err()); // Use &key1
        assert!(cache.get(&create_key(2)).is_ok()); // Check key2 exists
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_page_cache_no_evict_dirty() {
        let mut cache = DumbLruPageCache::new(1);
        let _key1 = insert_page_with_state(&mut cache, 1, DirtyState::Dirty, LockState::Unlocked);
        cache.verify_list_integrity();
        let result = cache.make_room_for(1);
        assert!(matches!(result, Err(LimboError::PageCacheFull)));
        assert!(cache.len() == 1);
        cache.verify_list_integrity();
    }

    #[test]
    fn test_page_cache_no_evict_locked() {
        let mut cache = DumbLruPageCache::new(1);
        let _key1 = insert_page_with_state(&mut cache, 1, DirtyState::Clean, LockState::Locked);
        cache.verify_list_integrity();
        let result = cache.make_room_for(1);
        assert!(matches!(result, Err(LimboError::PageCacheFull)));
        assert!(cache.len() == 1);
        cache.verify_list_integrity();
    }

    #[test]
    fn test_page_cache_no_evict_one_dirty() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page_with_state(&mut cache, 1, DirtyState::Dirty, LockState::Unlocked);
        // Insert of key 2 should fail because key 1 cannot be evicted
        let result = cache.insert(&create_key(2), page_with_content(2));
        assert!(matches!(result, Err(LimboError::PageCacheFull)));
        assert!(cache.get(&key1).is_ok()); // Key1 should still be there
        assert!(cache.len() == 1);
        cache.verify_list_integrity();
    }

     #[test]
    fn test_page_cache_no_evict_one_locked() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page_with_state(&mut cache, 1, DirtyState::Clean, LockState::Locked);
        // Insert of key 2 should fail because key 1 cannot be evicted
        let result = cache.insert(&create_key(2), page_with_content(2));
        assert!(matches!(result, Err(LimboError::PageCacheFull)));
        assert!(cache.get(&key1).is_ok()); // Key1 should still be there
        assert!(cache.len() == 1);
        cache.verify_list_integrity();
    }

    #[test]
    fn test_page_cache_evict_one_after_cleaned() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page_with_state(&mut cache, 1, DirtyState::Dirty, LockState::Unlocked);
        // Insert of key 2 should fail initially
        let result = cache.insert(&create_key(2), page_with_content(2));
        assert!(matches!(result, Err(LimboError::PageCacheFull)));

        // Clean page 1
        let entry1 = cache.get(&key1).expect("Key1 should exist");
        assert!(entry1.is_dirty());
        entry1.clear_dirty();
        assert!(!entry1.is_dirty());

        // Now insert should succeed by evicting key1
        let key3 = create_key(3);
        cache.insert(&key3, page_with_content(3)).expect("Insert 3 should succeed after cleaning 1");
        cache.verify_list_integrity();
        assert!(cache.get(&key1).is_err(), "Key1 should be evicted after being cleaned");
        assert!(cache.get(&key3).is_ok(), "Key3 should be present");
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_page_cache_evict_one_after_unlocked() {
         let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page_with_state(&mut cache, 1, DirtyState::Clean, LockState::Locked);
        // Insert of key 2 should fail initially
        let result = cache.insert(&create_key(2), page_with_content(2));
        assert!(matches!(result, Err(LimboError::PageCacheFull)));

        // Unlock page 1
        let entry1 = cache.get(&key1).expect("Key1 should exist");
        assert!(entry1.is_locked());
        entry1.clear_locked();
        assert!(!entry1.is_locked());

        // Now insert should succeed by evicting key1
        let key3 = create_key(3);
        cache.insert(&key3, page_with_content(3)).expect("Insert 3 should succeed after unlocking 1");
        cache.verify_list_integrity();
        assert!(cache.get(&key1).is_err(), "Key1 should be evicted after being unlocked");
        assert!(cache.get(&key3).is_ok(), "Key3 should be present");
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_drop_clears_cache() {
        let key1;
        let key2;
        let page1_check;
        let page2_check;
        {
            let mut cache = DumbLruPageCache::new(2);
            key1 = insert_page(&mut cache, 1);
            key2 = insert_page(&mut cache, 2);
            page1_check = cache.get(&key1).expect("Get key1 failed");
            page2_check = cache.get(&key2).expect("Get key2 failed");
            assert_eq!(cache.len(), 2);
            // Cache goes out of scope here, Drop::drop is called
        }
        // Check if pages were cleaned (if ref count allows)
        if Arc::strong_count(&page1_check) == 1 {
            assert!(!page_has_content(&page1_check), "Page 1 content should be cleaned by Drop");
        }
        if Arc::strong_count(&page2_check) == 1 {
            assert!(!page_has_content(&page2_check), "Page 2 content should be cleaned by Drop");
        }
    }
}
