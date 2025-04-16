use std::{cell::RefCell, collections::HashMap, ptr::NonNull, result::Result, sync::Arc};
use crate::error::LimboError;

use tracing::{debug, trace, warn};

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

impl PageCacheEntry {
    fn as_non_null(&mut self) -> NonNull<PageCacheEntry> {
        NonNull::new(&mut *self).unwrap()
    }
}

pub struct DumbLruPageCache {
    capacity: usize,
    map: RefCell<HashMap<PageCacheKey, NonNull<PageCacheEntry>>>,
    head: RefCell<Option<NonNull<PageCacheEntry>>>,
    tail: RefCell<Option<NonNull<PageCacheEntry>>>,
}
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

    pub fn contains_key(&mut self, key: &PageCacheKey) -> bool {
        self.map.borrow().contains_key(key)
    }

    #[must_use]
    pub fn insert(&mut self, key: &PageCacheKey, value: PageRef) -> Result<(), LimboError> {
        trace!("cache_insert(key={:?})", key);
        let mut map = self.map.borrow_mut();
        if let Some(ptr_ref_mut) = map.get_mut(key) {
            trace!("cache_insert(key={:?}) replacing existing entry", key);
            let entry_mut = unsafe { ptr_ref_mut.as_mut() };
            // Clean the OLD page first
            self.clean_page(&mut entry_mut.page)?; // ...or fail
            entry_mut.page = value;
            let ptr_copy = *ptr_ref_mut;
            self._detach(ptr_ref_mut);
            self._insert_head(&ptr_copy);
        } else {
            trace!("cache_insert(key={:?}) evicting some entry", key);
            if !self.make_room_for_page() {
                return Err(LimboError::PageCacheFull);
            }

            let entry = Box::new(PageCacheEntry {
                key: key.clone(),
                next: None,
                prev: None,
                page: value,
            });
            let ptr_raw = Box::into_raw(entry);
            let ptr = unsafe { ptr_raw.as_mut().unwrap().as_non_null() };
            self._insert_head(&ptr);

            map.insert(key.clone(), ptr);
        }
        Ok(())
    }

    pub fn delete(&mut self, key: PageCacheKey) {
        trace!("cache_delete(key={:?})", key);
        self._delete(&key, true)
    }

    pub fn _delete(&mut self, key: &PageCacheKey, clean_page: bool) {
        let ptr = self.map.borrow_mut().remove(&key);
        if ptr.is_none() {
            return;
        }
        let ptr = ptr.unwrap();
        self.detach(&ptr, clean_page);
        unsafe { std::ptr::drop_in_place(ptr.as_ptr()) };
    }

    fn get_ptr(&mut self, key: &PageCacheKey) -> Option<NonNull<PageCacheEntry>> {
        let m = self.map.borrow_mut();
        let ptr = m.get(key);
        ptr.copied()
    }

    pub fn get(&mut self, key: &PageCacheKey) -> Option<PageRef> {
        self.peek(key, true)
    }

    /// Get page without promoting entry
    pub fn peek(&mut self, key: &PageCacheKey, touch: bool) -> Option<PageRef> {
        trace!("cache_get(key={:?})", key);
        let mut ptr = self.get_ptr(key)?;
        let page = unsafe { ptr.as_mut().page.clone() };
        if touch {
            self._detach(&ptr);
            self._insert_head(&ptr);
        }
        Some(page)
    }

    pub fn resize(&mut self, capacity: usize) {
        let _ = capacity;
        todo!();
    }

    fn clean_page(&mut self, page: &mut PageRef) -> Result<(), LimboError> {
        if let Some(page_mut) = Arc::get_mut(page) {
            page_mut.clear_loaded();
            debug!("cleaning up page {}", page_mut.get().id);
            let _ = page_mut.get().contents.take();
            Ok(())
        } else {
            Err(LimboError::PageHasMultipleRefs)
        }
    }

    #[must_use]
    fn detach(&mut self, entry: &mut NonNull<PageCacheEntry>, clean_page: bool) -> Result<(), LimboError> {
        let entry_mut = unsafe { entry.as_mut() };
        
        if entry_mut.page.is_locked() {
            return Err(LimboError::PageLocked);
        }
        if entry_mut.page.is_dirty() {
            return Err(LimboError::PageDirty);
        }

        if clean_page {
            self.clean_page(&mut entry_mut.page)?;
        }

        self._detach(entry);
        Ok(())
    }

    fn _detach(&mut self, entry: &mut NonNull<PageCacheEntry>) {
        let (next, prev) = unsafe {
            let c = entry.as_mut();
            let next = c.next;
            let prev = c.prev;
            c.prev = None;
            c.next = None;
            (next, prev)
        };

        match (prev, next) {
            (None, None) => {
                self.head.replace(None);
                self.tail.replace(None);
            }
            (None, Some(mut n)) => {
                unsafe { n.as_mut().prev = None };
                self.head.borrow_mut().replace(n);
            }
            (Some(mut p), None) => {
                unsafe { p.as_mut().next = None };
                self.tail = RefCell::new(Some(p));
            }
            (Some(mut p), Some(mut n)) => unsafe {
                let p_mut = p.as_mut();
                p_mut.next = Some(n);
                let n_mut = n.as_mut();
                n_mut.prev = Some(p);
            },
        };
    }

    /// inserts into head, assuming we detached first
    fn _insert_head(&mut self, mut entry: &NonNull<PageCacheEntry>) {
        if let Some(mut head) = *self.head.borrow_mut() {
            unsafe {
                entry.as_mut().next.replace(head);
                let head = head.as_mut();
                head.prev = Some(*entry);
            }
        }

        if self.tail.borrow().is_none() {
            self.tail.borrow_mut().replace(*entry);
        }
        self.head.borrow_mut().replace(*entry);
    }

    // Tries to evict pages until there's capacity for one more page
    #[must_use]
    fn make_room_for_page(&mut self) -> bool {
        if self.len() < self.capacity {
            return true;
        }

        let mut current = match *self.tail.borrow() {
            Some(tail) => tail,
            None => {
                debug!("make_room_for_page() nothing at tail");
                return false;
            }
        };

        while self.len() >= self.capacity {
            let current_entry = unsafe { current.as_mut() };

            if current_entry.page.is_dirty() || current_entry.page.is_locked() {
                match current_entry.prev {
                    Some(prev) => current = prev,
                    None => {
                        debug!("make_room_for_page() not enough clean and unlocked pages to evict");
                        return false;
                    }
                }
                continue;
            }

            debug!("make_room_for_page(key={:?})", current_entry.key);

            let prev = current_entry.prev;
            let key_to_remove = current_entry.key.clone();
            if !self.detach(current, true) {
                warn!("make_room_for_page(key={:?}) failed to detach", current_entry.key);
                break;
            }
            assert!(self.map.borrow_mut().remove(&key_to_remove).is_some());
            if prev.is_none() {
                break;
            }
            current = prev.unwrap();
        }
        true
    }

    pub fn clear(&mut self) {
        let to_remove: Vec<PageCacheKey> = self.map.borrow().iter().map(|v| v.0.clone()).collect();
        for key in to_remove {
            self.delete(key);
        }
    }

    pub fn print(&mut self) {
        println!("page_cache={}", self.map.borrow().len());
        println!("page_cache={:?}", self.map.borrow())
    }

    pub fn len(&self) -> usize {
        self.map.borrow().len()
    }

    #[cfg(test)]
    fn get_entry_ptr(&self, key: &PageCacheKey) -> Option<NonNull<PageCacheEntry>> {
        self.map.borrow().get(key).copied()
    }

    #[cfg(test)]
    fn verify_list_integrity(&self) {
        let map_len = self.map.borrow().len();
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
                    self.map.borrow().contains_key(&node_ref.key),
                    "Node key {:?} not found in map during forward traversal",
                    node_ref.key
                );
                assert_eq!(
                    self.map.borrow().get(&node_ref.key).copied(),
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
                    self.map.borrow().contains_key(&node_ref.key),
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

    fn create_key(id: usize) -> PageCacheKey {
        PageCacheKey::new(id, Some(id as u64))
    }

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

    fn insert_page(cache: &mut DumbLruPageCache, id: usize) -> PageCacheKey {
        let key = create_key(id);
        let page = page_with_content(id);
        cache.insert(key.clone(), page);
        key
    }

    fn page_has_content(page: &PageRef) -> bool {
        page.is_loaded() && page.get().contents.is_some()
    }

    #[derive(Clone, Copy)]
    pub enum DirtyState {
        Clean,
        Dirty,
    }

    #[derive(Clone, Copy)]
    pub enum LockState {
        Unlocked,
        Locked,
    }

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
        cache.insert(key.clone(), page.clone());
        key
    }

    #[test]
    fn test_detach_only_element() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = insert_page(&mut cache, 1);
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 1);
        assert!(cache.head.borrow().is_some());
        assert!(cache.tail.borrow().is_some());
        assert_eq!(*cache.head.borrow(), *cache.tail.borrow());

        cache.delete(key1.clone());

        assert_eq!(
            cache.len(),
            0,
            "Length should be 0 after deleting only element"
        );
        assert!(
            cache.map.borrow().get(&key1).is_none(),
            "Map should not contain key after delete"
        );
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
        assert_eq!(
            unsafe { &head_ptr_before.as_ref().key },
            &key3,
            "Initial head check"
        );

        cache.delete(key3.clone());

        assert_eq!(cache.len(), 2, "Length should be 2 after deleting head");
        assert!(
            cache.map.borrow().get(&key3).is_none(),
            "Map should not contain deleted head key"
        );
        cache.verify_list_integrity();

        let new_head_ptr = cache.head.borrow().unwrap();
        assert_eq!(
            unsafe { &new_head_ptr.as_ref().key },
            &key2,
            "New head should be key2"
        );
        assert!(
            unsafe { new_head_ptr.as_ref().prev.is_none() },
            "New head's prev should be None"
        );

        let tail_ptr = cache.tail.borrow().unwrap();
        assert_eq!(
            unsafe { new_head_ptr.as_ref().next },
            Some(tail_ptr),
            "New head's next should point to tail (key1)"
        );
    }

    #[test]
    fn test_detach_tail() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = insert_page(&mut cache, 1); // Tail
        let key2 = insert_page(&mut cache, 2); // Middle
        let _key3 = insert_page(&mut cache, 3); // Head
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 3);

        let tail_ptr_before = cache.tail.borrow().unwrap();
        assert_eq!(
            unsafe { &tail_ptr_before.as_ref().key },
            &key1,
            "Initial tail check"
        );

        cache.delete(key1.clone()); // Delete tail

        assert_eq!(cache.len(), 2, "Length should be 2 after deleting tail");
        assert!(
            cache.map.borrow().get(&key1).is_none(),
            "Map should not contain deleted tail key"
        );
        cache.verify_list_integrity();

        let new_tail_ptr = cache.tail.borrow().unwrap();
        assert_eq!(
            unsafe { &new_tail_ptr.as_ref().key },
            &key2,
            "New tail should be key2"
        );
        assert!(
            unsafe { new_tail_ptr.as_ref().next.is_none() },
            "New tail's next should be None"
        );

        let head_ptr = cache.head.borrow().unwrap();
        assert_eq!(
            unsafe { head_ptr.as_ref().prev },
            None,
            "Head's prev should point to new tail (key2)"
        );
        assert_eq!(
            unsafe { head_ptr.as_ref().next },
            Some(new_tail_ptr),
            "Head's next should point to new tail (key2)"
        );
        assert_eq!(
            unsafe { new_tail_ptr.as_ref().next },
            None,
            "Double check new tail's next is None"
        );
    }

    #[test]
    fn test_detach_middle() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = insert_page(&mut cache, 1); // Tail
        let key2 = insert_page(&mut cache, 2); // Middle
        let key3 = insert_page(&mut cache, 3); // Middle
        let _key4 = insert_page(&mut cache, 4); // Head
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 4);

        let head_ptr_before = cache.head.borrow().unwrap();
        let tail_ptr_before = cache.tail.borrow().unwrap();

        cache.delete(key2.clone()); // Detach a middle element (key2)

        assert_eq!(cache.len(), 3, "Length should be 3 after deleting middle");
        assert!(
            cache.map.borrow().get(&key2).is_none(),
            "Map should not contain deleted middle key2"
        );
        cache.verify_list_integrity();

        // Check neighbors
        let key1_ptr = cache.get_entry_ptr(&key1).expect("Key1 should still exist");
        let key3_ptr = cache.get_entry_ptr(&key3).expect("Key3 should still exist");
        assert_eq!(
            unsafe { key3_ptr.as_ref().next },
            Some(key1_ptr),
            "Key3's next should point to key1"
        );
        assert_eq!(
            unsafe { key1_ptr.as_ref().prev },
            Some(key3_ptr),
            "Key1's prev should point to key2"
        );

        assert_eq!(
            cache.head.borrow().unwrap(),
            head_ptr_before,
            "Head should remain key4"
        );
        assert_eq!(
            cache.tail.borrow().unwrap(),
            tail_ptr_before,
            "Tail should remain key1"
        );
    }

    #[test]
    fn test_detach_via_delete() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = create_key(1);
        let page1 = page_with_content(1);
        cache.insert(key1.clone(), page1.clone());
        assert!(
            page_has_content(&page1),
            "Page content should exist before delete"
        );
        cache.verify_list_integrity();

        cache.delete(key1.clone());
        assert_eq!(cache.len(), 0);
        assert!(
            !page_has_content(&page1),
            "Page content should be removed after delete"
        );
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_via_insert() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page_with_content(1);

        cache.insert(key1.clone(), page1_v1.clone());
        assert!(
            page_has_content(&page1_v1),
            "Page1 V1 content should exist initially"
        );
        cache.verify_list_integrity();

        cache.insert(key1.clone(), page1_v2.clone()); // Trigger delete page1_v1

        assert_eq!(
            cache.len(),
            1,
            "Cache length should still be 1 after replace"
        );
        assert!(
            !page_has_content(&page1_v1),
            "Page1 V1 content should be cleaned after being replaced in cache"
        );
        assert!(
            page_has_content(&page1_v2),
            "Page1 V2 content should exist after insert"
        );

        let current_page = cache.get(&key1).unwrap();
        assert!(
            Arc::ptr_eq(&current_page, &page1_v2),
            "Cache should now hold page1 V2"
        );

        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_nonexistent_key() {
        let mut cache = DumbLruPageCache::new(5);
        let key_nonexist = create_key(99);

        cache.delete(key_nonexist.clone()); // no-op
    }

    #[test]
    fn test_page_cache_fuzz() {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        tracing::info!("super seed: {}", seed);
        let max_pages = 10;
        let mut cache = DumbLruPageCache::new(10);
        let mut lru = LruCache::new(NonZeroUsize::new(10).unwrap());

        for _ in 0..10000 {
            match rng.next_u64() % 3 {
                0 => {
                    // add
                    let id_page = rng.next_u64() % max_pages;
                    let id_frame = rng.next_u64() % max_pages;
                    let key = PageCacheKey::new(id_page as usize, Some(id_frame));
                    #[allow(clippy::arc_with_non_send_sync)]
                    let page = Arc::new(Page::new(id_page as usize));
                    // println!("inserting page {:?}", key);
                    cache.insert(key.clone(), page.clone());
                    lru.push(key, page);
                    assert!(cache.len() <= 10);
                }
                1 => {
                    // remove
                    let random = rng.next_u64() % 2 == 0;
                    let key = if random || lru.is_empty() {
                        let id_page = rng.next_u64() % max_pages;
                        let id_frame = rng.next_u64() % max_pages;
                        let key = PageCacheKey::new(id_page as usize, Some(id_frame));
                        key
                    } else {
                        let i = rng.next_u64() as usize % lru.len();
                        let key = lru.iter().skip(i).next().unwrap().0.clone();
                        key
                    };
                    // println!("removing page {:?}", key);
                    lru.pop(&key);
                    cache.delete(key);
                }
                2 => {
                    // test contents
                    for (key, page) in &lru {
                        // println!("getting page {:?}", key);
                        cache.peek(&key, false).unwrap();
                        assert_eq!(page.get().id, key.pgno);
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key1).unwrap().get().id, 1);
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
    }

    #[test]
    fn test_page_cache_over_capacity() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        assert!(cache.get(&key1).is_none());
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
        assert_eq!(cache.get(&key3).unwrap().get().id, 3);
    }

    #[test]
    fn test_page_cache_delete() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        cache.delete(key1.clone());
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        cache.clear();
        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_none());
    }

    #[test]
    fn test_page_cache_insert_sequential() {
        let mut cache = DumbLruPageCache::new(2);
        for i in 0..10000 {
            let key = insert_page(&mut cache, i);
            assert_eq!(cache.peek(&key, false).unwrap().get().id, i);
        }
    }

    #[test]
    fn test_page_cache_evict_one() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page(&mut cache, 1);
        let _ = insert_page(&mut cache, 2);
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_page_cache_no_evict_dirty() {
        let mut cache = DumbLruPageCache::new(1);
        let _ = insert_page_with_state(&mut cache, 1, DirtyState::Dirty, LockState::Unlocked);
        assert!(cache.make_room_for_page() == false);
        assert!(cache.len() == 1);
    }

    #[test]
    fn test_page_cache_no_evict_locked() {
        let mut cache = DumbLruPageCache::new(1);
        let _ = insert_page_with_state(&mut cache, 1, DirtyState::Clean, LockState::Locked);
        assert!(cache.make_room_for_page() == false);
        assert!(cache.len() == 1);
    }

    #[test]
    fn test_page_cache_no_evict_one_dirty() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page_with_state(&mut cache, 1, DirtyState::Dirty, LockState::Unlocked);
        let _ = insert_page(&mut cache, 2); // fails to evict 1
        assert!(cache.get(&key1).is_some());
    }

    #[test]
    fn test_page_cache_no_evict_one_locked() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page_with_state(&mut cache, 1, DirtyState::Clean, LockState::Locked);
        let _ = insert_page(&mut cache, 2); // fails to evict 1
        assert!(cache.get(&key1).is_some());
    }

    #[test]
    fn test_page_cache_evict_one_after_cleaned() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page_with_state(&mut cache, 1, DirtyState::Dirty, LockState::Unlocked);
        let _ = insert_page(&mut cache, 2);
        let entry1 = cache.get(&key1);
        assert!(entry1.is_some());
        entry1.unwrap().clear_dirty();
        let _ = insert_page(&mut cache, 3);
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_page_cache_evict_one_after_unlocked() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page_with_state(&mut cache, 1, DirtyState::Clean, LockState::Locked);
        let _ = insert_page(&mut cache, 2);
        let entry1 = cache.get(&key1);
        assert!(entry1.is_some());
        entry1.unwrap().clear_locked();
        let _ = insert_page(&mut cache, 3);
        assert!(cache.get(&key1).is_none());
    }
}
