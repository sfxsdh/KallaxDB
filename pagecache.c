#include "headers.h"

/*
 * Basic page cache implementation.
 *
 * Only 1 operation: get_page(hash).
 * Hash must be chosen carefully otherwise the page cache might return the same cached page for two different files / offsets.
 * Currently the IO engine appends the file descriptor number and the page offset to create a hash (fd << 40 + page_num).
 *
 * A hash is used to remember what is in the cache.
 * hash_to_page[hash].data1 = address of the page
 * hash_to_page[hash].data2 = lru entry of the page
 *
 * The lru entry is used to have a lru order of cached content + some metadata.
 * lru_entry.dirty = the page has been written but not flushed
 * lru_entry.contains_data = the page already contains the correct content, no need to read page from disk
 * These metadata are cleared by the page cache and set by the IO engine.
 *
 * The page cache shouldn't be used directly, the interface of the IO engine is a more convenient way to access data.
 */

void page_cache_init(struct pagecache *p) {
}

struct lru *add_page_in_lru(struct pagecache *p, void *page, uint64_t hash) {
   return NULL;
}

void bump_page_in_lru(struct pagecache *p, struct lru *me, uint64_t hash) {
}

void *get_my_page(struct pagecache *p, int idx) {
   return NULL;
}

/*
 * Get a page from the page cache.
 * *page will be set to the address in the page cache
 * @return 1 if the page already contains the right data, 0 otherwise.
 */
int get_page(struct pagecache *p, uint64_t hash, void **page, struct lru **lru) {
   return 0;
}
