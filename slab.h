#ifndef SLAB_H
#define SLAB_H 1

#include "ioengine.h"
#include "wal_rbtree.h"

struct slab;
struct slab_callback;

typedef struct item_entry_s {
	struct item_metadata *data;
	struct item_entry_s *next;
} item_entry;

struct slab {
    //struct slab_context *ctx;

    int id;

    // volatile int fd;
    char path[MAX_FILE_NAME_LEN];
    // volatile uint64_t size_on_disk;
    // volatile size_t num_pages;

    pthread_spinlock_t meta_lock;
    int idx_old;
    uint32_t old_ref;
    int fd_old;
    
    // int idx_new;
    // uint32_t idx_new_ref;
    int fd_new;
    uint8_t slab_switch;

    uint32_t init_num_pages;
    uint32_t rehash_radix;
    // uint32_t rehash_radix;
    // volatile being_rehashed;

    uint32_t item_being_written;
    volatile uint8_t in_rehash_task_queue;
    rbtree *except_wal;

    volatile long except_wal_item_cnt;
    volatile long except_wal_item_migration_cnt;

    pthread_mutex_t except_wal_lock;
    pthread_mutex_t except_wal_index_lock;
    volatile int except_wal_inner_idx;
    uint32_t except_wal_index;
    uint64_t except_wal_size;
    uint64_t synced_except_wal_size;
    uint64_t except_offset;

    FILE* except_fp_old;
    FILE* except_fp_new;

    uint8_t *page_bitmap;
    uint8_t *migrate_page_bitmap;

    item_entry *head;
};

/* This is the callback enqueued in the engine.
 * slab_callback->item = item looked for (that needs to be freed)
 * item = page on disk (in the page cache)
 */
typedef void (slab_cb_t)(struct slab_callback *, char *item);
enum slab_action { ADD, UPDATE, DELETE, READ, SCAN, READ_NO_LOOKUP, ADD_OR_UPDATE };
enum callback_tag {INIT, REDO};

struct slab_callback {
    struct slab_context *ctx;
    struct io_context *io_ctx;
    uint64_t hash;
    int fd;

    slab_cb_t *cb;
    char *payload;
    char *item;

    // Private
    enum slab_action action;
    enum callback_tag tag;
    struct slab *slab;
    union {
        uint64_t slab_idx;
        uint64_t tmp_page_number; // when we add a new item we don't always know it's idx directly, sometimes we just know which page it will be placed on
    };
    struct lru *lru_entry;
    io_cb_t *io_cb;
    int id;
    size_t tid;
    size_t seq;
    void *page_buffer;

    uint64_t page_idx;
    int slot_idx;
    uint8_t rd;
    uint8_t page_full;
    uint8_t migration;

    volatile uint8_t index_added;
};

// struct slab* create_slab(size_t item_size, size_t slab_size, int slab_id);
struct slab* create_slab(char *path, int slab_id);
struct slab* resize_slab(struct slab *s);

char *read_item(struct slab *s, char * item, size_t idx);
void read_item_async(struct slab_callback *callback);
void add_item_async(struct slab_callback *callback);
void migrate_item_async(struct slab_callback *callback);
void update_item_async(struct slab_callback *callback);
void remove_item_async(struct slab_callback *callback);
void add_to_except_item_list(struct slab_callback *callback);

void add_item_sync(struct slab_callback *callback);

off_t item_page_num(struct slab *s, size_t idx);
//off_t item_in_page_offset(char *item, char * disk_page);
//off_t free_item_in_page_offset(char *item, char * disk_page);
void update_item(struct slab_callback *callback, char *disk_page, off_t offset_in_page);
struct slab* get_rehash_slab(struct slab_context *ctx);
off_t read_item_in_page(char *item, char * disk_page);
off_t add_item_in_page(char *item, char * disk_page);
int insert_into_exceptional_tree(struct slab_context *ctx, struct slab *s, struct item_metadata *meta);
#endif
