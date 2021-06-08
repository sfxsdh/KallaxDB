#include "headers.h"
#include "utils.h"
#include "items.h"
#include "slab.h"
#include "ioengine.h"
#include "pagecache.h"
#include "slabworker.h"

//#define KEY_TRACE

extern int disable_wal;
extern int nb_pages_per_slab;
extern uint32_t page_size;
extern int create_db;

off_t item_page_num(struct slab *s, size_t idx) {
   return idx;
}


// For read
off_t read_item_in_page(char *item, char *disk_page) {
    struct item_metadata *req_meta = (struct item_metadata *)item;
    struct item_metadata *disk_meta;
    char *req_key = item + sizeof(struct item_metadata);
    char *disk_key = NULL;
    off_t offset = 0, item_size;

    while (offset + sizeof(struct item_metadata) <= page_size) {
        disk_meta = (struct item_metadata *)(disk_page + offset);
        disk_key = (char *)(disk_meta + 1);
        if((req_meta->key_size == disk_meta->key_size) &&
           (!memcmp(req_key, disk_key, req_meta->key_size))) {
            return offset;
        } else if (disk_meta->key_size == 0 && disk_meta->value_size == 0) {
            return -1;
        }

        item_size = sizeof(struct item_metadata) + disk_meta->key_size + disk_meta->value_size;
        offset += item_size;
    }

    return -1;
}

// For write
// If KV found, copy left KVs and then add new KV in the end 
off_t add_item_in_page(char *item, char *disk_page) {
    struct item_metadata *req_meta = (struct item_metadata *)item;
    struct item_metadata *disk_meta;
    char *disk_key;
    size_t item_size, new_item_size, old_size = 0;
    off_t offset = 0;
    char *req_key = item + sizeof(struct item_metadata);
    char *old_meta = NULL;
    int i;

    disk_meta = (struct item_metadata *)(disk_page);
    item_size = sizeof(struct item_metadata) + disk_meta->key_size + disk_meta->value_size;
    while (offset + item_size <= page_size) {
        disk_key = disk_page + offset + sizeof(struct item_metadata);
        if((req_meta->key_size == disk_meta->key_size) &&
           (!memcmp(req_key, disk_key, req_meta->key_size))) {
            // Also check seq ID
            if (req_meta->seq_id <= disk_meta->seq_id) {
                return 0;
            }
            old_meta = (char *)disk_meta;
            old_size = item_size;
        } else if (disk_meta->key_size == 0 && disk_meta->value_size == 0) {
            // Key not written before; try to append it
            break;
        }

        offset += item_size;
        disk_meta = (struct item_metadata *)(disk_page + offset);
        if (offset + sizeof(struct item_metadata) >= page_size) {
            break;
        }
        if (disk_meta->key_size == 0 && disk_meta->value_size == 0) {
            break;
        }
        item_size = sizeof(struct item_metadata) + disk_meta->key_size + disk_meta->value_size;
    }

    assert((char *)disk_meta - disk_page <= page_size);

    // Delete old KV and copy left KVs if applicable
    // Update in place only if the lengths are the same for old and new KV
    // Optimize the procedure if this assumption is valid
#if 0    
    if (old_meta) {
        memcpy(old_meta, old_meta + old_size, (char *)disk_meta - (old_meta + old_size));
    }
#else
    if (old_meta) {
        for (i = 0; i < (char *)disk_meta - (old_meta + old_size); i++) {
            memcpy(old_meta + i, old_meta + old_size + i, 1);
        }
        assert(old_meta + old_size + i - disk_page <= page_size);
    }
#endif

    disk_meta = (struct item_metadata *)((char *)disk_meta - old_size);
    assert((char *)disk_meta - disk_page <= page_size);
    // Copy new KV given enough space
    // Otherwise return -1
    // Set zero for page left space if necessary 
    new_item_size = sizeof(struct item_metadata) + req_meta->key_size + req_meta->value_size;
    // assert(new_item_size == 432);
    if (((char *)disk_meta + new_item_size) - disk_page > page_size) {
        if (old_meta) {
            memset((char *)disk_meta, 0, old_size);
            assert((char *)disk_meta + old_size - disk_page <= page_size);
        }
        return -1;
    } else {
        assert((char *)disk_meta + new_item_size - disk_page <= page_size);
        memcpy((char *)disk_meta, item, new_item_size);
        if (old_meta && (new_item_size < old_size)) {
            memset((char *)disk_meta + new_item_size, 0, old_size - new_item_size);
        }
        return (char *)disk_meta - disk_page;
    }

    die("Should not be here in add item in page function!\n");
    return -1;
}

// TODO: recovery path
struct slab* create_slab(char *path_prefix, int slab_id) {
    struct slab *s = calloc(1, sizeof(*s));
    int fd = -1, file_exist = 0, allocate = 0;
    // struct stat st;
    char path[MAX_FILE_NAME_LEN];

    strcpy(s->path, path_prefix);
    s->id = slab_id;
    sprintf(path, "%s/%d-%d.slab", path_prefix, slab_id, s->idx_old);
    file_exist = !access(path, 0);

    if (create_db) {
        if (file_exist) {
            remove(path);
        }
        fd = open(path,  O_RDWR|O_CREAT|O_DIRECT, 0666);
        if (fd < 0) {
	        die("Cannot allocate slab %s", path);
        } else {
            // printf("Initial fd = %d for path %s\n", fd, path);
        }
	    allocate = fallocate(fd, 0, 0, (uint64_t)nb_pages_per_slab * page_size);
        if (allocate < 0) {
            die("Cound not allocate space for slab %s!\n", path);
        }
    }


    pthread_spin_init(&(s->meta_lock), 0);
    pthread_spin_lock(&(s->meta_lock));
    s->slab_switch = 0;
    s->fd_old = fd;
    s->old_ref = 0;
    s->idx_old = 0;
    s->fd_new = -1;
    s->rehash_radix = 0;
    pthread_spin_unlock(&(s->meta_lock));

    s->init_num_pages = nb_pages_per_slab;
    s->item_being_written = 0;
    s->in_rehash_task_queue = 0;

    s->except_wal = createRBtree(normalCompare);
    pthread_mutex_init(&(s->except_wal_lock), NULL);
    pthread_mutex_init(&(s->except_wal_index_lock), NULL);
    pthread_mutex_lock(&s->except_wal_lock);
    s->except_fp_old = NULL;
    s->except_fp_new = NULL;
    s->except_wal_index = 0;
    pthread_mutex_unlock(&s->except_wal_lock);
    if (create_db) {
        except_wal_open(s, 0);
    }

    s->page_bitmap = (uint8_t *)calloc(BITMAP_BYTES, 1);
    s->head = (item_entry *)calloc(sizeof(item_entry), 1);
    s->head->next = NULL;

    return s; 
}

/*
 * Synchronous read item
 */
char *read_item(struct slab *s, char * item, size_t idx) {
#if 0
   size_t page_num = item_page_num(s, idx);
   char *disk_data = safe_pread(s->fd, page_num*page_size);
   off_t page_offset = item_in_page_offset(item, disk_data);
   return (page_offset == -1) ? NULL: &disk_data[page_offset];
#endif
   return NULL;
}

void update_item(struct slab_callback *callback, char *disk_page, off_t offset_in_page) {
   char *item = callback->item;
   struct item_metadata *meta = (struct item_metadata *)item;
   struct item_metadata *old_meta = (struct item_metadata *)&disk_page[offset_in_page];

   if(callback->action == UPDATE) {
      size_t new_key_size = meta->key_size;
      size_t old_key_size = old_meta->key_size;
      if(new_key_size != old_key_size) {
         die("Updating an item, but key size changed! Likely this is because 2 keys have the same prefix in the index and we got confused because they have the same prefix.\n");
      }

      char *new_key = &disk_page[offset_in_page + sizeof(*meta)];
      char *old_key = &(((char*)old_meta)[sizeof(*meta)]);
      if(memcmp(new_key, old_key, new_key_size))
         die("Updating an item, but key mismatch! Likely this is because 2 keys have the same prefix in the index\n");
   }

   if(meta->key_size == -1)
      memcpy(&disk_page[offset_in_page], meta, sizeof(*meta));
   else
      memcpy(&disk_page[offset_in_page], item, get_item_size(item));
}

void read_item_async_cb(struct slab_callback *callback) {
#if 0
   char *disk_page = callback->lru_entry->page;
   off_t in_page_offset = item_in_page_offset(callback->item, disk_page);
   char *item = NULL;
   if (in_page_offset != -1) {
       item = &disk_page[in_page_offset];
   } else {
       printf("not found item\n");
   }
   if(callback->cb)
      callback->cb(callback, item);
   free_callback(callback, item);
#endif
}

void read_item_async(struct slab_callback *callback) {
#if 0
   char *item = read_from_except_wal(callback->slab, callback->item);
   if (item) {
       if (callback->cb)
           callback->cb(callback, item);
       free_callback(callback, item);
       free(item);
   } else {
       callback->io_cb = read_item_async_cb;
       read_page_async(callback);
   }
#endif
}

static inline uint8_t bit_reset(uint8_t *page_bitmap, uint64_t page_idx) {
    uint32_t byte = (page_idx >> 3) % BITMAP_BYTES;
    uint8_t bit = (page_idx & 0x7);

    if ((page_bitmap[byte] & (1 << bit)) > 0) {
        page_bitmap[byte] &= (~(1 << bit));
        return 1;
    } else {
        return 0;
    }
}

/*
 * Asynchronous update item:
 * - First read the page where the item is staying
 * - Once the page is in page cache, write it
 * - Then send the order to flush it.
 */

void add_to_except_item_list(struct slab_callback *callback) {
    struct item_metadata *meta;
    item_entry *node = NULL, *head, *tmp;
    uint32_t size;

    node = (item_entry *)calloc(sizeof(item_entry), 1);
    meta = (struct item_metadata *)callback->item;
    size = sizeof(struct item_metadata) + meta->key_size;
    node->data = (struct item_metadata *)malloc(size);
    memcpy(node->data, callback->item, size);
    //assert(size == 40);

    head = callback->slab->head;
    tmp = head->next;
    head->next = node;
    node->next = tmp;
}

void update_item_async_cb2(struct slab_callback *callback) {
    // char *disk_page = callback->page_buffer;
  
    if (bit_reset(callback->slab->page_bitmap, callback->page_idx) == 0) {
        die("Bit map error!!! slab_idx = %ld\n", callback->page_idx);
    }
	remove_from_wal_index(callback->ctx, (struct item_metadata *)callback->item);
    __sync_fetch_and_sub(&callback->slab->item_being_written, 1);
#ifdef KEY_TRACE
    struct item_metadata *meta = (struct item_metadata *)callback->item;
    printf("key = %ld has been written to normal slab during flush!\n", *(uint64_t *)(meta + 1));
#endif
}

void migrate_item_async_cb2(struct slab_callback *callback) {
    // char *disk_page = callback->page_buffer;

    if (bit_reset(callback->slab->migrate_page_bitmap, callback->page_idx) == 0) {
        die("Bit map error in migrating!!! slab_idx = %ld\n", callback->page_idx);
    }

    // Items in exception WAL moved to new slab during rehash should be deleted from exception WAL index, so we add them to a list and do the deletion after rehash is done
    add_to_except_item_list(callback); 
#ifdef KEY_TRACE
    struct item_metadata *meta = (struct item_metadata *)callback->item;
    printf("key = %ld has been written to new slab during migration!\n", *(uint64_t *)(meta + 1));
#endif
}

// We need to detect whether it is an update
void update_item_async_cb1(struct slab_callback *callback) {
    char *disk_page = callback->page_buffer;
    char *item = callback->item;
    // struct item_metadata *meta = (struct item_metadata *)item;
    off_t offset = add_item_in_page(item, disk_page);

    if (offset == -1) {
	    except_wal_write(callback->slab, item);
        if (bit_reset(callback->slab->page_bitmap, callback->page_idx) == 0) {
            die("Bitmap error when page overflow!\n");
        }
        remove_from_wal_index(callback->ctx, (struct item_metadata *)callback->item);
        callback->io_cb = NULL;
        callback->page_full = 1;
        __sync_fetch_and_sub(&callback->slab->item_being_written, 1);
#ifdef KEY_TRACE
        printf("key = %ld has been written to exc wal during flush due to page overflow!\n", *(uint64_t *)(meta + 1));
#endif
    } else {
        callback->io_cb = update_item_async_cb2;
        write_page_async(callback);
    }
}

void migrate_item_async_cb1(struct slab_callback *callback) {
    char *disk_page = callback->page_buffer;
    char *item = callback->item;
    // struct item_metadata *meta = (struct item_metadata *)item;
    off_t offset = add_item_in_page(item, disk_page);

    if (offset == -1) {
	    except_wal_write(callback->slab, item);
        if (bit_reset(callback->slab->migrate_page_bitmap, callback->page_idx) == 0) {
            die("Bitmap error when page overflow in migration!\n");
        }

        callback->io_cb = NULL;
        callback->page_full = 1;
#ifdef KEY_TRACE
        printf("key = %ld has been rewritten to exc wal due to page overflow during migration!\n", *(uint64_t *)(meta + 1));
#endif
    } else {
        callback->io_cb = migrate_item_async_cb2;
        write_page_async(callback);
    }
}

void update_item_async(struct slab_callback *callback) {
#if 0
    callback->io_cb = update_item_async_cb1;
    read_page_async(callback);
#endif
}

void add_item_async_cb1(struct slab_callback *callback) {
    //printf("add_item_async_cb1 after read page async!\n");
	update_item_async(callback);
}

void add_item_async(struct slab_callback *callback) {
    callback->io_cb = update_item_async_cb1;
    read_page_async(callback);
}

void migrate_item_async(struct slab_callback *callback) {
    callback->io_cb = migrate_item_async_cb1;
    read_page_async(callback);
}

void add_item_sync(struct slab_callback *callback) {
    int ret;
    off_t off;
    ret = pread(callback->fd, callback->page_buffer, page_size, page_size * callback->page_idx);
    assert(ret == page_size);

    off = add_item_in_page(callback->item, callback->page_buffer);
    if (off == -1) {
        except_wal_write(callback->slab, callback->item);
        // printf("Page overflow in sync!\n");
    } else {
        ret = pwrite(callback->fd, callback->page_buffer, page_size, page_size * callback->page_idx);
        assert(ret == page_size);
    }
}

void remove_item_by_idx_async_cb1(struct slab_callback *callback) {
   char *disk_page = callback->page_buffer;

   // struct slab *s = callback->slab;

   off_t offset_in_page = 0;

   struct item_metadata *meta = (struct item_metadata *) &disk_page[offset_in_page];
   if(meta->key_size == -1) { // already removed
      if(callback->cb)
         callback->cb(callback, &disk_page[offset_in_page]);
      free_callback(callback, NULL);
      return;
   }

   meta->key_size = -1;

   callback->io_cb = update_item_async_cb2; 
   write_page_async(callback);
}

void remove_item_async(struct slab_callback *callback) {
   callback->io_cb = remove_item_by_idx_async_cb1;
   read_page_async(callback);
}
