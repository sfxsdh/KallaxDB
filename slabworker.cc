#include <atomic>
#include <vector>
#include "headers.h"
#include "options.h"
#include <dirent.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "rocks_meta.h"

volatile int YCSB_start;
int create_db;
int run_test;
int nb_paths;
int nb_workers;
int nb_slabs;

uint64_t load_kv_num;
uint64_t request_kv_num;

int loader_num;
int client_num;

size_t item_size;
size_t key_size;
size_t value_size;
uint64_t slab_size;
uint32_t page_size;

int path_num;
char** db_path;
uint32_t queue_depth;

int nb_slabs_per_worker;
int nb_slabs_per_path;
int use_default_path;
int use_default_bench;
int nb_rehash;

int g_is_var;
int scan;

volatile uint8_t worker_idle[MAX_WORKER_NUM];
volatile uint8_t rehash_idle[MAX_REHASH_NUM];

bench_t* db_bench;
int db_bench_count;

//#define WAL_DEBUG

//int nb_workers;
//int nb_slabs_per_worker;
int nb_pages_per_slab;

int nb_disks;
static int nb_workers_launched;
static int nb_workers_ready;
static int nb_rehash_workers_ready;
static int nb_del_wal_workers_ready;
static int nb_rocks_workers_ready;


std::atomic<long> exp_num;
std::atomic<int> conflict_page_kv;

std::atomic<int> need_throttle;
std::atomic<long> not_found_item;
std::atomic<long> error_read;
//std::atomic<long> except_wal_item_cnt;
//std::atomic<long> except_wal_item_migration_cnt;


// pthread_mutex_t hash_lock;

static std::atomic<int> rehash_times;

// int rehash_threads_idx[REHASH_THREADS];

std::atomic<long> item_cnt_in_except_wal;


struct slab **slabs;

static uint64_t total_written_kv_num;
static uint64_t total_flushed_kv_num;
static uint64_t cur_seq_id;

#define INVALID_REHASH_INDEX (0xFFFF)

typedef struct recovery_context_s {
    char path[MAX_FILE_NAME_LEN];
    uint32_t worker_id;
    uint32_t wal_start_idx;
    uint32_t wal_end_idx;
} recovery_context_t;

typedef struct rehash_elem_s {
    uint32_t idx;
    struct rehash_elem_s *prev;
    struct rehash_elem_s *next;
} rehash_elem_t;

enum {
    NONE_KEY,
    BUF_KEY,
    ROCKS_KEY,
};

static __thread struct io_context *read_io_ctx; 
static __thread char *read_page_buf[MAX_READS];

static __thread uint8_t *scan_buf;
static __thread void *rocks_iter;
static __thread rbtree *sorted_buf_tree;
static __thread slice key_iter;
static __thread slice value_iter;
static __thread uint8_t where_key;

typedef struct pagelock {
  int fd;
  off_t page;
  int count; //reference counts
  struct pagelock *next;
  pthread_mutex_t lock;
} pagelock_t;

typedef struct segment_lock {
  pthread_spinlock_t lock;
  pagelock_t *pagelocks;
} segment_lock_t;
segment_lock_t * segments_locks;

void segments_locks_init(void) {
   segments_locks = (segment_lock_t *) malloc(NUM_SEGMENTS * sizeof(segment_lock_t));
   for(size_t s = 0; s < NUM_SEGMENTS ; s++) {
      pthread_spin_init(&(segments_locks[s].lock), PTHREAD_PROCESS_PRIVATE);
      segments_locks[s].pagelocks = (pagelock_t *)malloc(1 * sizeof(pagelock_t));
      segments_locks[s].pagelocks->fd = -1;
      segments_locks[s].pagelocks->page = -1;
      segments_locks[s].pagelocks->count = 0;
      segments_locks[s].pagelocks->next = NULL;
      pthread_mutex_init(&(segments_locks[s].pagelocks->lock), NULL);
   }
}

pthread_mutex_t * page_lock(int fd, size_t idx) {
  off_t segid= idx % NUM_SEGMENTS;
  pthread_spin_lock(&(segments_locks[segid].lock));

  pagelock_t *current = segments_locks[segid].pagelocks;
  int found = 0;
  while (1) {
    if( (current->page = -1) ||
        ((current->page == idx) && (current->fd == fd)) ) {
      current->fd = fd;
      current->page = idx;
      current->count++;
      found = 1;
      break;
    } else if(current->next == NULL) {
      break;
    } else {
      current = current->next;
    }
  }

  if(!found) {
      current->next = (struct pagelock *) malloc(1 * sizeof(pagelock_t));
      current = current->next;
      current->fd = fd; 
      current->page = idx;
      current->count = 1;
      current->next = NULL;
      pthread_mutex_init(&(current->lock), NULL);
  }
  pthread_spin_unlock(&(segments_locks[segid].lock));

  pthread_mutex_lock(&(current->lock));
  return &(current->lock);
}


void page_unlock(int fd, size_t idx, pthread_mutex_t *lock) {
  off_t segid= idx % NUM_SEGMENTS;
  pthread_mutex_unlock(lock);

  pthread_spin_lock(&segments_locks[segid].lock);
  pagelock_t *prev = NULL;
  pagelock_t * current = segments_locks[segid].pagelocks;
  while (current != NULL) {
     if((current->page == idx) && (current->fd == fd)) {
        current->count--;
        if(current->count == 0) {
          if(prev == NULL) {
            current->fd = -1;
            current->page = -1;
          } else {
            prev->next = current->next;
          }
        }
        break;
     } else {
       prev =  current;
       current = current->next;
     }
  }
  pthread_spin_unlock(&segments_locks[segid].lock);
}

char *read_page_sync(int fd, size_t idx) {
  //pthread_mutex_t *pagelock = page_lock(fd, idx);
  char* disk_page = (char *) safe_pread(fd, idx*page_size);
  //page_unlock(fd, idx, pagelock);
  return disk_page;
}

void write_page_sync(int fd, size_t idx, char * disk_page) {
  //pthread_mutex_t *pagelock = page_lock(fd, idx);
  safe_pwrite(fd, idx * page_size, disk_page);
  //page_unlock(fd, idx, pagelock);
}

int get_nb_workers(void) {
   return nb_workers;
}

int get_nb_disks(void) {
   return nb_disks;
}

typedef struct buf_elem_s {
    uint32_t kv_num;
    uint64_t file_idx;
    uint8_t *buf;
} buf_elem_t;

/*
 * Worker context - Each worker thread has one of these structure
 */
struct slab_context {
    size_t worker_id __attribute__((aligned(64)));        // ID
    struct slab **slabs;                                  // Files managed by this worker
    struct slab_callback **callbacks;                     // Callbacks associated with the requests
    volatile size_t buffered_callbacks_idx;               // Number of requests enqueued or in the process of being enqueued
    volatile size_t sent_callbacks;                       // Number of requests fully enqueued
    volatile size_t processed_callbacks;                  // Number of requests fully submitted and processed on disk
    size_t max_pending_callbacks;                         // Maximum number of enqueued requests
    struct pagecache *pagecache __attribute__((aligned(64)));
    struct io_context *io_ctx;
    uint64_t rdt;                                         // Latest timestamp
    size_t nb_slabs;
    pthread_mutex_t wallock;
    uint64_t walsize; //active wal_id;
    uint64_t syncedwalsize; //active wal_id; 
    uint64_t walid; // 
    FILE *walfile; //active WAL
    char path[MAX_PATH_LEN];
    std::vector<FILE *> walfiles; //sealed WAL
    volatile int idle; 
    volatile int rehashing;
    volatile int rehash_idx;
    rbtree *wal_tree;
    void ***wal_hash;
    size_t buckets;

    uint8_t *buffer;
    size_t buffer_size;
    pthread_mutex_t wal_tree_lock;
    volatile size_t queue_full; 
    volatile size_t enqueue_busy; 

    pthread_mutex_t slab_lock;

    pthread_mutex_t worker_lock;
    pthread_cond_t worker_ready;

    volatile uint32_t wal_to_flush_id;
    volatile uint32_t wal_cur_id;
    uint32_t wal_to_flush_num;
    uint32_t sleep_us;

    uint32_t busy_read_slot;
    uint32_t busy_write_slot;
    uint8_t *io_slot_map;
    char *io_page_buffer;

    uint32_t rehash_call;
    uint64_t total_written_kv;

    pthread_mutex_t buf_lock;
    buf_elem_t buf_elem[KV_BUF_NUM];
    uint8_t buf_in_use;
    uint8_t buf_in_flush;
    uint8_t buf_need_flush;

    uint64_t kv_pending;
} *slab_contexts;

struct rehash_worker {
    int worker_id __attribute__((aligned(64)));
    struct slab_callback *callbacks;
    struct io_context *io_ctx;
    char path[MAX_FILE_NAME_LEN];
   
    uint8_t *buffer;
    uint32_t buffer_size;
    uint8_t *io_buffer;
    int queue_full; 
    uint32_t batch_size;
    uint8_t *old_batch;
    uint8_t *new_1_batch;
    uint8_t *new_2_batch;

    uint8_t *page_bitmap;

    uint32_t busy_read_slot;
    uint32_t busy_write_slot;
    uint8_t *io_slot_map;
    uint8_t *io_page_buffer;

    uint32_t rehash_num;
    int rehash_idx;
    pthread_mutex_t rehash_lock;
    pthread_cond_t rehash_ready;

    rehash_elem_t *head;
} *rehash_contexts;

typedef struct del_wal_elem_s {
    char path[MAX_FILE_NAME_LEN];
    struct slab_context *ctx;
    del_wal_elem_s *next;
} del_wal_elem_t;

typedef struct del_wal_context_s {
    uint32_t worker_id;
    uint32_t del_wal_file_cnt;
    del_wal_elem_t *head;
    pthread_mutex_t lock;
    pthread_cond_t ready;
} del_wal_context_t;

del_wal_context_t *del_wal_contexts;

typedef struct {
    int id;
    uint8_t **buf;
    int *buf_len_used;
    int buf_in_use;
    int buf_used;

    pthread_mutex_t lock;
} rocks_buf_group_elem_t;

rocks_buf_group_elem_t *rocks_buf_groups;

typedef struct rocks_node_s {
    int group_id;
    int buf_id;
    struct rocks_node_s *prev;
    struct rocks_node_s *next;
} rocks_node_t;

typedef struct rocks_context_s {
    int worker_id;

    rocks_node_t *head;
    int task_num;
    pthread_mutex_t lock;
    pthread_cond_t ready;
} rocks_context_t;

rocks_context_t *rocks_ctx;

struct pagecache *get_pagecache(struct slab_context *ctx) {
   return ctx->pagecache;
}

struct io_context *get_io_context(struct slab_context *ctx) {
    return ctx->io_ctx;
}

uint64_t get_rdt(struct slab_context *ctx) {
   return ctx->rdt;
}

void set_rdt(struct slab_context *ctx, uint64_t val) {
   ctx->rdt = val;
}

struct slab* get_rehash_slab(struct slab_context *ctx) {
    if (!ctx->rehashing) {
        return NULL;
    }
    return ctx->slabs[ctx->rehash_idx];
}

uint64_t get_file_size(char *file_name) {
    uint64_t size = 0;
    FILE *fp;
    
    fp = fopen(file_name, "rb");
    if (fp == NULL) {
        perr("Cannot open file %s\n", file_name);
        return 0;
    }

    fseek(fp, 0L, SEEK_END);
    size = ftell(fp);

    fclose(fp);

    return size;
}

/* Get next available slot in a workers's context */
static size_t get_slab_buffer(struct slab_context *ctx) {
   size_t next_buffer = __sync_fetch_and_add(&ctx->buffered_callbacks_idx, 1);
   volatile size_t pending = next_buffer - ctx->processed_callbacks;
   if(pending >= ctx->max_pending_callbacks) {
        __sync_fetch_and_sub(&ctx->buffered_callbacks_idx, 1);
        return INVALID_SLOT;
   }
   
   return next_buffer % ctx->max_pending_callbacks;
}

/* Once we get a slot, we fill it, and then submit it */
static size_t submit_slab_buffer(struct slab_context *ctx, int buffer_idx) {
   size_t cur_sent = __sync_fetch_and_add(&ctx->sent_callbacks, 1);

   return cur_sent - 1;
}

static uint64_t get_hash_for_item(char *item) {
    struct item_metadata *meta = (struct item_metadata *)item;
    char *item_key = &item[sizeof(*meta)];
    XXH64_hash_t hash;
 
    hash = XXH64(item_key, meta->key_size, 0);
    if (hash == 0) {
        printf("hash == 0\n");
    }

    return hash;
}

/* Requests are statically attributed to workers using this function */
static struct slab_context *get_slab_context(char *item) {
   uint64_t hash = get_hash_for_item(item);
   return &slab_contexts[(hash % (nb_slabs * nb_pages_per_slab)) / (nb_pages_per_slab * nb_slabs_per_worker)];
}

size_t get_item_size(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   return sizeof(*meta) + meta->key_size + meta->value_size;
}

static struct slab *get_slab(struct slab_context *ctx, char *item, struct slab_callback *callback) {
   return NULL;
}

struct slab *get_slab_with_abs_index(uint64_t hash) {
    uint32_t idx;
    idx = (hash / nb_pages_per_slab) % nb_slabs;
    return slabs[idx];
}

struct slab *get_slab_with_index(struct slab_context *ctx, uint64_t hash, int *slab_index) {
    *slab_index = (hash / nb_pages_per_slab) % nb_slabs;
    return slabs[*slab_index];
}

uint32_t get_slab_page_idx(struct slab_callback *callback) {
    uint32_t idx;
    uint8_t inc = callback->migration ? 1 : 0; 
    struct slab *s = callback->slab;

    idx = callback->hash % (nb_pages_per_slab << (s->rehash_radix + inc));
    return idx;
}

uint32_t get_slab_page_idx_with_radix(uint64_t hash, uint32_t radix) {
    uint32_t idx;
    idx = hash % (nb_pages_per_slab << radix);
    return idx;
}

static void enqueue_slab_callback(struct slab_context *ctx, enum slab_action action, struct slab_callback *callback) {
#if 0
   size_t buffer_idx;

loop:
   pthread_mutex_lock(&(ctx->slab_lock));
   buffer_idx = get_slab_buffer(ctx);
   if (buffer_idx == INVALID_SLOT) {
        pthread_mutex_unlock(&(ctx->slab_lock));
        usleep(2);
        goto loop;
   }
   callback->action = action;
   ctx->callbacks[buffer_idx] = callback;
   //add_time_in_payload(callback, 0);
   submit_slab_buffer(ctx, buffer_idx);

   pthread_mutex_unlock(&(ctx->slab_lock));

   //printf("enqueue slab %d, id %d\n", buffer_idx, callback->id);
   add_time_in_payload(callback, 1);

   if(disable_wal) {
     if((action == ADD) || (action == UPDATE)) { //xubin
       //usleep(2);
       if(callback->cb) { //compute_stats
         callback->cb(callback, NULL);
       }
     }
   }
#endif
}

static int schedule_for_rehash(struct slab_context *ctx, uint8_t cond_sig);

void remove_from_wal_index(struct slab_context *ctx, struct item_metadata *meta) {
#ifdef USE_WAL_TREE
    slice key;
    key.len = meta->key_size + 8;
    key.data = (char *)malloc(key.len);
    memcpy(key.data, (char *)(meta + 1), meta->key_size);
    memcpy(key.data + meta->key_size, (char *)(&meta->seq_id), 8);


    // printf("Will remove key = %ld, seq id = %ld\n", *((uint64_t *)key.data), meta->seq_id);

    // key.data = (char *)(meta + 1);
    pthread_mutex_lock(&(ctx->wal_tree_lock));
    removeRBNode(ctx->wal_tree, key);
    pthread_mutex_unlock(&(ctx->wal_tree_lock));
    free(key.data);
#else
    int left = 0;
    uint64_t hash = get_hash_for_item((char *)meta);
    size_t idx = hash % ctx->buckets;
    uint64_t i, count;
    struct item_metadata *dest;

    pthread_mutex_lock(&(ctx->wal_tree_lock));
    if (ctx->wal_hash[idx] != NULL) {
        count = *(uint64_t *)ctx->wal_hash[idx];
        for (i = 1; i <= count; i++) {
            if (ctx->wal_hash[idx][i]) {
                left++;
                dest = (struct item_metadata *)ctx->wal_hash[idx][i];
                if (dest->key_size == meta->key_size && !memcmp(dest + 1, meta + 1, dest->key_size)) {
                    free(ctx->wal_hash[idx][i]);
                    ctx->wal_hash[idx][i] = NULL;
                    left--;
                }
            }
        }

        if (left == 0) {
            free(ctx->wal_hash[idx]);
            ctx->wal_hash[idx] = NULL;
        }
    }
    pthread_mutex_unlock(&(ctx->wal_tree_lock));
#endif
}

#ifdef USE_WAL_TREE
void remove_from_except_wal_index(struct slab *s, struct item_metadata *meta) {
    slice key;
    key.len = meta->key_size + 8;
    key.data = (char *)malloc(key.len);
    memcpy(key.data, (char *)(meta + 1), meta->key_size);
    memcpy(key.data + meta->key_size, (char *)(&meta->seq_id), 8);

    pthread_mutex_lock(&(s->except_wal_index_lock));
    removeRBNode(s->except_wal, key);
    pthread_mutex_unlock(&(s->except_wal_index_lock));

    free(key.data);
}
#else
void remove_from_except_wal_index(struct slab *s, struct item_metadata *meta) {
    // TODO:
    return;
}
#endif


void insert_into_except_index(struct slab *s, char *item, uint64_t off, uint32_t index) {
    slice key, value;
    struct item_metadata *meta = (struct item_metadata*)item;

    key.len = meta->key_size + 8;
    key.data = (char *)malloc(key.len);
    memcpy(key.data, (char *)(meta + 1), meta->key_size);
    memcpy(key.data + meta->key_size, (uint8_t *)(&meta->seq_id), 8);

    value.len = sizeof(uint64_t) * 3 + sizeof(uint32_t);
    if (!value.len) {
        die("Memory failed!\n");
    }

    value.data = (char *)malloc(value.len);
    memset(value.data, 0, value.len);
    if (!value.data) {
        die("Memory failed!\n");
    }

    *((uint64_t *)value.data) = off;
    *((uint64_t *)value.data + 1) = off + sizeof(*meta) + meta->key_size;
    *((uint64_t *)value.data + 2) = meta->value_size;
    *((uint32_t *)((char *)value.data + sizeof(uint64_t) * 3)) = index;
#ifdef KEY_TRACE
    printf("key = %ld will be inserted exc index; exc idx = %d!\n", *(uint64_t *)(meta + 1), index);
#endif

    pthread_mutex_lock(&(s->except_wal_index_lock));
    insertRBNode(s->except_wal, key, value);
    pthread_mutex_unlock(&(s->except_wal_index_lock));


    // printf("insert into exc index key = %ld and seq = %ld\n", *((uint64_t *)(key.data)), meta->seq_id);
}

void reload_except_wal(struct slab *s)
{
#if 0
    int len;
    char item[ITEM_SIZE];
    fseek(s->except_pf, 0, SEEK_SET);
    while (1) {
        len = fread(item, 1, ITEM_SIZE, s->except_pf);
        if (len != ITEM_SIZE) {
            break;
        }
        struct item_metadata *meta = (struct item_metadata *)item;
        if (meta->key_size != KEY_SIZE || meta->value_size != VALUE_SIZE) {
        	printf("wrong metadata\n");
        	continue;
        }
        insert_into_except_wal(NULL, s, item);
        s->except_offset += ITEM_SIZE;
        s->pending++;
    }

    s->except_wal_size = s->except_offset;
#endif
}

char *kv_read_item_sync(char *item) {
#if 0
   struct slab_context *ctx = get_slab_context(item, NULL);
   struct slab *s = get_slab(ctx, item, NULL);
   return read_item(s, item, get_slab_idx(ctx, item, NULL));
#endif
   return NULL;
}

#if 1
void iterator_init() {
    uint32_t i, j, len_used;
    uint64_t off, total_len;
    slice key, value;
    struct item_metadata *meta;
    uint8_t *ptr = NULL;
    rocks_buf_group_elem_t *buf_group;
    int nb_groups = (nb_workers + WORKERS_PER_ROCKS_BUF_GROUP - 1) / WORKERS_PER_ROCKS_BUF_GROUP;

    rocks_iter = rocks_iterator_init();
    assert(rocks_iter != NULL);
    total_len = ROCKS_BUF_LEN * ROCKS_BUF_NUM_PER_GROUP * nb_groups;
    if (scan_buf == NULL) {
        scan_buf = (uint8_t *)valloc(total_len);
    }

    // Copy buffered keys that have not written into rocksdb to this scan buffer
    off = 0;
    for (i = 0; i < nb_groups; i++) {
        buf_group = &(rocks_buf_groups[i]);
        pthread_mutex_lock(&buf_group->lock);
        for (j = 0; j < ROCKS_BUF_NUM_PER_GROUP; j++) {
            len_used = buf_group->buf_len_used[j];
            if (len_used > 0) {
                memcpy(scan_buf + off, buf_group->buf[j], len_used);
                off += len_used;
            }
        }
        pthread_mutex_unlock(&buf_group->lock);
    }
    if (off + sizeof(*meta) < total_len) {
        memset(scan_buf + off, 0, sizeof(*meta));
    } else {
        memset(scan_buf + off, 0, total_len - off);
    }

    // Sort for key in scan buffer
    if (sorted_buf_tree == NULL) {
        sorted_buf_tree = createRBtree(normalCompare);
    }

    off = 0;
    ptr = scan_buf;
    value.len = 0;
    value.data = NULL;
    while (ptr + sizeof(*meta) < scan_buf + total_len) {
        meta = (struct item_metadata *)ptr;
        if (meta->key_size == 0) {
            break;
        }
        key.data = (char *)malloc(meta->key_size + 8);
        key.len = meta->key_size + 8;
        memcpy(key.data, (char *)(meta + 1), meta->key_size);
        memcpy(key.data + meta->key_size, (uint8_t *)(&meta->seq_id), 8);
   
        insertRBNode(sorted_buf_tree, key, value);
        if (meta->value_size == 0) {
            removeRBNode(sorted_buf_tree, key);
        }
        ptr += (sizeof(*meta) + meta->key_size);
    }
    where_key = NONE_KEY;
}

// Need to double check the precise definition of Seek().
void Seek_internal(char *buf, uint32_t size) {
    int ret;
    uint32_t size_tmp;
    rocks_Seek(rocks_iter, buf, size);
    lookupMinRBNode(sorted_buf_tree, &key_iter, &value_iter);
    //assert(size == key_iter.len - 8);
    
    while (key_iter.data != NULL && memcmp(key_iter.data, buf, key_iter.len - 8) < 0) {
        if (key_iter.data) {
            free(key_iter.data);
            key_iter.data = NULL;
        }
        // Value data should always be NULL for now
        if (value_iter.data) {
            free(value_iter.data);
            value_iter.data = NULL;
        }
        lookupMinRBNode(sorted_buf_tree, &key_iter, &value_iter);
    }

    if (rocks_Valid(rocks_iter) && key_iter.data != NULL) {
        // Find the smaller one
        ret = memcmp(key_iter.data, rocks_GetKey(rocks_iter, &size_tmp), key_iter.len - 8);
        if (ret < 0) {
            where_key = BUF_KEY;
        } else if (ret == 0) {
            where_key = BUF_KEY;
            rocks_Next(rocks_iter);
        } else {
            where_key = ROCKS_KEY;
        }
    } else if (rocks_Valid(rocks_iter) && key_iter.data == NULL) {
        where_key = ROCKS_KEY;
    } else if (!rocks_Valid(rocks_iter) && key_iter.data != NULL) {
        where_key = BUF_KEY;
    } else {
        where_key = NONE_KEY;
    }
}

void SeekToFirst() {
    int ret;
    uint32_t size_tmp;
    rocks_SeekToFirst(rocks_iter);
    lookupMinRBNode(sorted_buf_tree, &key_iter, &value_iter);

    if (rocks_Valid(rocks_iter) && key_iter.data != NULL) {
        // Find the smaller one
        ret = memcmp(key_iter.data, rocks_GetKey(rocks_iter, &size_tmp), key_iter.len-8);

        if (ret < 0) {
            where_key = BUF_KEY;
        } else if (ret == 0) {
            where_key = BUF_KEY;
            rocks_Next(rocks_iter);
        } else {
            where_key = ROCKS_KEY;
        }
    } else if (rocks_Valid(rocks_iter) && key_iter.data == NULL) {
        where_key = ROCKS_KEY;
    } else if (!rocks_Valid(rocks_iter) && key_iter.data != NULL) {
        where_key = BUF_KEY;
    } else {
        where_key = NONE_KEY;
    }
}

int Valid() {
    if (where_key == NONE_KEY) {
        return 0;
    } else if (where_key == BUF_KEY) {
        return key_iter.data != NULL;
    } else if (where_key == ROCKS_KEY) {
        return rocks_Valid(rocks_iter);
    } else {
        die("Shoule not be here!\n");
    }
}

void GetKey(char *buf, uint32_t *size) {
    char *ptr;
    uint32_t size_tmp;
    if (where_key == BUF_KEY) {
        memcpy(buf, key_iter.data, key_iter.len);
        *size = key_iter.len - 8;
    } else if (where_key == ROCKS_KEY) {
        ptr = rocks_GetKey(rocks_iter, &size_tmp);
        memcpy(buf, ptr, size_tmp);
        *size = size_tmp;
    } else {
        die("Should not be here!");
    }
}

void Next() {
    int ret;
    uint32_t size_tmp;

    if (where_key == BUF_KEY) {
        if (key_iter.data) {
            free(key_iter.data);
            key_iter.data = NULL;
        }
        // Value data should always be NULL for now
        if (value_iter.data) {
            free(value_iter.data);
            value_iter.data = NULL;
        }
        lookupMinRBNode(sorted_buf_tree, &key_iter, &value_iter);
        if (key_iter.data == NULL) {
            if (rocks_Valid(rocks_iter)) {
                // rocks_Next(rocks_iter);
                where_key = ROCKS_KEY;
            } else {
                where_key = NONE_KEY;
            }
        } else {
            if (rocks_Valid(rocks_iter)) {
                // printf("key_iter.len = %d, key_size = %d\n", key_iter.len, key_size);
                // assert(key_iter.len - 8 == key_size);
                ret = memcmp(key_iter.data, rocks_GetKey(rocks_iter, &size_tmp), key_iter.len - 8);
                if (ret < 0) {
                    where_key = BUF_KEY;
                } else if (ret == 0) {
                    //die("Should not be here!\n");
                    rocks_Next(rocks_iter);
                    where_key = BUF_KEY; 
                } else {
                    where_key = ROCKS_KEY;
                }
            } else {
                where_key = BUF_KEY;
            }
        } 
    } else if (where_key == ROCKS_KEY) {
        if (rocks_Valid(rocks_iter)) {
            rocks_Next(rocks_iter);
            if (rocks_Valid(rocks_iter) && key_iter.data != NULL) {
                assert(key_iter.len - 8 == key_size);
                ret = memcmp(key_iter.data, rocks_GetKey(rocks_iter, &size_tmp), key_iter.len - 8);
                if (ret < 0) {
                    where_key = BUF_KEY;
                } else if (ret == 0) {
                    rocks_Next(rocks_iter);
                    where_key = BUF_KEY;
                } else {
                    where_key = ROCKS_KEY;
                }
            } else if (rocks_Valid(rocks_iter) && key_iter.data == NULL) {
                where_key = ROCKS_KEY;
            } else if (!rocks_Valid(rocks_iter) && key_iter.data != NULL) {
                where_key = BUF_KEY;
            } else if (!rocks_Valid(rocks_iter) && key_iter.data == NULL) {
                where_key = NONE_KEY;
            } else {
                where_key = NONE_KEY;
            }
        } else {
            lookupMinRBNode(sorted_buf_tree, &key_iter, &value_iter);
            if (key_iter.data != NULL) {
                where_key = BUF_KEY;
            } else {
                where_key = NONE_KEY;
            }
        }
    } else {
        die("Should not be here!\n");
    }
}

void iterator_del() {
    rocks_iterator_del(rocks_iter);
    rocks_iter = NULL;
}

// Wrap internal functions for user APIs
void Seek(slice *key) {
/*
    item = (char *)malloc(sizeof(struct item_metadata) + key->len);
    meta = (struct item_metadata *)item;

    meta->seq_id = 0;
    meta->key_size = key->len;
    meta->value_size = 0;
    memcpy(item + sizeof(*meta), key->data, key->len);

    printf("Key data in seek = %ld\n", *(long *)key->data);

    Seek_internal(item, key->len);
*/

    Seek_internal(key->data, key->len);
}
 
#endif

// Notice the switch of slabs when doing rehash
slice *kv_read_sync(struct slab_callback *callback) {
    add_time_in_payload(callback, 0);
    struct slab_context *ctx = get_slab_context(callback->item);
    size_t idx;
    char *disk_page = NULL;
    char *data = NULL;
    struct slab *s;
    uint64_t hash;
    int fd;
    uint32_t rehash_radix;
    off_t in_page_offset;
    slice *value;
    struct item_metadata *meta = NULL, *meta_tmp = NULL;
    uint64_t key_value = *(uint64_t *)((char *)callback->item + sizeof(struct item_metadata));

    meta = (struct item_metadata *)callback->item;

    value = (slice *)malloc(sizeof(slice));
    value->len = INVALID_LEN;
    value->data = NULL;

    callback->ctx = ctx;
#ifdef USE_WAL_TREE 
    read_from_wal_tree(ctx, callback->item, value);
#else
#endif
   
    callback->migration = 0;

    if (value->len != 0 && value->data == NULL) {
        //printf("not found in wal tree for key = %ld\n", key_value);
        hash = get_hash_for_item(callback->item);
        s = get_slab_with_abs_index(hash);
        callback->hash = hash;
        callback->slab = s;

wait_switch:
        pthread_spin_lock(&(s->meta_lock));
        if (s->slab_switch == 1) {
            pthread_spin_unlock(&(s->meta_lock));
            usleep(1);
            printf("Will retry...\n");
            goto wait_switch;
        }
        fd = s->fd_old;
        ++s->old_ref;
        rehash_radix = s->rehash_radix;
        pthread_spin_unlock(&(s->meta_lock));

        // idx = get_new_slab_idx(ctx, s, callback->item, callback);
        idx = get_slab_page_idx_with_radix(hash, rehash_radix);

        disk_page = read_page_sync(fd, idx);
        in_page_offset = read_item_in_page(callback->item, disk_page);
        if (in_page_offset != -1) {
            meta_tmp = (struct item_metadata *)(&disk_page[in_page_offset]);
            value->len = meta_tmp->value_size;
            if (value->len > 0) {
                value->data = (char *)malloc(value->len);
                memcpy(value->data, (char *)(meta_tmp+1) + meta_tmp->key_size, value->len);
            } else {
                value->data = NULL;
            }
        }
       
        if (value->len != 0 && value->data == NULL) {
            read_from_except_wal(s, callback->item, value);
        }

        if (value->len != 0 && value->data == NULL) {
            ++not_found_item;
            if (not_found_item) {
                printf("Not found item num = %ld, kv = %ld, idx = %ld, radix = %d\n", not_found_item.load(), key_value, idx, rehash_radix);
            }
        }

        pthread_spin_lock(&(s->meta_lock));
        --s->old_ref;
        pthread_spin_unlock(&(s->meta_lock));
    }

    if (callback->cb) {
        callback->cb(callback, data);
    }
/*
    if (!disk_page && data) {
        free(data);
    }
*/
    return value;
}

// Wrap kv_read_sync to form kv_get; could be further optimized
slice *kv_get(slice *key) {
    slice *value;
    uint64_t item_size = sizeof(struct item_metadata) + key->len;
    char *item = (char *)calloc(1, item_size);
    struct item_metadata *meta = (struct item_metadata *)item;
    struct slab_callback *cb = (struct slab_callback *)malloc(sizeof(*cb));

    meta->seq_id = 0;
    meta->key_size = key->len;
    meta->value_size = 0;
    memcpy(item + sizeof(*meta), key->data, key->len);

    cb->cb = NULL;
    cb->item = item;
    
    value = kv_read_sync(cb);

    free(cb->item);
    free(cb);

    return value;
}

// Similar to Multi-Get
slice **kv_multi_read(struct slab_callback **callback, uint32_t number) {
    // add_time_in_payload(callback, 0);
    // struct slab_context *ctx = get_slab_context(callback->item);
    struct slab_context *ctx[MAX_READS];
    size_t idx[MAX_READS];
    char *disk_page;
    // char *data[MAX_READS];
    struct slab *s;
    uint64_t hash;
    int fd;
    uint32_t rehash_radix;
    off_t in_page_offset;
    slice **value;
    struct item_metadata *meta, *meta_tmp;
    uint8_t found[MAX_READS];
    uint32_t pages_read = 0;
    int i, ret;

    assert(number <= MAX_READS);

    value = (slice **)malloc(sizeof(slice *) * MAX_READS);
    if (!read_io_ctx) {
        read_io_ctx = worker_ioengine_init(MAX_READS);
        for (i = 0; i < MAX_READS; i++) {
            read_page_buf[i] = (char *)valloc(page_size);
        }
    }

    for (i = 0; i < number; i++) {
        found[i] = 0;
        ctx[i] = get_slab_context(callback[i]->item);
        callback[i]->ctx = ctx[i];
        callback[i]->migration = 0;
        value[i] = (slice *)malloc(sizeof(slice));
        value[i]->len = INVALID_LEN;
        value[i]->data = NULL;

#ifdef USE_WAL_TREE 
        read_from_wal_tree(ctx[i], callback[i]->item, value[i]);
#else
#endif

        if (value[i]->len != 0 && value[i]->data == NULL) {
            hash = get_hash_for_item(callback[i]->item);
            s = get_slab_with_abs_index(hash);
            callback[i]->hash = hash;
            callback[i]->slab = s;
            
wait_switch:
            pthread_spin_lock(&(s->meta_lock));
            if (s->slab_switch == 1) {
                pthread_spin_unlock(&(s->meta_lock));
                usleep(1);
                goto wait_switch;
            }
            fd = s->fd_old;
            ++s->old_ref;
            rehash_radix = s->rehash_radix;
            pthread_spin_unlock(&(s->meta_lock));

            idx[i] = get_slab_page_idx_with_radix(hash, rehash_radix);
            callback[i]->fd = fd;
            callback[i]->page_idx = idx[i];
            callback[i]->slot_idx = i;
            // Read pages in async mode
            callback[i]->io_ctx = read_io_ctx;
            callback[i]->page_buffer = read_page_buf[i];

            read_page_async(callback[i]);
            ++pages_read;
        } else {
            found[i] = 1;
        }

    }

    // Wait until all pages have been read
    ret = io_getevents_wrapper(read_io_ctx, pages_read, number);
    assert(ret == pages_read);

    for (i = 0; i < number; i++) {
        if (found[i]) {
            continue;
        }

        disk_page = read_page_buf[i];
        in_page_offset = read_item_in_page(callback[i]->item, disk_page);
        if (in_page_offset != -1) {
            meta_tmp = (struct item_metadata *)(&disk_page[in_page_offset]);
            value[i]->len = meta_tmp->value_size;
            if (value[i]->len > 0) {
                value[i]->data = (char *)malloc(value[i]->len);
                memcpy(value[i]->data, (char *)(meta_tmp+1) + meta_tmp->key_size, value[i]->len);
            } else {
                value[i]->data = NULL;
            }
        }

        if (value[i]->len != 0 && value[i]->data == NULL) {
            read_from_except_wal(callback[i]->slab, callback[i]->item, value[i]);
        }

        if (value[i]->len != 0 && value[i]->data == NULL) {
            ++not_found_item;
            if (not_found_item) {
                printf("Not found item num = %ld\n", not_found_item.load());
            }
        }

        pthread_spin_lock(&(s->meta_lock));
        --callback[i]->slab->old_ref;
        pthread_spin_unlock(&(s->meta_lock));

        if (callback[i]->cb) {
            callback[i]->cb(callback[i], NULL);
        }
    }

    return value;
}

void kv_add_sync(struct slab_callback *callback) {
#if 0
   struct slab_context *ctx = get_slab_context(callback->item, callback);
   callback->ctx = ctx;
   __sync_fetch_and_add(&ctx->rdt, 1);
   struct slab *s = get_slab(ctx, callback->item, callback);
   size_t idx = get_slab_idx(ctx, callback->item, callback);
   char *disk_page = read_page_sync(s->fd, idx);
   off_t free_in_page_offset = free_item_in_page_offset(callback->item, disk_page);
   if(free_in_page_offset == -1) {
     //add to SQLite
   } else {
     update_item(callback, disk_page, free_in_page_offset);
     write_page_sync(s->fd, idx, disk_page);
     //sync_file_range(s->fd, idx*page_size, (idx+1)*page_size, SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER);  
     if(callback->cb) {
       callback->cb(callback, &disk_page[free_in_page_offset]);
     }
   }
#endif
}

#if 0
void add_item_sync(struct slab_callback *callback) {
   //struct slab_context *ctx = get_slab_context(callback->item, callback);
   //callback->ctx = ctx;
   struct slab_context *ctx = callback->ctx;
   __sync_fetch_and_add(&ctx->rdt, 1);
   struct slab *s = get_slab(ctx, callback->item, callback);
   size_t idx = get_slab_idx(ctx, callback->item, callback);
   char *disk_page = read_page_sync(s->fd, idx);
   off_t free_in_page_offset = free_item_in_page_offset(callback->item, disk_page);
   if(free_in_page_offset == -1) {
     //add to SQLite
   } else {
     update_item(callback, disk_page, free_in_page_offset);
     write_page_sync(s->fd, idx, disk_page);
     //sync_file_range(s->fd, idx*page_size, (idx+1)*page_size, SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER);  
     if(callback->cb) {
       callback->cb(callback, &disk_page[free_in_page_offset]);
     }
   }
}
#endif

//void kv_add_sync(struct slab_callback *callback) {
//   struct slab_context *ctx = get_slab_context(callback->item, callback);
//   return enqueue_slab_callback(ctx, ADD, callback);
//}

 
/*
void kv_update_sync(struct slab_callback *callback) {
   struct slab_context *ctx = get_slab_context(callback->item);
   callback->ctx = ctx;
   __sync_fetch_and_add(&ctx->rdt, 1);
   struct slab *s = get_slab(ctx, callback->item);
   size_t idx = get_slab_idx(ctx, callback->item);
   char *disk_page = read_page_sync(s->fd, idx);
   off_t in_page_offset = item_in_page_offset(callback->item, disk_page);
   if(in_page_offset == -1) {
     //add to SQLite
   } else {
     update_item(callback, disk_page, in_page_offset);
     write_page_sync(s->fd, idx, disk_page);
     //sync_file_range(s->fd, idx*page_size, (idx+1)*page_size, SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER);
     if(callback->cb) {
       callback->cb(callback, &disk_page[in_page_offset]);
     }
   }
}
*/

void update_item_sync(struct slab_callback *callback) {
#if 0
   //struct slab_context *ctx = get_slab_context(callback->item);
   //callback->ctx = ctx;
   struct slab_context *ctx = callback->ctx;
   __sync_fetch_and_add(&ctx->rdt, 1);
   struct slab *s = get_slab(ctx, callback->item, callback);
   size_t idx = get_new_slab_idx(ctx, s, callback->item, callback);
   char *disk_page = read_page_sync(s->fd, idx);
   off_t in_page_offset = item_in_page_offset(callback->item, disk_page);
   if(in_page_offset == -1) {
     //add to SQLite
   } else {
     update_item(callback, disk_page, in_page_offset);
     write_page_sync(s->fd, idx, disk_page);
     //sync_file_range(s->fd, idx*page_size, (idx+1)*page_size, SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER);
     if(callback->cb) {
       callback->cb(callback, &disk_page[in_page_offset]);
     }
   }
#endif
}

void kv_update_sync(struct slab_callback *callback) {
   add_time_in_payload(callback, 0);
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, UPDATE, callback);
}

void kv_read_async(struct slab_callback *callback) {
   add_time_in_payload(callback, 0);
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, READ, callback);
}

void kv_read_async_no_lookup(struct slab_callback *callback, struct slab *s, size_t slab_idx) {
   callback->slab = s;
   callback->slab_idx = slab_idx;
   struct slab_context *ctx = get_slab_context(callback->item);
   return enqueue_slab_callback(ctx, READ_NO_LOOKUP, callback);
}

// TODO: Do not need to free and reallocate space
#ifdef USE_WAL_TREE
int insert_into_wal_index(struct slab_context *ctx, char *item) {
    struct item_metadata *meta = (struct item_metadata *)item;
    slice key, value;

    key.data = (char *)malloc(meta->key_size + 8);
    key.len = meta->key_size + 8;
    assert(key.data != NULL && meta != NULL);
    memcpy(key.data, (char *)(meta + 1), meta->key_size);
    memcpy(key.data + meta->key_size, (uint8_t *)(&meta->seq_id), 8);

    value.len = meta->value_size;
    if (value.len != 0) {
        value.data = (char *)malloc(value.len);
        memcpy(value.data, (char *)item + sizeof(*meta) + meta->key_size, value.len);
    } else {
        value.data = NULL;
    }

    pthread_mutex_lock(&(ctx->wal_tree_lock));
    insertRBNode(ctx->wal_tree, key, value);
    pthread_mutex_unlock(&(ctx->wal_tree_lock));

    return 0;
}
#else
int insert_into_wal_index(struct slab_context *ctx, char *item) {
    int found = 0;
    uint64_t count = 0;
    uint64_t hash = get_hash_for_item(item);
    size_t idx = hash % ctx->buckets;
    struct item_metadata *meta = (struct item_metadata *)item;
    char *data = (char *)calloc(sizeof(struct item_metadata) + meta->key_size + meta->value_size, 1);
    memcpy(data, item, sizeof(struct item_metadata) + meta->key_size + meta->value_size);

    if (ctx->wal_hash[idx] == NULL) {
        ctx->wal_hash[idx] = (void **)calloc(sizeof(void *), page_size / ITEM_SIZE + 1);
        *(uint64_t *)ctx->wal_hash[idx] = page_size / ITEM_SIZE; //store capacity in the first 8 bytes
    }
    count = *(uint64_t*)(&ctx->wal_hash[idx][0]);
    for (uint64_t i = 1; i <= count; i++) {
        if (ctx->wal_hash[idx][i] == NULL) {
            ctx->wal_hash[idx][i] = data;
            found = 1;
            break;
        }
    }

    if (!found) {
        ctx->wal_hash[idx] = (void **)realloc((void *)ctx->wal_hash[idx], sizeof(void *) * (2 * count + 1));
        memset(&ctx->wal_hash[idx][count + 1], 0, count * sizeof(void *));
        ctx->wal_hash[idx][count + 1] = data;
        *(uint64_t *)ctx->wal_hash[idx] = 2 * count;
    }

    return 0;
}
#endif

static inline void insert_into_rocks_buf(struct slab_context *ctx, char *item) {
    struct item_metadata *meta = (struct item_metadata *)item;
    int group_id, buf_id, buf_len_used, buf_used, cur_len;
    rocks_buf_group_elem_t *buf_group;
    uint8_t flush_need = 0;
    int rocks_idx = -1;
    rocks_node_t *elem, *elem_tmp;
    rocks_context_t *this_rocks;

    group_id = ctx->worker_id / WORKERS_PER_ROCKS_BUF_GROUP;
    buf_group = &(rocks_buf_groups[group_id]);
    cur_len = sizeof(*meta) + meta->key_size;

recheck_buf:
    pthread_mutex_lock(&buf_group->lock);

    if (buf_group->buf_used >= ROCKS_BUF_NUM_PER_GROUP - 2) {
        pthread_mutex_unlock(&buf_group->lock);
        usleep(1);
        goto recheck_buf;
    }

    buf_id = buf_group->buf_in_use;
    buf_len_used = buf_group->buf_len_used[buf_id];

    if (buf_len_used + cur_len > ROCKS_BUF_LEN) {
        flush_need = 1;
        // printf("buf_len_used = %d, ROCKS_BUF_LEN = %d\n", buf_len_used, ROCKS_BUF_LEN);
        memset(buf_group->buf[buf_id] + buf_len_used, 0, ROCKS_BUF_LEN - buf_len_used);
        ++buf_group->buf_in_use;
        buf_group->buf_in_use %= ROCKS_BUF_NUM_PER_GROUP;
        buf_group->buf_len_used[buf_group->buf_in_use] = 0;
        ++buf_group->buf_used;

        memcpy(buf_group->buf[buf_group->buf_in_use], item, sizeof(*meta) + meta->key_size);
        buf_group->buf_len_used[buf_group->buf_in_use] += (sizeof(*meta) + meta->key_size);
    } else {
        memcpy(buf_group->buf[buf_id] + buf_len_used, item, sizeof(*meta) + meta->key_size);
        buf_group->buf_len_used[buf_id] += (sizeof(*meta) + meta->key_size);
    }
    pthread_mutex_unlock(&buf_group->lock);

    if (flush_need) {
        rocks_idx = ctx->worker_id / ((nb_workers + ROCKS_THREADS - 1) / ROCKS_THREADS);
        elem = (rocks_node_t *)malloc(sizeof(rocks_node_t));
        elem->prev = NULL;
        elem->next = NULL;
        
        this_rocks = &(rocks_ctx[rocks_idx]);
        pthread_mutex_lock(&this_rocks->lock);
        ++this_rocks->task_num;
        elem->group_id = group_id;
        elem->buf_id = buf_id;
        elem->next = this_rocks->head;
        elem_tmp = this_rocks->head->prev;
        this_rocks->head->prev = elem;
        elem_tmp->next = elem;
        elem->prev = elem_tmp;
        pthread_mutex_unlock(&this_rocks->lock);

        pthread_cond_signal(&this_rocks->ready);
    }
}

void kv_add_async(struct slab_callback *callback) {
    add_time_in_payload(callback, 0);
    struct slab_context *ctx = get_slab_context(callback->item);
    uint8_t flush_needed = 0;
    uint8_t rocks_idx = 0;
    
    wal_write(ctx, callback, 0, &flush_needed);

    if (scan) {
#ifdef DIRECT_WRITE_INTO_ROCKSDB
        write_into_rocks(callback->item + sizeof(struct item_metadata),
                         ((struct item_metadata *)(callback->item))->key_size,
                         ((struct item_metadata *)(callback->item))->value_size);
#else
        insert_into_rocks_buf(ctx, callback->item);
#endif
    }

    callback->index_added = 0;

    insert_into_wal_index(ctx, callback->item);

    callback->index_added = 1;

    if (flush_needed) {
        pthread_mutex_lock(&(ctx->worker_lock));
        ++ctx->buf_need_flush;
        pthread_mutex_unlock(&(ctx->worker_lock));
        pthread_cond_signal(&(ctx->worker_ready));
    }

    if (callback->cb) {
        // compute_stats
        callback->cb(callback, NULL);
    }
}

void conflict_kv_add_async(struct slab_context *ctx, struct slab_callback *callback) {
    uint8_t flush_needed = 0;

    wal_write(ctx, callback, 1, &flush_needed);

    if (flush_needed) {
        pthread_mutex_lock(&(ctx->worker_lock));
        ++ctx->buf_need_flush;
        pthread_mutex_unlock(&(ctx->worker_lock));
        pthread_cond_signal(&(ctx->worker_ready));
    }
}

// Delete is treated as PUT with a value length 0 KV
void kv_remove_async(struct slab_callback *callback) {
    kv_add_async(callback);
}

void kv_update_async(struct slab_callback *callback) {
#if 0
   add_time_in_payload(callback, 0);
   struct slab_context *ctx = get_slab_context(callback->item, callback);
   //callback->action = UPDATE;
   if(!disable_wal) {
     wal_write(ctx, callback);
   }
   enqueue_slab_callback(ctx, UPDATE, callback);
   if(!disable_wal) {
     if(callback->cb) { //compute_stats
       callback->cb(callback, NULL);
     }
   }
#endif
}

// TODO: simply all internal structures
// For now we wrap the previous test functions for user APIs.
void kv_put(slice *key, slice *value) {
    int i;
    uint64_t item_size = sizeof(struct item_metadata) + key->len + value->len;
    char *item = (char *)calloc(1, item_size);
    struct item_metadata *meta = (struct item_metadata *)item;
    struct slab_callback *cb = (struct slab_callback *)malloc(sizeof(*cb));

    meta->seq_id = 0;
    meta->key_size = key->len;
    meta->value_size = value->len;
    memcpy(item + sizeof(*meta), key->data, key->len);
    memcpy(item + sizeof(*meta) + key->len, value->data, value->len);

    cb->cb = NULL;
    cb->item = item;
    kv_add_async(cb);
}

void kv_update(slice *key, slice *value) {
    kv_put(key, value);
}

// Treat delete as a KV put with value length = 0.
void kv_delete(slice *key) {
    slice value;

    value.data = NULL;
    value.len = 0;
    kv_put(key, &value);
}

tree_scan_res_t kv_init_scan(char *item, size_t scan_size) {
   //return memory_index_scan(item, scan_size);
   tree_scan_res_t res = {NULL, NULL, 0};
   return res;
}

uint32_t except_wal_open(struct slab *s, uint8_t has_old) {
    uint32_t idx;
    FILE *fp;
    char file_name[MAX_FILE_NAME_LEN];

    pthread_mutex_lock(&s->except_wal_lock);
    s->except_offset = 0;
    s->except_wal_size = 0;
    s->synced_except_wal_size = 0;
    idx = (s->except_wal_index++);

    s->except_wal_item_cnt = 0;
    sprintf(file_name, "%s/%d-%d.except", s->path, s->id, idx);
    fp = fopen(file_name, "a+b");
    if(fp == NULL) {
        die("Failed to open exception wal file %s \n", file_name);
    }
    if (has_old) {
        assert(s->except_fp_old == NULL);
        s->except_fp_old = s->except_fp_new;
    } else {
        s->except_fp_old = NULL;
    }
    s->except_fp_new = fp;
    pthread_mutex_unlock(&s->except_wal_lock);

    if (has_old) {
        fflush(s->except_fp_old);
        if(fdatasync(fileno(s->except_fp_old)) != 0) {
            die("Failed to sync except wal file!\n");
        }
    }

    return idx;
}

void wal_open(struct slab_context *ctx) {
    char filename[MAX_FILE_NAME_LEN];

    sprintf(filename, "%s/%lu-%lu.wal", ctx->path, ctx->worker_id, ctx->walid);
    ctx->walsize = 0;   
    ctx->syncedwalsize = 0;   
    ctx->walfile = fopen(filename, "a+b");
    if (ctx->walfile == NULL) { 
        die("Failed to open wal file %s!\n", filename);
    }
    //printf("Wal file %s is opened!\n\n", filename);
}

// Notice there may be two exception WAL files corresponding to one slab
// TODO: Optimize lock
void read_from_except_wal(struct slab *s, char *item, slice *value) {
    struct item_metadata *meta = (struct item_metadata *)item;
    slice key;
    rb_node* node;
    uint64_t off, len;
    uint32_t idx_saved, idx;
    FILE *fp;
    int ret;

    key.data = &item[sizeof(struct item_metadata)];
    key.len = meta->key_size;

    pthread_mutex_lock(&(s->except_wal_index_lock));
    node = lookupRBNode(s->except_wal, key);
    pthread_mutex_unlock(&(s->except_wal_index_lock));

    if (node != NULL) {
        off = *(uint64_t *)(node->value.data + sizeof(uint64_t));
        len = *(uint64_t *)(node->value.data + sizeof(uint64_t) * 2);
 
        value->len = len;   
        if (len == 0) {
            value->data = NULL;
            return;
        }

        value->data = (char *)malloc(len);
        idx_saved = *(uint32_t *)(node->value.data + sizeof(uint64_t) * 3);

        pthread_mutex_lock(&(s->except_wal_lock));
        if (idx_saved == s->except_wal_index - 1) {
            fp = s->except_fp_new;
        } else if (idx_saved == s->except_wal_index - 2) {
            fp = s->except_fp_old;
            if (fp == NULL) {
                die("fp == NULL! key = %ld\n", *(uint64_t *)(meta + 1));
            }
            assert(fp != NULL);
        } else {
            die("exc index too small: cur = %ld saved = %ld! key = %ld\n", s->except_wal_index, idx_saved, *(uint64_t *)(meta + 1));
            ++error_read;
            assert(0);
            pthread_mutex_unlock(&(s->except_wal_lock));
            printf("Error read = %ld\n", error_read.load());
            return;
        }
        if (fp == NULL) {
            printf("idx_saved = %d, except_wal_idx = %d\n", idx_saved, s->except_wal_index);
            ++error_read;
            pthread_mutex_unlock(&(s->except_wal_lock));
            printf("Error read = %ld\n", error_read.load());
            assert(0);
            return;
        }
        assert(fp != NULL);
        fseek(fp, off, SEEK_SET);
        ret = fread(value->data, 1, len, fp);
        assert(ret == len);
        pthread_mutex_unlock(&(s->except_wal_lock));
    }
}

void read_from_wal_tree(struct slab_context *ctx, char *item, slice *value) {
    struct item_metadata *meta = (struct item_metadata *)item;
    slice key;
    rb_node* node = NULL;

    key.data = &item[sizeof(struct item_metadata)];
    key.len = meta->key_size;

    pthread_mutex_lock(&(ctx->wal_tree_lock));
    node = lookupRBNode(ctx->wal_tree, key);
    if (node != NULL) {
        value->len = node->value.len;
        if (value->len != 0) {
            value->data = (char *)malloc(value->len);
            memcpy(value->data, node->value.data, value->len);
        } else {
            value->data = NULL;
        }
    }
    pthread_mutex_unlock(&(ctx->wal_tree_lock));
}


char* read_from_wal_hash(struct slab_context *ctx, char *item)
{
    char *data = NULL;
    pthread_mutex_lock(&(ctx->wal_tree_lock));
    struct item_metadata *meta = (struct item_metadata *)item;
    uint64_t hash = get_hash_for_item(item);
    size_t idx = hash % ctx->buckets;
    if (ctx->wal_hash[idx] != NULL) {
        uint64_t count = *(uint64_t *)ctx->wal_hash[idx];
        for (uint64_t i = 1; i <= count; i++) {
            if (ctx->wal_hash[idx][i]) {
                struct item_metadata *dest = (struct item_metadata *)ctx->wal_hash[idx][i];
                if (dest->key_size == meta->key_size && !memcmp(dest + 1, meta + 1, dest->key_size)) {
                    data = (char *)malloc(sizeof(*dest) + dest->key_size + dest->value_size);
                    memcpy(data, dest, sizeof(*dest) + dest->key_size + dest->value_size);
                    break;
                }
            }
        }
    }
    pthread_mutex_unlock(&(ctx->wal_tree_lock));
    return data;
}

void except_wal_write(struct slab *s, char *item) {
    int item_size, fd;
    uint8_t sync_needed = 0;
    uint64_t off;
    uint32_t except_wal_index;
    // FILE *fp;

    ++s->except_wal_item_cnt;

    item_size = get_item_size(item);
    pthread_mutex_lock(&(s->except_wal_lock));
    // fp = (s->except_fp_new == NULL) ? s->except_fp_old : s->except_fp_new;
    fseek(s->except_fp_new, s->except_wal_size, SEEK_SET);
    fwrite(item, item_size, 1, s->except_fp_new);
    off = s->except_wal_size; 
    except_wal_index = s->except_wal_index - 1;
    s->except_wal_size += item_size;
    if((s->except_wal_size - s->synced_except_wal_size) >= EXCEPTION_WAL_SYNC_SIZE) {
        sync_needed = 1;
        fd = fileno(s->except_fp_new);
        s->synced_except_wal_size = s->except_wal_size;
    }
    // s->except_offset += get_item_size(item);
    // fflush(s->except_fp_new);
    // fdatasync(fileno(s->except_fp_new));
    pthread_mutex_unlock(&(s->except_wal_lock));

    if (sync_needed) {
        fflush(s->except_fp_new);
        if(fdatasync(fd) != 0) {
            die("Failed to sync exception wal file!\n");
        }
    }
    insert_into_except_index(s, item, off, except_wal_index);

    ++exp_num;
    if ((exp_num & 0xFFFFF) == 0) {
        fdatasync(fd);
        // printf("Current total KV num in exp wal = %d\n", exp_num.load());
        need_throttle = exp_num / (THROTTLE_STEP);
    }
}

// TODO: sync every xx ms in background thread
// TODO: may be we can also put sync operation in background thread
void wal_write(struct slab_context *ctx, struct slab_callback *callback, uint8_t has_seq_id, uint8_t *flush_needed) {
    uint8_t close_needed = 0;
    int fd = -1, ret = -1;
    FILE *fp = NULL;
    struct item_metadata *meta;
    uint32_t wal_to_flush_num = 0, sleep_us;
    uint64_t total_written_kv = 0;
    buf_elem_t *buf;
    uint8_t buf_full = 0, buf_in_use;
#ifdef WAL_DEBUG
    uint32_t cnt = 0;
#endif

    meta = (struct item_metadata *)callback->item;

    while (__sync_add_and_fetch(&ctx->kv_pending, 1) > KV_NUM_IN_BUF_LIMIT * (KV_BUF_NUM - 2) + has_seq_id * nb_workers) {
        __sync_sub_and_fetch(&ctx->kv_pending, 1);
        usleep(1);
#ifdef WAL_DEBUG
        if (++cnt % 12800 == 0) {
            printf("Still waiting; has seq id = %d, cnt = %d\n", has_seq_id, cnt);
        }
#endif
    }

    pthread_mutex_lock(&(ctx->wallock));
    if (!has_seq_id) {
        meta->seq_id = __sync_add_and_fetch(&cur_seq_id, 1);
    }
    ret = fwrite(callback->item, 1, get_item_size(callback->item), ctx->walfile);
    fp = ctx->walfile;
    fd = fileno(fp); 
    ctx->walsize += get_item_size(callback->item);

    pthread_mutex_lock(&(ctx->buf_lock));
    buf = &(ctx->buf_elem[ctx->buf_in_use]);

    memcpy(buf->buf + buf->kv_num * sizeof(void *), &callback, sizeof(callback));

    ++buf->kv_num;
    // assert(buf->kv_num <= KV_NUM_IN_BUF_LIMIT);
    if (buf->kv_num == KV_NUM_IN_BUF_LIMIT) {
        buf_full = 1;
        ctx->buf_in_use = (ctx->buf_in_use + 1) % KV_BUF_NUM;
        ctx->buf_elem[ctx->buf_in_use].kv_num = 0;
    }
    pthread_mutex_unlock(&(ctx->buf_lock));

    if (buf_full == 1 && ctx->walsize >= WAL_FILE_SIZE) {
        if (ctx->walsize >= WAL_FILE_SIZE) {
            close_needed = 1;
            __sync_lock_test_and_set(&buf->file_idx, ctx->walid);
            ++ctx->walid;
            wal_open(ctx);
        } else {
            __sync_lock_test_and_set(&buf->file_idx, INVALID_64_BIT);
        }
    }
    pthread_mutex_unlock(&(ctx->wallock));

    if (buf_full == 1) {
        fflush(fp);
        if(fdatasync(fd) != 0) {
            die("Failed to sync wal file!\n");
        }
    
        if (close_needed ==1) {
            fclose(fp);
        }

        *flush_needed = 1;
    }
}

/*
 * Worker context
 */

static inline uint8_t test_bit_set(uint8_t *page_bitmap, uint64_t page_idx) {
    uint32_t byte = (page_idx >> 3) % BITMAP_BYTES;
    uint8_t bit = (page_idx & 0x7);

    if ((page_bitmap[byte] & (1 << bit)) > 0) {
        return 1;
    } else {
        page_bitmap[byte] |= (1 << bit);
        return 0;
    }
}

void print_bitmap(struct slab *s, uint64_t page_idx) {
    uint32_t byte = (page_idx >> 3) % BITMAP_BYTES;
    uint8_t bit = (page_idx & 0x7);

    printf("Bitmap = 0x%x, bit_index = %d\n", s->page_bitmap[byte], bit);
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

/* Dequeue enqueued callbacks */
static void worker_dequeue_requests(struct slab_context *ctx) {
#if 0
   size_t retries =  0;
   size_t sent_callbacks = ctx->sent_callbacks;
   size_t pending = sent_callbacks - ctx->processed_callbacks;
   int file_index = -1000 ;
   uint8_t bit_set = 0;

   if(pending == 0)
      return;
   //printf("sent %lu, pending %lu\n", sent_callbacks, pending);
again:
   for(size_t i = 0; i < pending; i++) {
      struct slab_callback *callback = ctx->callbacks[ctx->processed_callbacks%ctx->max_pending_callbacks];
      enum slab_action action = callback->action;
      add_time_in_payload(callback, 2);

      //No need to lookup index for LEGO-KV
      
      //callback->slab = get_slab_with_index(ctx, callback->item, callback, &file_index);
      callback->slab = get_slab_with_index(ctx, callback->item, &file_index);
      callback->slab_idx = get_new_slab_idx(ctx, callback->slab, callback->item, callback);
      callback->ctx = ctx;

      if (ctx->rehash_idx == file_index) {
          ctx->processed_callbacks++;
		  except_wal_write(ctx, callback->slab, callback->item);
          free_callback(callback, NULL);
          continue;
      }

      bit_set = test_bit_set(callback->slab, callback->slab_idx);
      /*
      if (*(uint32_t *)(callback->item + sizeof(struct item_metadata)) == 36387 ||
              *(uint32_t *)(callback->item + sizeof(struct item_metadata)) == 36373) {
        printf("Will set map idx = %ld for %u, bit_set = %u\n", callback->slab_idx, *(uint32_t *)(callback->item + sizeof(struct item_metadata)), bit_set);
        print_bitmap(callback->slab, callback->slab_idx);
      }
      */
      if (bit_set) {
        // printf("Will write key %u to exception wal!\n", *(uint32_t *)(callback->item + sizeof(struct item_metadata)));
        ++conflict_page_kv;
        if ((conflict_page_kv & 0xFFF) == 0) {
          printf("Current conflict page KV (write to exception wal) num = %d (all workers)\n", conflict_page_kv.load());
        }
        ctx->processed_callbacks++;
		except_wal_write(ctx, callback->slab, callback->item);
        free_callback(callback, NULL);
        continue;
      }

      //printf("slab idx: %lu, id: %d, tag: %d\n", callback->slab_idx, callback->id, callback->tag);

      switch(action) {
         case READ_NO_LOOKUP:
            read_item_async(callback);
            break;
         case READ:
            {
               read_item_async(callback);
            }
            break;
         case ADD:
            {
               add_item_async(callback);
               //printf("Will add async!\n");
               //printf("add %d, %d, %d\n", callback->ctx->worker_id, callback->slab->id, callback->slab_idx);
            }
            break;
         case UPDATE:
            {
               update_item_async(callback);
               //update_item_sync(callback);
            }
            break;
         case DELETE:
            {
               //memory_index_delete(ctx->worker_id, callback->item);
               //remove_item_async(callback);
            }
            break;
         default:
            die("Unknown action\n");
      }
      ctx->processed_callbacks++;
      if(NEVER_EXCEED_QUEUE_DEPTH && io_pending(ctx->io_ctx) >= QUEUE_DEPTH)
         break;
   }

   if(WAIT_A_BIT_FOR_MORE_IOS) {
      while(retries < 5 && io_pending(ctx->io_ctx) < QUEUE_DEPTH) {
         retries++;
         pending = ctx->sent_callbacks - ctx->processed_callbacks;
         //printf("retries %lu, pending %lu", retries, pending);
         retries++;
         if(pending == 0) {
            wait_for(10000);
         } else {
            goto again;
         }
      }
   }
#endif
}

static void enqueue_exceptional_write(struct slab_context *ctx, struct slab *s)
{
#if 0
    pthread_mutex_lock(&(s->except_wal_lock));
    char path[512];
    sprintf(path, "%s-old", s->except_wal_path);
    fclose(s->except_pf);
    rename(s->except_wal_path, path);
    except_wal_open(s);
    /*deleteRBtree(s->except_wal);
    s->except_wal = createRBtree(normalCompare);*/
    s->pending = 0;
    ctx->rehashing = 0;
    pthread_mutex_unlock(&(s->except_wal_lock));

    int n;
    FILE *fp = fopen(path, "rb+");
    while (1) {
        char *item = (char *)malloc(ITEM_SIZE);
        n = fread(item, 1, ITEM_SIZE, fp);
        if (n != ITEM_SIZE) {
            free(item);
            break;
        }
        struct slab_callback *callback = (struct slab_callback *)malloc(sizeof(*callback));
        callback->payload = NULL;
        callback->item = item;
        //printf("key %d, value %d\n", entry->data->key_size, entry->data->value_size);
        callback->tag = REDO;

loop_1:
        pthread_mutex_lock(&(ctx->slab_lock));
        size_t buffer_idx = get_slab_buffer(ctx);
        if (buffer_idx == INVALID_SLOT) {
            pthread_mutex_unlock(&(ctx->slab_lock));
            usleep(2);
            goto loop_1;
        }
        callback->action = ADD;
        //ctx->callbacks[buffer_idx] = callback;
        submit_slab_buffer(ctx, buffer_idx);
        pthread_mutex_unlock(&(ctx->slab_lock));

        add_time_in_payload(callback, 1);
    }
    fclose(fp);
    remove(path);
#endif
}

static inline int queue_full(uint16_t busy_read_slot, uint16_t busy_write_slot, uint32_t depth, int read_io) {
    if (read_io) {
        return (busy_read_slot >= depth * 2 ||
                (busy_read_slot + busy_write_slot) >= depth * 2);
    } else {
        return (busy_write_slot >= depth * 2);
    }
}

// When this function is invoked, there are free slots available 
static inline int get_io_slot(uint8_t *io_slot_map, uint32_t depth) {
    int len = (depth * 2 + 63) >> 6;
    int i, j, k, idx = -1;
    uint8_t *ptr_8;
    uint64_t *ptr_64;

    ptr_64 = (uint64_t *)io_slot_map;
    // printf("slot map = 0x%x\n", *ptr_64);
    for (i = 0; i < len; i++) {
        if (*ptr_64 == INVALID_64_BIT) {
            ++ptr_64;
            continue;
        }
        ptr_8 = (uint8_t *)ptr_64;
        for (j = 0; j < 8; j++) {
            //printf("this byte = 0x%x\n", *ptr_8);
            if (*ptr_8 == INVALID_8_BIT) {
                ++ptr_8;
                continue;
            }
            for (k = 0; k < 8; k++) {
                //printf("    k = %d, bit = %d\n", k, ((*ptr_8 >> k) & 0x1));
                if (((*ptr_8 >> k) & 0x1) == 0) {
                    idx = i * 64 + j * 8 + k;
                    *ptr_8 |= (1 << k);
                    return idx;       
                }
            }
        }
    }

    perr("Error: Should get a slot!\n");
    if (idx >= depth * 2) {
        die("Slot id error %d\n", idx);
    }
    return idx;
}

static inline void free_io_slot(uint8_t *io_slot_map, int slot_idx) {
    uint8_t *ptr_8;

    ptr_8 = (uint8_t *)io_slot_map;
    ptr_8 += (slot_idx >> 3);
    *ptr_8 &= (~(1 << (slot_idx & 0x7)));
}

// Check both read and write IOs when this function is invoked
// Guarantee at least one write IO is finished
static inline void check_finished_io(struct io_context *io_ctx, uint32_t *busy_read_slot, uint32_t *busy_write_slot, int depth, uint8_t *io_slot_map, int all_io, uint8_t foreground) {
    int i, j, ret;
    int min_io, freed_io_slot = 0;
    aio_context_t *aio_ctx;
    struct iocb *cb;
    struct slab_callback *callback;
    int total = 0;
    uint8_t has_kv_finished = 0;
    struct slab_context *ctx; 

    aio_ctx = &(io_ctx->ctx);
    min_io = all_io ? (*busy_read_slot + *busy_write_slot) : 1;
    
loop:
    ret = io_getevents_wrapper(io_ctx, min_io, depth * 2);
    if (ret < 0) {
        die("Error: got errors %d when invoking io_getevents.\n", ret);
    }

    for (i = 0; i < ret; i++) {
        has_kv_finished = 0;

        cb = (struct iocb *)io_ctx->events[i].obj;
        callback = (struct slab_callback *)cb->aio_data;

        if (io_ctx->events[i].res != page_size) {
            printf("res = %d, page size = %d, page idx = %ld\n", io_ctx->events[i].res, page_size, callback->page_idx);
        }
        assert(io_ctx->events[i].res == page_size);
        if (callback->rd) {
            callback->rd = 0;
            --(*busy_read_slot);
            ++(*busy_write_slot);
        } else {
            ++freed_io_slot;
            free_io_slot(io_slot_map, callback->slot_idx);
            --(*busy_write_slot);
            has_kv_finished = 1;
        }
        // Remove item from (exception) wal index in io_cb if applicable 
        callback->io_cb(callback);
        if (callback->page_full) {
            ++freed_io_slot;
            free_io_slot(io_slot_map, callback->slot_idx);
            --(*busy_write_slot);
            has_kv_finished = 1;
        }

        if (has_kv_finished && foreground) {
            ctx = callback->ctx;
            free_callback(callback, NULL);
            // __asm__ __volatile__("" ::: "memory");
            __sync_fetch_and_sub(&ctx->kv_pending, 1);
        }
    }

    if (!all_io && !freed_io_slot) {
        goto loop;
    }

    if (!all_io) {
        return;
    }
    
    assert((*busy_read_slot) == 0);
    ret = io_getevents_wrapper(io_ctx, *busy_write_slot, depth * 2);
    if (ret < 0) {
        die("Error: got errors %d when invoking io_getevents.\n", ret);
    }
    for (i = 0; i < ret; i++) {
        cb = (struct iocb *)io_ctx->events[i].obj;
        callback = (struct slab_callback *)cb->aio_data;
        assert(io_ctx->events[i].res == page_size);
        callback->io_cb(callback);
        if (!callback->rd) {
            free_io_slot(io_slot_map, callback->slot_idx);
            --(*busy_write_slot);
            
            if (foreground) {
                ctx = callback->ctx;
                // printf("Will free\n");
                free_callback(callback, NULL);
                // __asm__ __volatile__("" ::: "memory");
                __sync_fetch_and_sub(&ctx->kv_pending, 1);
            }
        } else {
            die("No read io should be here!\n");
        }
    }
}

static inline int fill_kv_info(uint8_t **submit_ptr) {
    int key_len = 0, value_len = 0;
    struct item_metadata *meta;
    int size;

    // callback->item = (char *)(*submit_ptr);
    meta = (struct item_metadata *)(*submit_ptr);

    if (meta->key_size == 0 && meta->value_size == 0) {
        size = INVALID_LEN;
    } else {
        size = (sizeof(struct item_metadata) + meta->key_size + meta->value_size);
    }

    *submit_ptr += size;

    return size;
}
// We double the slab size for rehash
// TODO: whether using direct IO?
static void do_rehash(struct slab *s, struct rehash_worker *rehash_ctx) {
    char path[MAX_FILE_NAME_LEN];
    uint8_t *page_old, *page_new_1, *page_new_2;
    int off_old, ret, fd;
    // int old_fd = open(s->path, O_RDONLY|O_DIRECT);
    // int old_fd = s->fd_old;
    int old_fd;
    uint32_t old_page_idx, new_page_idx;
    uint32_t new_page_off_1 = 0, new_page_off_2 = 0, item_size;
    uint64_t left_size = 0, cur_read_size = 0;
    uint64_t parsed_size = 0;
    int prev_fd, k;
    uint64_t cumulative_size = 0;
    struct item_metadata *meta_old;
    uint64_t hash;
    int batch_size = rehash_ctx->batch_size;
    uint8_t *old_batch = rehash_ctx->old_batch;
    uint8_t *new_1_batch = rehash_ctx->new_1_batch;
    uint8_t *new_2_batch = rehash_ctx->new_2_batch;
    int idx_old, idx_new;
    uint32_t old_start_idx;
    uint32_t old_page_num;
    uint64_t size_on_disk;
    uint32_t rehash_radix;

    pthread_spin_lock(&(s->meta_lock));
    idx_old = s->idx_old;
    rehash_radix = s->rehash_radix;
    pthread_spin_unlock(&(s->meta_lock));
    idx_new = (idx_old == 0) ? 1 : 0;
    sprintf(path, "%s/%d-%d.slab", s->path, s->id, idx_new);
    fd = open(path, O_RDWR|O_CREAT|O_DIRECT, 0666);
    if (fd < 0) {
        die("Cannot open slab %s for rehash!\n", path);
    }
    old_page_num = (s->init_num_pages << rehash_radix);
    ret = fallocate(fd, 0, 0, (off_t)old_page_num * 2 * page_size);
    if (ret < 0) {
        die("Cannot allocate space for slab %s with size %ld!\n", path, (off_t)old_page_num * 2 * page_size);
    }


    //printf("Will sleep 5s!\n");
    // usleep(5000000);


    sprintf(path, "%s/%d-%d.slab", s->path, s->id, idx_old);
    old_fd = open(path, O_RDONLY|O_DIRECT, 0666);
    if (old_fd < 0) {
        die("Old file %s open error when rehash!\n", path);
    }

    parsed_size = 0;
    old_start_idx = 0;
    size_on_disk = (uint64_t)old_page_num * page_size;

    assert(size_on_disk == get_file_size(path));
    while (parsed_size < size_on_disk) {
        left_size = size_on_disk - parsed_size;
        cur_read_size = (left_size >= batch_size) ? batch_size : left_size;
        ret = pread(old_fd, old_batch, cur_read_size, parsed_size);
        if (ret != cur_read_size) {
            die("Rehash read error! ret = %ld and cur_read_size = %ld\n", ret, cur_read_size);
        }

        memset(new_1_batch, 0, cur_read_size);
        memset(new_2_batch, 0, cur_read_size);

        for (k = 0; k < cur_read_size / page_size; k++) {
            page_old = old_batch + k * page_size;
            page_new_1 = new_1_batch + k * page_size;
            page_new_2 = new_2_batch + k * page_size;
            new_page_off_1 = 0;
            new_page_off_2 = 0;
            item_size = 0;
            for (off_old = 0; off_old + sizeof(*meta_old) <= page_size; off_old += item_size) {
                meta_old = (struct item_metadata *)(page_old + off_old);
                if (meta_old->key_size == 0 && meta_old->value_size == 0) {
                    assert(off_old + sizeof(*meta_old) <= page_size);
                    break;
                }

                item_size = sizeof(*meta_old) + meta_old->key_size + meta_old->value_size;
                hash = get_hash_for_item((char *)meta_old);
                new_page_idx = hash % (old_page_num * 2LU);

                assert(new_page_idx + 1 <= old_page_num * 2);

                if (new_page_idx < old_page_num) {
                    memcpy(page_new_1 + new_page_off_1, (uint8_t *)meta_old, item_size);
                    new_page_off_1 += item_size;
                } else {
                    memcpy(page_new_2 + new_page_off_2, (uint8_t *)meta_old, item_size);
                    new_page_off_2 += item_size;
                }
            }
        }

        // Write new pages
        pwrite(fd, new_1_batch, cur_read_size, old_start_idx * (uint64_t)page_size);
        pwrite(fd, new_2_batch, cur_read_size, (old_start_idx + old_page_num) * (uint64_t)page_size);
        parsed_size += cur_read_size;
        old_start_idx += k;

        assert(old_start_idx == parsed_size / 4096);
    }
    close(old_fd);

    s->fd_new = fd;
}

/*
void add_to_except_item_list(struct slab_callback *callback) {
    struct item_metadata *meta;
    item_entry *node = NULL, *head, *tmp;
    uint32_t size;

    node = (item_entry *)calloc(sizeof(item_entry), 1);
    meta = (struct item_metadata *)callback->item;
    size = sizeof(struct item_metadata) + meta->key_size;
    node->data = (struct item_metadata *)malloc(size);
    memcpy(node->data, callback->item, size);

    head = callback->slab->head;
    tmp = head->next;
    head->next = node;
    node->next = tmp;
}
*/

void migrate_exception_wal(struct slab *s, int rehash_idx, struct rehash_worker *ctx) {
    uint64_t flushed_size, file_size;
    uint32_t read_size, slot_idx, kv_size;   
    int fd;
    uint8_t *io_buffer, *submit_ptr, *tmp_ptr, *prev_ptr;
    uint8_t bit_set;
    struct slab_callback *callback;
    char file[MAX_FILE_NAME_LEN];
    int file_exist;
    uint8_t partial_end = 0;

    // TODO: use lock to get except wal index
    sprintf(file, "%s/%d-%d.except", s->path, s->id, s->except_wal_index - 2);
    file_exist = !access(file, 0);
    assert(file_exist == 1);
    file_size = get_file_size(file);
    flushed_size = 0;
    io_buffer = ctx->io_buffer;
    fd = open(file, O_RDONLY, 0666);

    while (flushed_size + sizeof(struct item_metadata) <= file_size) {
        read_size = (flushed_size + EXCEPT_WAL_IO_BUFFER_SIZE > file_size) ?
                    (file_size - flushed_size) : EXCEPT_WAL_IO_BUFFER_SIZE;
        assert(read_size <= EXCEPT_WAL_IO_BUFFER_SIZE);
        pread(fd, io_buffer, read_size, flushed_size);
        submit_ptr = io_buffer;

        while (submit_ptr - io_buffer < read_size) {
            if (submit_ptr - io_buffer + sizeof(struct item_metadata) > read_size) {
                // printf("Need more data in migration! flushed size = %ld and file size = %ld\n", flushed_size, file_size);
                break;
            }

            if (queue_full(ctx->busy_read_slot, ctx->busy_write_slot, REHASH_QUEUE_DEPTH, 1)) {
                check_finished_io(ctx->io_ctx, &ctx->busy_read_slot, &ctx->busy_write_slot, REHASH_QUEUE_DEPTH, ctx->io_slot_map, 0, 0);
            }
                
            slot_idx = get_io_slot(ctx->io_slot_map, REHASH_QUEUE_DEPTH);
            ++ctx->busy_read_slot;
            callback = &ctx->callbacks[slot_idx];

            tmp_ptr = submit_ptr;
            kv_size = fill_kv_info(&tmp_ptr);
            if (kv_size == INVALID_LEN) {
                partial_end = 1;
                break;
            }
            if (tmp_ptr - io_buffer > read_size) {
                // KV incomplete
                free_io_slot(ctx->io_slot_map, slot_idx);
                --ctx->busy_read_slot;
                if (flushed_size + read_size >= file_size) {
                    partial_end = 1;
                }
                break;
            }
            memcpy(callback->item, submit_ptr, kv_size);
            submit_ptr = tmp_ptr;

            s->except_wal_item_migration_cnt++;

            callback->ctx = NULL;
            callback->io_ctx = ctx->io_ctx;
            callback->rd = 1;
            callback->page_full = 0;
            callback->slot_idx = slot_idx;
            assert(slot_idx < REHASH_QUEUE_DEPTH * 2);
            callback->page_buffer = ctx->io_page_buffer + slot_idx * page_size;

            // Different from normal operation
            callback->hash = get_hash_for_item(callback->item);
            callback->slab = s;
            callback->migration = 1;
            callback->fd = s->fd_new;
            callback->page_idx = get_slab_page_idx(callback);
            callback->cb = NULL;

            // add_to_except_item_list(callback);
            // All slabs in one rehash worker share the same slab bitmap during rehash since slabs are rehashed one by one
            bit_set = test_bit_set(ctx->page_bitmap, callback->page_idx);
            if (bit_set) {
                ++conflict_page_kv;
                if (conflict_page_kv) {
                    //printf("Current conflict page KV (write to exception wal) num = %d (all workers)\n", conflict_page_kv.load());
                }

                // Will be written to the new opened exception WAL 
                except_wal_write(callback->slab, callback->item);
                free_io_slot(ctx->io_slot_map, slot_idx);
                --ctx->busy_read_slot;

#ifdef KEY_TRACE
                struct item_metadata *meta = (struct item_metadata *)callback->item; 
                printf("key = %ld has been rewritten to exc wal during migration due to bit map!\n", *(uint64_t *)(meta + 1));
#endif

                continue;
            }

            callback->slab->migrate_page_bitmap = ctx->page_bitmap;
            migrate_item_async(callback);
        }

        flushed_size += (submit_ptr - io_buffer);
        if (partial_end == 1) {
            break;
        }
    }
    
    check_finished_io(ctx->io_ctx, &ctx->busy_read_slot, &ctx->busy_write_slot, REHASH_QUEUE_DEPTH, ctx->io_slot_map, 1, 0);
    assert(ctx->busy_read_slot == 0 && ctx->busy_write_slot == 0);

    close(fd);
}

static void init_rocks_buf() {
    int i, j;
    int group_num;

    group_num = (nb_workers + WORKERS_PER_ROCKS_BUF_GROUP - 1) / WORKERS_PER_ROCKS_BUF_GROUP;
    rocks_buf_groups = (rocks_buf_group_elem_t *)malloc(group_num * sizeof(rocks_buf_group_elem_t));
    for (i = 0; i < group_num; i++) {
        rocks_buf_groups[i].id = i;

        pthread_mutex_init(&rocks_buf_groups[i].lock, NULL);
        pthread_mutex_lock(&rocks_buf_groups[i].lock);
        rocks_buf_groups[i].buf_in_use = 0;
        rocks_buf_groups[i].buf_used = 0;
        rocks_buf_groups[i].buf_len_used = (int *)calloc(ROCKS_BUF_NUM_PER_GROUP, sizeof(int));

        rocks_buf_groups[i].buf = (uint8_t **)malloc(ROCKS_BUF_NUM_PER_GROUP * sizeof(uint8_t *));
        for (j = 0; j < ROCKS_BUF_NUM_PER_GROUP; j++) {
            rocks_buf_groups[i].buf[j] = (uint8_t *)valloc(ROCKS_BUF_LEN);
            memset(rocks_buf_groups[i].buf[j], 0, ROCKS_BUF_LEN);
        }
        pthread_mutex_unlock(&rocks_buf_groups[i].lock);
    }
}

static void *worker_rocks(void *pdata) {
    rocks_context_t *ctx = (rocks_context_t *)pdata;
    int i;
    uint8_t *buf, *parsed;
    uint32_t buf_idx, len;
    uint32_t key_len, value_len;
    int group_id, buf_id;
    rocks_node_t *elem;

    pthread_mutex_init(&ctx->lock, NULL);
    pthread_mutex_lock(&ctx->lock);
    ctx->task_num = 0;
    ctx->head = (rocks_node_t *)malloc(sizeof(rocks_node_t));
    ctx->head->group_id = -1;
    ctx->head->buf_id = -1;
    ctx->head->next = ctx->head;
    ctx->head->prev = ctx->head;
    pthread_mutex_unlock(&ctx->lock);

    __sync_add_and_fetch(&nb_rocks_workers_ready, 1);

    while (1) {
        pthread_mutex_lock(&ctx->lock);
        while (ctx->task_num == 0) {
            pthread_cond_wait(&ctx->ready, &ctx->lock);
        }
        elem = ctx->head->next;
        group_id = elem->group_id;
        buf_id = elem->buf_id;
        ctx->head->next = elem->next;
        elem->next->prev = ctx->head;
        --ctx->task_num;
        pthread_mutex_unlock(&ctx->lock);
        // printf("Rocks start to work!\n");
        free(elem);
        elem = NULL;

        buf = rocks_buf_groups[group_id].buf[buf_id];
        write_into_rocks_batch((char *)buf, ROCKS_BUF_LEN);

        pthread_mutex_lock(&rocks_buf_groups[group_id].lock);
        --rocks_buf_groups[group_id].buf_used;
        rocks_buf_groups[group_id].buf_len_used[buf_id] = 0;
        // memset(rocks_buf_groups[group_id].buf[buf_id], 0, ROCKS_BUF_LEN);
        pthread_mutex_unlock(&rocks_buf_groups[group_id].lock);
    }
}

static void *worker_rehash(void *pdata) {
    struct slab_context *ctx;
    struct rehash_worker *rehash_ctx = (struct rehash_worker *)(pdata);
    struct slab *s;
    // int thread_idx = *((int *)(pdata));
    int ctx_per_rehash_thread = (nb_workers + nb_rehash - 1) / nb_rehash;
    int i, ctx_idx, slab_idx, idx_old, idx_new;
    uint8_t *old_batch, *new_1_batch, *new_2_batch;
    char file_name[MAX_FILE_NAME_LEN];
    uint32_t rehash_idx_tmp, except_wal_idx;
    item_entry *tmp;
    int batch_size;
    char *validate;
    uint32_t total_kv, switch_cnt;
    rehash_elem_t *elem;
    uint64_t retry_cnt;

    batch_size = page_size * REHASH_PAGES;
    rehash_ctx->batch_size = batch_size;
    rehash_ctx->old_batch = (uint8_t *)valloc(batch_size);
    rehash_ctx->new_1_batch = (uint8_t *)valloc(batch_size);
    rehash_ctx->new_2_batch = (uint8_t *)valloc(batch_size);

    rehash_ctx->io_page_buffer = (uint8_t *)valloc(REHASH_QUEUE_DEPTH * 2 * page_size);
    rehash_ctx->io_slot_map = (uint8_t *)calloc(1, (REHASH_QUEUE_DEPTH * 2 + 7) >> 3);
    rehash_ctx->page_bitmap = (uint8_t *)calloc(1, BITMAP_BYTES);

    rehash_ctx->io_ctx = worker_ioengine_init(REHASH_QUEUE_DEPTH * 2);
    rehash_ctx->io_buffer = (uint8_t *)valloc(EXCEPT_WAL_IO_BUFFER_SIZE);
    rehash_ctx->busy_read_slot = 0;
    rehash_ctx->busy_write_slot = 0;
    rehash_ctx->callbacks = (struct slab_callback *)calloc(REHASH_QUEUE_DEPTH * 2, sizeof(struct slab_callback));
    for (i = 0; i < REHASH_QUEUE_DEPTH * 2; i++) {
        rehash_ctx->callbacks[i].item = (char *)valloc(page_size);
    }

    pthread_mutex_init(&rehash_ctx->rehash_lock, NULL);
    pthread_mutex_lock(&rehash_ctx->rehash_lock);
    rehash_ctx->head = (rehash_elem_t *)malloc(sizeof(rehash_elem_t));
    rehash_ctx->head->idx = INVALID_REHASH_INDEX;
    rehash_ctx->head->next = rehash_ctx->head;
    rehash_ctx->head->prev = rehash_ctx->head;
    rehash_ctx->rehash_num = 0;
    pthread_mutex_unlock(&rehash_ctx->rehash_lock);

    rehash_idle[rehash_ctx->worker_id] = 1;

    __sync_add_and_fetch(&nb_rehash_workers_ready, 1);

    while (1) {
        pthread_mutex_lock(&rehash_ctx->rehash_lock);
        while (rehash_ctx->rehash_num == 0) {
            pthread_cond_wait(&rehash_ctx->rehash_ready, &rehash_ctx->rehash_lock);
        }
        elem = rehash_ctx->head->next;
        rehash_idx_tmp = elem->idx;
        rehash_ctx->head->next = elem->next;
        elem->next->prev = rehash_ctx->head;
        --rehash_ctx->rehash_num;
        pthread_mutex_unlock(&rehash_ctx->rehash_lock);
        free(elem);
        elem = NULL;

        rehash_idle[rehash_ctx->worker_id] = 0;

        assert(rehash_idx_tmp < nb_slabs);

        ctx_idx = rehash_idx_tmp / nb_slabs_per_worker;
        slab_idx = rehash_idx_tmp % nb_slabs;

        ctx = &slab_contexts[ctx_idx];
        s = slabs[slab_idx];

        assert(__sync_fetch_and_add(&ctx->rehash_idx, 0) == INVALID_REHASH_INDEX);

        __sync_lock_test_and_set(&ctx->rehash_idx, rehash_idx_tmp);
        // Make sure no new KV is written to this slab before rehash
        retry_cnt = 0;

        while (__sync_fetch_and_add(&s->item_being_written, 0) != 0) {
            usleep(50);
        }

        do_rehash(s, rehash_ctx);
        ++rehash_times;
        // printf("Done rehashing for worker %d, absolute slab %d, current total rehash times = %d\n", ctx->worker_id, rehash_idx_tmp, rehash_times.load());

        // Record items that can been written to the new slab so that
        // we can delete them from exception WAL index later
        assert(s->head->next == NULL);

        // Open a new exception WAL and make the old one static
        // fclose(s->except_fp);
        except_wal_idx = except_wal_open(s, 1); 
        migrate_exception_wal(s, rehash_idx_tmp, rehash_ctx);
        s->except_wal_item_migration_cnt = 0;
        
        switch_cnt = 0;
retry:
        pthread_spin_lock(&(s->meta_lock));
        s->slab_switch = 1;
        if (s->old_ref > 0) {
            pthread_spin_unlock(&(s->meta_lock));
            usleep(10);
            if (++switch_cnt % 1024) {
                // printf("Unable to switch slab!\n");
            }
            goto retry;
        }

        // Switch slab now
        close(s->fd_old);
        sprintf(file_name, "%s/%d-%d.slab", s->path, s->id, s->idx_old);
        remove(file_name);
        s->fd_old = s->fd_new;
        ++s->rehash_radix;
        s->idx_old = (s->idx_old == 0) ? 1 : 0;
        s->slab_switch = 0;
        pthread_spin_unlock(&(s->meta_lock));

        // Also we can close and delete the old exception WAL now since we could switch to the new slab and new exception WAL
        pthread_mutex_lock(&(s->except_wal_lock));
        fclose(s->except_fp_old);
        s->except_fp_old = NULL;
        pthread_mutex_unlock(&(s->except_wal_lock));
        sprintf(file_name, "%s/%d-%d.except", s->path, s->id, except_wal_idx - 1);
        remove(file_name);
        // printf("Exception wal file %s removed!\n", file_name);
        // Update exception WAL index
        // TODO: use spinning lock here
        total_kv = 0;
        while (s->head->next) {
            tmp = s->head->next;
            s->head->next = tmp->next;

            remove_from_except_wal_index(s, tmp->data);

            free(tmp->data);
            free(tmp);
            total_kv++;
        }
        exp_num -= total_kv;

        s->in_rehash_task_queue = 0;
        __sync_lock_test_and_set(&ctx->rehash_idx, INVALID_REHASH_INDEX);

        // Self-check rehash here in case the worker thread has no chance to add new rehash task
        if (ctx->idle == 1) {
            schedule_for_rehash(ctx, 0);
        }
        rehash_idle[rehash_ctx->worker_id] = 1;
    }

    return NULL;
}

// Rehash rules are flexible
// Here we use the size ratio between exception WAL and the corresponding slab
// TODO: consider to also use memory consumption in the rule
// Single caller for each slab_context
static int schedule_for_rehash(struct slab_context *ctx, uint8_t cond_sig) {
    int i, idx = -1;
    double ratio, ratio_max;
    struct slab *s;
    struct rehash_worker *rehash_ctx;
    uint8_t can_rehash = 0;
    rehash_elem_t *elem, *elem_tmp;

    // Due to single caller
    if (__sync_fetch_and_add(&ctx->rehash_call, 1) > 0) {
        __sync_fetch_and_sub(&ctx->rehash_call, 1);
        printf("Others calling for ctx id = %d\n", ctx->worker_id);
        return 0;
    }

    if (__sync_fetch_and_add(&ctx->rehash_idx, 0) != INVALID_REHASH_INDEX) {
        // printf("Being rehashed for ctx id = %d\n", ctx->worker_id);
        __sync_fetch_and_sub(&ctx->rehash_call, 1);
        return 0;
    }
    ratio_max = 0;
    for (i = 0; i < ctx->nb_slabs; i++) {
        s = ctx->slabs[i];

        if (s->in_rehash_task_queue) {
            continue;
        }

        ratio = (double)s->except_wal_size / (((uint64_t)s->init_num_pages << s->rehash_radix) * page_size);
        // printf("except_wal_size = %d, size_on_disk = %ld, ratio = %lf\n", s->except_wal_size, s->size_on_disk, ratio);
        if (ratio > ratio_max) {
            ratio_max = ratio;
            idx = i;
        }
    }

    // Add rehash to task queue
    if (ratio_max >= REHASH_RATIO) {
        idx = nb_slabs_per_worker * ctx->worker_id + idx;
        rehash_ctx = &rehash_contexts[idx / ((nb_workers * nb_slabs_per_worker + nb_rehash - 1) / nb_rehash)];

        elem = (rehash_elem_t *)malloc(sizeof(rehash_elem_t));
        memset(elem, 0, sizeof(rehash_elem_t));
        pthread_mutex_lock(&rehash_ctx->rehash_lock);
        elem->idx = idx;
        elem->next = rehash_ctx->head;
        elem_tmp = rehash_ctx->head->prev;
        rehash_ctx->head->prev = elem;
        elem_tmp->next = elem;
        elem->prev = elem_tmp;
        ++rehash_ctx->rehash_num;
        pthread_mutex_unlock(&rehash_ctx->rehash_lock);

        slabs[idx]->in_rehash_task_queue = 1;

        if (cond_sig) {
            pthread_cond_signal(&rehash_ctx->rehash_ready);
        }
    }

    __sync_fetch_and_sub(&ctx->rehash_call, 1);

    return 0;
}

static uint32_t get_bucket_num() {
    if (nb_pages_per_slab > MAX_BUCKET_NUM) {
        return MAX_BUCKET_NUM;
    } else if (nb_pages_per_slab < MIN_BUCKET_NUM) {
        return MIN_BUCKET_NUM;
    } else {
        return nb_pages_per_slab;
    }
}

// First sync exception WAL and then delete the WAL file
void *worker_del_wal(void *pdata) {
    del_wal_context_t *ctx = (del_wal_context_t *)pdata;
    del_wal_elem_t *elem = NULL;
    uint32_t i, del_cnt;
    struct slab_context *slab_ctx;
    int fd;
    FILE *fp;

    pthread_mutex_init(&ctx->lock, NULL);
    pthread_mutex_lock(&ctx->lock);
    ctx->head = (del_wal_elem_t *)malloc(sizeof(del_wal_elem_t));
    ctx->head->next = NULL;
    ctx->del_wal_file_cnt = 0; 
    pthread_mutex_unlock(&ctx->lock);

    __sync_add_and_fetch(&nb_del_wal_workers_ready, 1);

    while(1) {
        pthread_mutex_lock(&ctx->lock);
        while (ctx->del_wal_file_cnt == 0) {
            pthread_cond_wait(&ctx->ready, &ctx->lock);
        }

        elem = ctx->head->next;
        ctx->head->next = elem->next;
        slab_ctx = elem->ctx;
        --ctx->del_wal_file_cnt;
        pthread_mutex_unlock(&ctx->lock);

        for (i = 0; i < nb_slabs_per_worker; i++) {
            pthread_mutex_lock(&(slab_ctx->slabs[i]->except_wal_lock));
            fp = slab_ctx->slabs[i]->except_fp_new;
            fd = fileno(fp);
            pthread_mutex_unlock(&(slab_ctx->slabs[i]->except_wal_lock));
            
            fflush(fp);
            fdatasync(fd);
        }

        remove(elem->path);
        free(elem);
    }
}

static inline void record_seq_id(struct slab_callback *callback, char *file_name) {
    // struct item_metadata *meta = (struct item_metadata *)callback->item;
    FILE *fp;
   
    sprintf(file_name, "%s/SEQ", callback->ctx->path);
    // printf("record seq id in file %s\n", file_name); 
    fp = fopen(file_name, "w");
    if (!fp) {
        die("Meta file %s open error when writing!\n", file_name);
    }
    fprintf(fp, "%ld", __sync_fetch_and_add(&cur_seq_id, 0));
    fdatasync(fileno(fp));
    fclose(fp);
}

static void *worker_slab_init(void *pdata) {
    struct slab_context *ctx = (struct slab_context *) pdata;
    int i;
	uint8_t *wal_io_buffer = NULL, *submit_ptr = NULL, *tmp_ptr = NULL, *prev_ptr = NULL;
    struct slab_callback *callback = NULL;
    uint32_t read_size;
    int fd, slot_idx, bit_set, slab_index, tmp;
    char file[MAX_FILE_NAME_LEN];
    del_wal_elem_t *del_wal_elem;
    del_wal_context_t *del_wal_ctx;
    uint8_t item_being_written_added;
    uint8_t partial_end = 0;
    uint64_t mark_wal_idx;
    uint8_t *buf_flush;
    uint8_t buf_flush_id;
    uint64_t kv_cnt_round = 0;
    
    __sync_add_and_fetch(&nb_workers_launched, 1);

    ctx->nb_slabs = nb_slabs_per_worker;
    ctx->rehashing = 0;
    __sync_lock_test_and_set(&ctx->rehash_idx, INVALID_REHASH_INDEX);
    ctx->wal_tree = createRBtree(normalCompare);
    ctx->buckets = get_bucket_num();
    ctx->wal_hash = (void ***)calloc(sizeof(void **), ctx->buckets);
    pthread_mutex_init(&(ctx->wallock), NULL);

    ctx->buffer = (uint8_t *)malloc(WAL_BUFFER_SIZE);
    ctx->buffer_size = 0;
    pthread_mutex_init(&(ctx->wal_tree_lock), NULL);
    pthread_mutex_init(&(ctx->slab_lock), NULL);

    ctx->slabs = (struct slab **)malloc(nb_slabs_per_worker * sizeof(*ctx->slabs));
    for (i = 0; i < nb_slabs_per_worker; i++) {
        ctx->slabs[i] = slabs[ctx->worker_id * nb_slabs_per_worker + i];
    }

    // Initialize the async io for the worker
    ctx->io_ctx = worker_ioengine_init(queue_depth * 2);
    wal_io_buffer = (uint8_t *)valloc(WAL_IO_BUFFER_SIZE);
    ctx->busy_read_slot = 0;
    ctx->busy_write_slot = 0;

    ctx->callbacks = (struct slab_callback **)calloc(queue_depth * 2, sizeof(struct slab_callback *));
    // Allocate IO buffer for async read/write
    ctx->io_page_buffer = (char *)valloc(queue_depth * 2 * page_size);
    ctx->io_slot_map = (uint8_t *)calloc((queue_depth * 2 + 7) >> 3, sizeof(uint8_t));

    pthread_mutex_init(&(ctx->buf_lock), NULL);

    pthread_mutex_lock(&(ctx->buf_lock));
    ctx->buf_in_use = 0;
    ctx->buf_in_flush = 0;
    for (i = 0; i < KV_BUF_NUM; i++) {
        ctx->buf_elem[i].kv_num = 0;
        ctx->buf_elem[i].file_idx = INVALID_64_BIT;
    }
    pthread_mutex_unlock(&(ctx->buf_lock));

    for (i = 0; i < KV_BUF_NUM; i++) {
        ctx->buf_elem[i].buf = (uint8_t *)malloc(sizeof(void *) * KV_NUM_IN_BUF_LIMIT);
        memset(ctx->buf_elem[i].buf, 0, sizeof(void *) * KV_NUM_IN_BUF_LIMIT);
    }

    pthread_mutex_init(&(ctx->worker_lock), NULL);
    pthread_mutex_lock(&(ctx->worker_lock));
    ctx->wal_to_flush_id = 0;
    ctx->wal_cur_id = 0;
    ctx->wal_to_flush_num = 0;
    ctx->buf_need_flush = 0;
    pthread_mutex_unlock(&(ctx->worker_lock));

    ctx->sleep_us = 0;
    ctx->idle = 0;
    ctx->rehash_call = 0;

    ctx->walid = 0;
    ctx->walsize = 0;
    wal_open(ctx);
    worker_idle[ctx->worker_id] = 1;
   
    ctx->kv_pending = 0;
    
    __sync_add_and_fetch(&nb_workers_ready, 1);

    while(1) {
        pthread_mutex_lock(&ctx->worker_lock);
        while (ctx->buf_need_flush == 0) {
            pthread_cond_wait(&ctx->worker_ready, &ctx->worker_lock);
        }
        --ctx->buf_need_flush;
        pthread_mutex_unlock(&ctx->worker_lock);

        ctx->idle = 0;
        worker_idle[ctx->worker_id] = 0;

        pthread_mutex_lock(&(ctx->buf_lock));
        buf_flush_id = ctx->buf_in_flush;
        ++ctx->buf_in_flush;
        ctx->buf_in_flush %= KV_BUF_NUM;
        buf_flush = ctx->buf_elem[buf_flush_id].buf;
        pthread_mutex_unlock(&(ctx->buf_lock));

        for (i = 0; i < KV_NUM_IN_BUF_LIMIT; i++) {
            if (queue_full(ctx->busy_read_slot, ctx->busy_write_slot, queue_depth, 1)) {
                check_finished_io(ctx->io_ctx, &ctx->busy_read_slot, &ctx->busy_write_slot, queue_depth, ctx->io_slot_map, 0, 1);
            }

            slot_idx = get_io_slot(ctx->io_slot_map, queue_depth);
            ++ctx->busy_read_slot;

            memcpy(&ctx->callbacks[slot_idx], buf_flush + i * sizeof(void *), sizeof(callback));

            callback = ctx->callbacks[slot_idx];
            assert(callback != NULL);
            callback->ctx = ctx;
            callback->io_ctx = ctx->io_ctx;
            callback->rd = 1;
            callback->page_full = 0;
            callback->slot_idx = slot_idx;
            callback->page_buffer = ctx->io_page_buffer + slot_idx * page_size;

            // Whether the slab is being rehashed or the same page is being modifed
            // If so, redirect the KV to the exception WAL
            callback->hash = get_hash_for_item(callback->item);
            callback->slab = get_slab_with_index(ctx, callback->hash, &slab_index);

            callback->migration = 0;
            callback->page_idx = get_slab_page_idx(callback);

            while (callback->index_added == 0) {
                // printf("Wait for index added!\n");
                NOP10();
            }

            __sync_fetch_and_add(&total_flushed_kv_num, 1);

            if ((++kv_cnt_round & 0xFFFFF) == 0) {
                record_seq_id(callback, file);
                kv_cnt_round = 0;
            }

            item_being_written_added = 0;
            if (__sync_fetch_and_add(&ctx->rehash_idx, 0) != slab_index) {
                __sync_fetch_and_add(&callback->slab->item_being_written, 1);
                item_being_written_added = 1;
            }

            bit_set = test_bit_set(callback->slab->page_bitmap, callback->page_idx);
            if (bit_set || item_being_written_added == 0 || __sync_fetch_and_add(&ctx->rehash_idx, 0) == slab_index) {
                if (bit_set) {
                    ++conflict_page_kv;
                    if ((conflict_page_kv & 0xFFF) == 0) {
                        // printf("Current conflict page KV (write to exception wal) num = %d (all workers)\n", conflict_page_kv.load());
                    }
                } else {
                    // printf("Being rehashed!\n");
                }

                conflict_kv_add_async(ctx, callback);

#ifdef KEY_TRACE
                struct item_metadata *meta = (struct item_metadata *)callback->item; 
                printf("key = %ld has been rewritten to wal during flush during conflict!\n", *(uint64_t *)(meta + 1));
#endif


                if (!bit_set) {
                    bit_reset(callback->slab->page_bitmap, callback->page_idx);
                }
        
                // except_wal_write(callback->slab, callback->item);
                // Remove this item from WAL index since io_cb is unavailable
                // remove_from_wal_index(ctx, (struct item_metadata *)callback->item);
                free_io_slot(ctx->io_slot_map, slot_idx);
                --ctx->busy_read_slot;
                
                if (item_being_written_added) {
                    __sync_fetch_and_sub(&callback->slab->item_being_written, 1);
                }

                __sync_fetch_and_sub(&ctx->kv_pending, 1);
                continue;
            }
            // Get a slot and submit io
            callback->fd = callback->slab->fd_old;
            add_item_async(callback);
        }

        // Wait all other IOs to be finished
        check_finished_io(ctx->io_ctx, &ctx->busy_read_slot, &ctx->busy_write_slot, queue_depth, ctx->io_slot_map, 1, 1);
        assert(ctx->busy_read_slot == 0 && ctx->busy_write_slot == 0);

        // If rehash needed, add slab to task queue
        schedule_for_rehash(ctx, 1);
   
        mark_wal_idx = __sync_fetch_and_add(&ctx->buf_elem[buf_flush_id].file_idx, 0);
        if (mark_wal_idx != INVALID_64_BIT) {
            del_wal_elem = (del_wal_elem_t *)malloc(sizeof(del_wal_elem_t));
            sprintf(file, "%s/%d-%ld.wal", ctx->path, ctx->worker_id, mark_wal_idx);
            strcpy(del_wal_elem->path, file);
            del_wal_elem->ctx = ctx;

            del_wal_ctx = &(del_wal_contexts[ctx->worker_id / ((nb_workers + DEL_WAL_FILE_THREAD - 1) / DEL_WAL_FILE_THREAD)]);
            pthread_mutex_lock(&(del_wal_ctx->lock));
            ++del_wal_ctx->del_wal_file_cnt;
            del_wal_elem->next = del_wal_ctx->head->next;
            del_wal_ctx->head->next = del_wal_elem;
            pthread_mutex_unlock(&(del_wal_ctx->lock));
            pthread_cond_signal(&(del_wal_ctx->ready));
        }

        __sync_lock_test_and_set(&ctx->buf_elem[buf_flush_id].file_idx, INVALID_64_BIT);

        ctx->idle = 1;
        worker_idle[ctx->worker_id] = 1;
    }

    return NULL;
}

void clean_dir(char *path) {
    char *name;
	DIR *dir;
	struct dirent *entry = NULL;
    char file_name[MAX_FILE_NAME_LEN];

    dir = opendir(path);
    if (dir != NULL) {
        while ((entry = readdir(dir))) {
            name = basename(entry->d_name);
            sprintf(file_name, "%s/%s", path, name);
            remove(file_name);
        }
    }
}

// For safety, only delete files we recognize
void clean_db() {
    int i, len;
    char *name;
	DIR *dir;
	struct dirent *entry = NULL;
    char file_name[MAX_FILE_NAME_LEN];
    struct stat buf;
    int result;

    for (i = 0; i < nb_paths; i++) {
	    dir = opendir(db_path[i]);
        if (dir != NULL) {
            while ((entry = readdir(dir))) {
                name = basename(entry->d_name);
                sprintf(file_name, "%s/%s", db_path[i], name);
                result = stat(file_name, &buf);

	            len = strlen(file_name);
                
	            if (!strcmp(file_name + len - 4, ".wal") ||
                    !strcmp(file_name + len - 5, ".slab") ||
                    !strcmp(file_name + len - 7, ".except") ||
                    !strcmp(file_name + len - 4, "META") ||
                    !strcmp(file_name + len - 3, "SEQ")) {
                    remove(file_name);
                } else if (!strcmp(file_name + len - 10, "rocks_meta")) {
                    clean_dir(file_name);
                    rmdir(file_name);
                }
            }
        }
    }
}

uint32_t power_2(uint64_t size_src, uint64_t size_des) {
    uint32_t idx = 0;

    while (size_src < size_des) {
        ++idx;
        size_src <<= 1;
        assert(size_src <= size_des);
    }

    return idx;
}

void get_db_meta_info(char *path, uint32_t *worker_num, uint32_t **wal_id_array) {
	char *name;
	int len, ret, fd;
	int worker_id, wal_id;
	struct dirent *entry = NULL;
	char file_name[MAX_FILE_NAME_LEN];
	FILE *fp = NULL;
	uint32_t slab_id, idx, rehash_radix;
    struct slab *s;
    uint64_t file_size, read_size, item_size, off;
    struct item_metadata meta;
	DIR *dir;
    char *buf = NULL;

    buf = (char *)valloc(page_size);

	dir = opendir(path);
	if (dir == NULL) {
        die("DB path %s seems wrong!\n", path);
    }

	while ((entry = readdir(dir))) {
	    name = basename(entry->d_name);
	    len = strlen(name);

        if (len <= 4) {
            continue;
        }

	    if (!strcmp(name + len - 4, ".wal")) {
            sscanf(name, "%d-%d.wal", &worker_id, &wal_id);
            assert(worker_id < MAX_WORKER_NUM);
            if (worker_id > *worker_num) {
                *worker_num = worker_id;
            }
            if (wal_id < wal_id_array[worker_id][0]) {
                wal_id_array[worker_id][0] = wal_id;
            }
            if (wal_id > wal_id_array[worker_id][1]) {
                wal_id_array[worker_id][1] = wal_id;
            }
        } else if (!strcmp(name + len - 5, ".slab")) {
            sscanf(name, "%d-%d.slab", &slab_id, &idx);
            sprintf(file_name, "%s/%d-%d.slab", path, slab_id, idx);
            file_size = get_file_size(file_name);
            s = slabs[slab_id];
            rehash_radix = power_2(s->init_num_pages * (uint64_t)page_size, file_size);

            fd = open(file_name, O_RDWR|O_DIRECT, 0666);
            pthread_spin_lock(&(s->meta_lock));
            s->old_ref = 0;
            s->fd_old = fd;
            s->idx_old = idx;
            s->fd_new = -1;
            s->rehash_radix = rehash_radix;
            pthread_spin_unlock(&(s->meta_lock));
        } else if (!strcmp(name + len - 7, ".except")) {
            sscanf(name, "%d-%d.except", &slab_id, &idx);
            sprintf(file_name, "%s/%d-%d.except", path, slab_id, idx);
            s = slabs[slab_id];
            pthread_mutex_lock(&s->except_wal_lock);
            s->except_fp_new = fopen(file_name, "a+");
            s->except_wal_index = idx + 1;
            s->except_wal_size = get_file_size(file_name);
            s->synced_except_wal_size = s->except_wal_size;
            s->except_offset = s->except_wal_size;
            pthread_mutex_unlock(&s->except_wal_lock);

            // Build index for exception WAL
            off = 0;
            while (off < s->except_wal_size) {
                fseek(s->except_fp_new, off, SEEK_SET);
		        ret = fread(&meta, 1, sizeof(struct item_metadata), s->except_fp_new);

                if (ret != sizeof(struct item_metadata)) {
                    printf("Early exit from exception WAL when recovering DB!\n");
                    break;
                }
    
                fseek(s->except_fp_new, off, SEEK_SET);
                ret = fread(buf, 1, sizeof(meta) + meta.key_size, s->except_fp_new);
                /*
                assert(ret == sizeof(meta) + meta.key_size);

                insert_into_except_index(s, buf, off, idx);
            
                item_size = sizeof(meta) + meta.key_size + meta.value_size;   
                off += item_size;
                */
                if (ret == sizeof(meta) + meta.key_size) {
                    insert_into_except_index(s, buf, off, idx);
                    item_size = sizeof(meta) + meta.key_size + meta.value_size;
                    off += item_size;
                } else {
                    printf("It seems data has been corrupted; very likely the program did not exit correctly!\n");
                    // Truncate the file
                    ret = ftruncate(fileno(s->except_fp_new), off);
                    assert(ret == 0);
                    pthread_mutex_lock(&s->except_wal_lock);
                    s->except_wal_size = off;
                    s->synced_except_wal_size = off;
                    s->except_offset = s->except_wal_size;
                    pthread_mutex_unlock(&s->except_wal_lock);
                    break;
                }
            }
            fseek(s->except_fp_new, 0, SEEK_END);
        } else {
            // printf("Unknown file %s in the DB path!\n", name);
        }
    }
    
    closedir(dir);
    free(buf);

    *worker_num += 1;
}

void *flush_wal_in_recovery(void *input) {
    recovery_context_t *ctx = (recovery_context_t *)input;
    uint32_t i, worker_id, start_idx, end_idx;
    char file_name[MAX_FILE_NAME_LEN];
    int fd, ret;
    FILE *fp;
    struct item_metadata *meta;
    uint64_t off, file_size;
    char *buf, *page_buf, *page_buf_tmp;
    struct slab_callback *callback;

    worker_id = ctx->worker_id;
    start_idx = ctx->wal_start_idx; 
    end_idx = ctx->wal_end_idx; 
    buf = (char *)calloc(1, page_size);
    // printf("Page size in wal flush = %d, start idx = %d, end idx = %d\n", page_size, start_idx, end_idx);
    callback = (struct slab_callback *)malloc(sizeof(struct slab_callback));
    page_buf = (char *)valloc(page_size);

    // Use sync IO to do the flush job
    // Update the corresponding slab and the exception WAL 
    for (i = start_idx; i <= end_idx; i++) {
        sprintf(file_name, "%s/%d-%d.wal", ctx->path, worker_id, i);
        file_size = get_file_size(file_name);
        fp = fopen(file_name, "rb");
        fseek(fp, 0, SEEK_SET);
        if (fp == NULL) {
            die("Open file %s failed during recovery!\n", file_name);
        }
        
        off = 0;
        while (off < file_size) {
            ret = fread(buf, 1, sizeof(struct item_metadata), fp);

            if (ret != sizeof(struct item_metadata)) {
                printf("Error: ret = %d and should be %d\n", ret, sizeof(struct item_metadata));
                break;
            }

            meta = (struct item_metadata *)buf;
            assert(ret == sizeof(struct item_metadata));
            assert(ret + meta->key_size + meta->value_size <= page_size);
            off += ret;

            ret = fread(buf + ret, 1, meta->key_size + meta->value_size, fp);
            if (ret != meta->key_size + meta->value_size) {
                printf("Error: ret = %d and should be %d\n", ret, meta->key_size + meta->value_size);
                break;
            }
            off += ret;

            // Insert item into slab or exception WAL
            memset(callback, 0, sizeof(struct slab_callback));
            callback->item = buf;
            callback->migration = 0;
            callback->hash = get_hash_for_item(callback->item);
            callback->slab = get_slab_with_abs_index(callback->hash);
            callback->page_idx = get_slab_page_idx(callback);
            callback->fd = callback->slab->fd_old;
            callback->page_buffer = page_buf;

            add_item_sync(callback);
        }

        fclose(fp);

        // printf("Will remove %s in recovery!\n", file_name);
        remove(file_name);
    }

    free(page_buf);
    free(callback);
    free(buf);

    return NULL;
}

void recover_db() {
    uint32_t i;
    uint32_t worker_num = 0;
    uint32_t **wal_id_array;
    pthread_t t[MAX_WORKER_NUM];
    recovery_context_t *ctx = NULL;

    wal_id_array = (uint32_t **)malloc(sizeof(uint32_t *) * MAX_WORKER_NUM);
    for (i = 0; i < MAX_WORKER_NUM; i++) {
        wal_id_array[i] = (uint32_t *)malloc(sizeof(uint32_t) * 2);
        wal_id_array[i][0] = INVALID_32_BIT;
        wal_id_array[i][1] = 0;
    }

    for (i = 0; i < nb_paths; i++) {
        get_db_meta_info(db_path[i], &worker_num, wal_id_array);
    }

    // slab info has been rebuilt; flush WAL then
    printf("Previous worker number = %d\n", worker_num);
    ctx = (recovery_context_t *)calloc(sizeof(recovery_context_t), worker_num);
    for (i = 0; i < worker_num; i++) {
        strcpy(ctx[i].path, db_path[i / (worker_num / nb_paths)]);
        ctx[i].worker_id = i;
        ctx[i].wal_start_idx = wal_id_array[i][0];
        ctx[i].wal_end_idx = wal_id_array[i][1];
        pthread_create(&t[i], NULL, flush_wal_in_recovery, &ctx[i]);
    }
    
    for (i = 0; i < worker_num; i++) {
        pthread_join(t[i], NULL);
    }

    free(ctx);
    for (i = 0; i < MAX_WORKER_NUM; i++) {
        free(wal_id_array[i]);
    }
    free(wal_id_array);

    printf("DB has been recovered!\n");
}

void slab_workers_init() {
    int idx, i, w;
    struct slab_context *ctx = NULL;
    uint32_t *worker_ids;
    // int *slab_items;
    pthread_t t;
    int nb_workers_per_path = nb_workers / nb_paths;
    uint32_t bucket_num, _nb_paths;
    char file_name[MAX_FILE_NAME_LEN];
    FILE *fp;

    if (create_db) {
        clean_db();
        for (i = 0; i < nb_paths; i++) {
            sprintf(file_name, "%s/META", db_path[i]);
            fp = fopen(file_name, "w");
            if (!fp) {
                die("Meta file %s open error when writing!\n", file_name);
            }
            fprintf(fp, "%d %d %ld %d %ld", nb_paths, nb_slabs, slab_size, page_size, cur_seq_id);
            fdatasync(fileno(fp));
            fclose(fp);

            sprintf(file_name, "%s/SEQ", db_path[i]);
            fp = fopen(file_name, "w");
            if (!fp) {
                die("Meta file %s open error when writing!\n", file_name);
            }
            fprintf(fp, "%ld", cur_seq_id);
            fdatasync(fileno(fp));
            fclose(fp);
        }
    } else {
        sprintf(file_name, "%s/META", db_path[0]);
        fp = fopen(file_name, "r");
        if (!fp) {
            die("Meta file %s open error when reading!\n", file_name);
        }
        fscanf(fp, "%d %d %ld %d", &_nb_paths, &nb_slabs, &slab_size, &page_size);
        fclose(fp);

        sprintf(file_name, "%s/SEQ", db_path[0]);
        fp = fopen(file_name, "r");
        if (!fp) {
            die("Meta file %s open error when reading!\n", file_name);
        }
        fscanf(fp, "%ld", &cur_seq_id);
        fclose(fp);
        printf("Stored seq id = %ld\n", cur_seq_id);
        cur_seq_id += (1024 * 1024 * 1024);

        printf("Meta info read: nb_paths = %d, nb_slabs = %d, slab_size = %ld, page_size = %d, input nb_paths = %d\n", _nb_paths, nb_slabs, slab_size, page_size, nb_paths);
        assert(nb_paths == _nb_paths);
    }

    exp_num = 0;
    nb_slabs_per_worker = (nb_slabs + nb_workers - 1) / nb_workers;
    nb_slabs = nb_slabs_per_worker * nb_workers;

    nb_pages_per_slab = slab_size / nb_slabs / page_size;
    assert(nb_pages_per_slab > 0);
    bucket_num = get_bucket_num();

    printf("slab size = %ld, nb_workers = %d, nb_workers_per_path = %d, nb_slabs_per_worker = %d, nb_pages_per_slab = %d, nb_slabs = %d\n", slab_size, nb_workers, nb_workers_per_path, nb_slabs_per_worker, nb_pages_per_slab, nb_slabs);

    worker_ids = (uint32_t *)malloc(nb_workers * sizeof(uint32_t));
    memset(worker_ids, 0, nb_workers * sizeof(uint32_t));

    slabs = (struct slab **)calloc(nb_slabs, sizeof(struct slab *));
    nb_slabs_per_path = (nb_slabs + nb_paths - 1) / nb_paths;

    printf("nb_slabs_per_path = %d\n", nb_slabs_per_path);

    for (i = 0; i < nb_slabs; i++) {
        slabs[i] = create_slab(db_path[i/nb_slabs_per_path], i);
    }

    if (!create_db) {
        recover_db();
    }

    slab_contexts = (struct slab_context *)calloc(nb_workers, sizeof(*slab_contexts));
    for (w = 0; w < nb_workers; w++) {
        ctx = &slab_contexts[w];
        ctx->worker_id = w;
        sprintf(ctx->path, "%s", db_path[w/nb_workers_per_path]);

        pthread_create(&t, NULL, worker_slab_init, ctx);
    }

    rehash_contexts = (struct rehash_worker *)calloc(nb_rehash, sizeof(*rehash_contexts));
    printf("nb_rehash = %d\n", nb_rehash);
    for (idx = 0; idx < nb_rehash; idx++) {
        rehash_contexts[idx].worker_id = idx;
        pthread_create(&t, NULL, worker_rehash, &(rehash_contexts[idx]));
    }

    del_wal_contexts = (del_wal_context_t *)calloc(DEL_WAL_FILE_THREAD, sizeof(del_wal_context_t));
    printf("DEL_WAL_FILE_THREAD = %d\n", DEL_WAL_FILE_THREAD);
    for (idx = 0; idx < DEL_WAL_FILE_THREAD; idx++) {
        del_wal_contexts[idx].worker_id = idx;
        pthread_create(&t, NULL, worker_del_wal, &(del_wal_contexts[idx]));
    }
    
    if (scan) {
        sprintf(file_name, "%s/rocks_meta/", db_path[0]);
        init_rocks(file_name);
        init_rocks_buf();
    
        // printf("start to init rocks\n");
        rocks_ctx = (rocks_context_t *)calloc(ROCKS_THREADS, sizeof(rocks_context_t));
        for (idx = 0; idx < ROCKS_THREADS; idx++) {
            rocks_ctx[idx].worker_id = idx;
            pthread_create(&t, NULL, worker_rocks, &(rocks_ctx[idx]));
        }
        while (*(volatile int*)&nb_rocks_workers_ready != ROCKS_THREADS) {
            NOP10();
        }
        // printf("rocks init finished\n");
    }

    while (*(volatile int*)&nb_workers_ready != nb_workers) {
        NOP10();
    }
    while (*(volatile int*)&nb_rehash_workers_ready != nb_rehash) {
        NOP10();
    }
    while (*(volatile int*)&nb_del_wal_workers_ready != DEL_WAL_FILE_THREAD) {
        NOP10();
    }
}

void kallaxdb_init() {
    slab_workers_init();
}

// TODO
size_t get_database_size(void) {
    return 0;
}
