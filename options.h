#ifndef OPTIONS_H
#define OPTIONS_H

#define DEBUG 0
#define PINNING 0 //xubin
#define PATH "/scratch%lu/tenvisdb/slab-%d"

/* In memory structures */
#define RBTREE 0
#define RAX 1
#define ART 2
#define BTREE 3
#define ASYNC_IO 1
#define DISABLE_WAL 0

#define WAL_BUFFER_SIZE (1024*1024*1)
#define REAL_LOCK
#define WAL_IO_BUFFER_SIZE (1024*1024)
#define EXCEPT_WAL_IO_BUFFER_SIZE (1024*1024)
#define BITMAP_BYTES (1024)

#define REHASH_PAGES (32)
#define THROTTLE_STEP (10 * 1024 * 1024)

#define REHASH_QUEUE_DEPTH (REHASH_PAGES)

#define MAX_PATH_LEN (512)
#define WAL_SYNC_SIZE (1024 * 64)
#define WAL_FILE_SIZE (1024 * 1024 * 64)

#define REHASH_RATIO (0.08)
#define KV_NUM_GAP (1024 * 5120LU)
#define MEMORY_INDEX BTREE
#define PAGECACHE_INDEX BTREE

#define KV_BUF_NUM (4)
#define KV_NUM_IN_BUF_LIMIT (1024)

#define USE_WAL_TREE (1)

/* Queue depth management */
#define QUEUE_DEPTH 1
#define MAX_NB_PENDING_CALLBACKS_PER_WORKER (4*QUEUE_DEPTH)
#define NEVER_EXCEED_QUEUE_DEPTH 1 // Never submit more than QUEUE_DEPTH IO requests simultaneously, otherwise up to 2*MAX_NB_PENDING_CALLBACKS_PER_WORKER (very unlikely)
#define WAIT_A_BIT_FOR_MORE_IOS 0 // If we realize we don't have QUEUE_DEPTH IO pending when submitting IOs, check again if new incoming requests have arrived. Boost performance a tiny bit for zipfian workloads on AWS, but really not worthwhile

/* Page cache */
#define PAGE_CACHE_SIZE (4096) //3GB
#define MAX_PAGE_CACHE (PAGE_CACHE_SIZE / PAGE_SIZE)
#define MAX_NB_PENDING_CALLBACKS_PER_WORKER (4*QUEUE_DEPTH)

#define EXCEPTION_WAL_SYNC_SIZE (64 * 1024)

#define INVALID_8_BIT   (0xFF)
#define INVALID_32_BIT   (0xFFFFFFFF)
#define INVALID_64_BIT  (0xFFFFFFFFFFFFFFFF)

#define INVALID_LEN (0x3FFFFFFF)

#define MAX_FILE_NAME_LEN (512)

#define MIN_BUCKET_NUM (1024)
#define MAX_BUCKET_NUM (1024 * 128)

#define MAX_WORKER_NUM (512)
#define MAX_REHASH_NUM (512)

#define MAX_READS (8)
#define ROCKS_THREADS (1)
#define WORKERS_PER_ROCKS_BUF_GROUP (2)
#define ROCKS_BUF_NUM_PER_GROUP (8)
#define ROCKS_BUF_LEN (4 * 1024)

#define DEL_WAL_FILE_THREAD (1)

#define DEFAULT_CREATE_DB 1
#define DEFAULT_RUN_TEST 1
#define DEFAULT_DISK_NUM 1
#define DEFAULT_WORKER_NUM 1
#define DEFAULT_SLABS_PER_WORKER 1
#define DEFAULT_REHASH_NUM 1

#define DEFAULT_LOAD_KV_NUM (40000000LU)
#define DEFAULT_REQUEST_KV_NUM (20000000LU)

#define DEFAULT_LOADER_NUM 1
#define DEFAULT_CLIENT_NUM 1

#define DEFAULT_KEY_SIZE 16
#define DEFAULT_VALUE_SIZE 200

#define DEFAULT_SLAB_SIZE (1)
#define DEFAULT_PAGE_SIZE (4096LU)

#define DEFAULT_PATH_NUM (0)
#define DEFAULT_DB_PATH "null"

#define DEFAULT_DB_BENCH 0
#define DEFAULT_DB_BENCH_COUNT 1
#define DEFAULT_QUEUE_DEPTH (64)

#define DEFAULT_IS_VAR (0)
#define DEFAULT_SCAN (1)

#endif
