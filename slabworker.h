#ifndef SLAB_WORKER_H
#define SLAB_WORKER_H 1

#include "pagecache.h"
#include <stdint.h>

struct slab_callback;
struct slab_context;

slice *kv_read_sync(struct slab_callback *callback);
void kv_add_sync(struct slab_callback *callback);
void kv_update_sync(struct slab_callback *callback);
void kv_read_async(struct slab_callback *callback);
slice **kv_multi_read(struct slab_callback **callback, uint32_t number);
void kv_add_async(struct slab_callback *callback);
void kv_update_async(struct slab_callback *callback);
void kv_remove_async(struct slab_callback *callback);

void kv_put(slice *key, slice *value);
void kv_update(slice *key, slice *value);
void kv_delete(slice *key);
slice *kv_get(slice *key);

void wal_open(struct slab_context *ctx);
void wal_write(struct slab_context *ctx, struct slab_callback *callback, uint8_t has_seq_id, uint8_t *flush_needed);

uint32_t except_wal_open(struct slab *s, uint8_t has_old);
void reload_except_wal(struct slab *s);
void except_wal_write(struct slab *s, char *item);

void read_from_except_wal(struct slab *s, char *item, slice *value);
void read_from_wal_tree(struct slab_context *ctx, char *item, slice *value);

typedef struct index_scan tree_scan_res_t;
tree_scan_res_t kv_init_scan(char *item, size_t scan_size);
void kv_read_async_no_lookup(struct slab_callback *callback, struct slab *s, size_t slab_idx);

size_t get_database_size(void);


void kallaxdb_init(void);
//void slab_workers_init(int nb_disks, int nb_workers_per_disk);
void slab_workers_init();
int get_nb_workers(void);
char *kv_read_item_sync(char *item); // Unsafe
struct pagecache *get_pagecache(struct slab_context *ctx);
struct io_context *get_io_context(struct slab_context *ctx);
uint64_t get_rdt(struct slab_context *ctx);
void set_rdt(struct slab_context *ctx, uint64_t val);
int get_worker(struct slab *s);
int get_nb_disks(void);
size_t get_item_size(char *item);

char* read_from_wal_hash(struct slab_context *ctx, char *item);

void remove_from_wal_tree(struct slab_context *ctx, struct item_metadata *meta);
void remove_from_wal_hash(struct slab_context *ctx, struct item_metadata *meta);
void remove_from_wal_index(struct slab_context *ctx, struct item_metadata *meta);

// For scan operation
void iterator_init();
void Seek_internal(char *buf, uint32_t size);
void Seek(slice *key);
void SeekToFirst();
int Valid();
void GetKey(char *buf, uint32_t *size);
void Next();
void iterator_del();

#endif
