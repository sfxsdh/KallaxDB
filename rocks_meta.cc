#include "rocks_meta.h"
#include "items.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace std;
using namespace rocksdb;

static DB *db;
static Options options;
static rocksdb::WriteOptions write_options;

void init_rocks(char *path) {
    options.create_if_missing = true;
    options.max_background_jobs = 8;
    Status status = DB::Open(options, path, &db);
    assert(status.ok());
    write_options.disableWAL = false;
    printf("RocksDB open success!\n");
}

void write_into_rocks(char *item, uint64_t key_size, uint64_t value_size) {
    // Insert and update are equivalent
    // Note Delete operation
    Status status;

    Slice key(item, key_size);

    if (value_size > 0) {
        status = db->Put(write_options, key, "");
    } else {
        status = db->Delete(write_options, key);
    }

    if (!status.ok()) {
        printf("Failed writing into RocksDB!\n");    
    }
}

void write_into_rocks_batch(char *item, uint64_t len) {
    Status status;
    WriteBatch batch;
    char *ptr = item;
    uint64_t key_size, value_size;
    struct item_metadata *meta = (struct item_metadata *)item;

    while (ptr + sizeof(*meta) < item + len) {
        meta = (struct item_metadata *)ptr;
        key_size = meta->key_size;
        if (key_size == 0) {
            break;
        }
        value_size = meta->value_size;
        ptr += sizeof(*meta);
        Slice key(ptr, key_size);

        // printf("batch key = %ld inserted!\n", *((uint64_t *)ptr));

        Slice value("");
        ptr += key_size;

        if (value_size > 0) {
            batch.Put(key, value);
        } else {
            batch.Delete(key);
        }
    }

    status = db->Write(write_options, &batch);
    if (!status.ok()) {
        printf("Failed writing batch into RocksDB!\n");    
    }
}

void *rocks_iterator_init() {
    return (void *)db->NewIterator(rocksdb::ReadOptions());
}

void rocks_Seek(void *iter, char *buf, uint32_t size) {
    Slice start(buf, size);
    ((rocksdb::Iterator *)iter)->Seek(start);
}

void rocks_SeekToFirst(void *iter) {
    ((rocksdb::Iterator *)iter)->SeekToFirst();
}

int rocks_Valid(void *iter) {
    return ((rocksdb::Iterator *)iter)->Valid() ? 1 : 0;
}

char *rocks_GetKey(void *iter, uint32_t *size) {
    char *ret = NULL;
    rocksdb::Iterator *it = (rocksdb::Iterator *)iter;

    /*
    uint8_t *buf = (uint8_t *)malloc(64);;

    memcpy(buf, it->key().data(), it->key().size());
    free(buf);
    printf("key = ");
    for (int i = 0; i < it->key().size(); i++) {
        printf("%x ", (uint32_t)buf[i]);   
    }
    printf("\n");
    */
    *size = it->key().size();
    ret = (char *)it->key().data();

    return ret;
}

void rocks_Next(void *iter) {
    ((rocksdb::Iterator *)iter)->Next();
}

void rocks_iterator_del(void *iter) {
    delete((rocksdb::Iterator*)iter);
}
