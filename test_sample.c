#include "headers.h" 

#define KV_NUM (1024)

int main(int argc, char **argv) {
    char config_path[512];
    int use_default_config_file = 1;
    long i, j, kv_cnt;
    slice key, value, *value_read = NULL, key_scan;
    int key_len = 8;
    int value_len = 400;
    int ret;
    char buf_prev[value_len], buf_update[value_len];
    char key_scan_buf[1024];
    uint32_t size;

    for (i = 1; i < argc; ++i) {
        config_option option = parse_option(argv[i]);
        if (!strcmp(option.name, "config_path")){
            strcpy(config_path, option.value);
            use_default_config_file = 0;
            break;
        }
    }

    if (use_default_config_file) {
        strcpy(config_path, "./kallaxdb.cfg");
        if (access(config_path, 0)) {
            printf("No configuration file specified!\n");
            exit(-1);
        }
    }

    parse_config_file(config_path);
    
    kallaxdb_init();

    for (i = 0; i < value_len; i++) {
        buf_prev[i] = rand();
        buf_update[i] = rand();
    }

    // Insert
    printf("Insertion started...\n");
    key.data = (char *)malloc(1024);
    value.data = (char *)malloc(1024);
    for (i = 0; i < KV_NUM; i++) {
        key.len = key_len;
        *(long *)key.data = (long)i;
        value.len = value_len;
        memcpy(value.data, buf_prev, value_len);

        kv_put(&key, &value);
    }
    printf("Insertion finished.\n");

    // Update
    printf("Update started...\n");
    for (i = 0; i < KV_NUM; i++) {
        key.len = key_len;
        *(long *)key.data = (long)i;
        value.len = value_len;
        memcpy(value.data, buf_update, value_len);

        kv_update(&key, &value);
    }
    printf("Update finished.\n");

    // Read
    // Need to release memory for value
    printf("Read started...\n");
    for (i = 0; i < KV_NUM; i++) {
        key.len = key_len;
        *(long *)key.data = (long)i;

        value_read = kv_get(&key);

        // Value should have been updated
        assert(value_read->len == value_len);
        assert(memcmp(value_read->data, buf_update, value_len) == 0);

        if (value_read->data) {
            free(value_read->data);
            value_read->data = NULL;
        }
        if (value_read) {
            free(value_read);
            value_read = NULL;
        }
    }
    printf("Read finished.\n");

    // Scan all KVs
    // 1. Init an iterator
    // 2. Tranverse the iterator
    // 3. Release the iterator
    kv_cnt = 0;
    iterator_init();
    for (SeekToFirst(); Valid(); Next()) {
        // Suppose key_scan_buf is large enough
        GetKey(key_scan_buf, &size);
        key_scan.len = size;
        key_scan.data = key_scan_buf;
        value_read = kv_get(&key_scan);
        if (value_read->data) {
            assert(value_len == value_read->len);
            assert(memcmp(value_read->data, buf_update, value_read->len) == 0);
            free(value_read->data);
            value_read->data = NULL;
            ++kv_cnt;
        } else {
            printf("Error!\n");
        }

        if (value_read) {
            free(value_read);
            value_read = NULL;
        }
    }
    iterator_del();
    printf("Total %ld KVs in the table before deletion!\n", kv_cnt);

    // Delete
    printf("Delete started...\n");
    for (i = 0; i < KV_NUM; i+=2) {
        key.len = key_len;
        *(long *)key.data = (long)i;

        kv_delete(&key);
    }
    printf("Delete finished.\n");

    // Scan
    kv_cnt = 0;
    iterator_init();
    key.len = key_len;
    *(long *)key.data = (long)0;
    for (Seek(&key); Valid(); Next()) {
        GetKey(key_scan_buf, &size);
        key_scan.len = size;
        key_scan.data = key_scan_buf;
        value_read = kv_get(&key_scan);
        if (value_read->data) {
            assert(value_len == value_read->len);
            assert(memcmp(value_read->data, buf_update, value_read->len) == 0);
            free(value_read->data);
            value_read->data = NULL;
            ++kv_cnt;
        }
        if (value_read) {
            free(value_read);
            value_read = NULL;
        }
    }
    iterator_del();
    printf("Total %ld KVs in the table after deletion!\n", kv_cnt);

    free(key.data);
    free(value.data);

    return 0;
}
