/*
 * YCSB Workload
 */

#include "headers.h"
#include "workload-common.h"
#include "options.h"
#include "slabworker.h"

#define VAR_LIMIT 0.3
extern size_t key_size;
extern size_t value_size;
extern int g_is_var;
extern uint32_t page_size;
extern int scan;
extern uint64_t load_kv_num;

static char *_create_unique_item_ycsb(uint64_t uid, int update) {
   size_t v_size = update == 0 ? 0 : value_size;
   size_t item_size = sizeof(struct item_metadata) + key_size + v_size;
   return create_unique_item(item_size, uid);
}

static char *create_unique_item_ycsb(uint64_t uid, int update, int is_var) {
    size_t v_size = update == 0 ? 0 : value_size;
    size_t item_size = 0;
    if (is_var) {
        double var_ratio = VAR_LIMIT * (uniform_next() % 100) / 100.0;
        int p_n = (uniform_next() % 2) == 0 ? -1 : 1;
        item_size = sizeof(struct item_metadata) + key_size + v_size * (1 + p_n * var_ratio);
    } else {
        item_size = sizeof(struct item_metadata) + key_size + v_size;
    }
    return create_unique_item(item_size, uid);
}

static slice create_unique_slice_ycsb(uint64_t uid, int update, int is_var) {
    size_t v_size = update == 0 ? 0 : value_size;
    size_t item_size = 0;
    if (is_var) {
        double var_ratio = VAR_LIMIT * (uniform_next() % 100) / 100.0;
        int p_n = (uniform_next() % 2) == 0 ? -1 : 1;
        item_size = sizeof(struct item_metadata) + key_size + v_size * (1 + p_n * var_ratio);
    } else {
        item_size = sizeof(struct item_metadata) + key_size + v_size;
    }

    slice result;
    result.data = create_unique_item(item_size, uid);
    result.len = item_size;
    return result;
}
/* Is the current request a get or a put? */
static int random_get_put(int test) {
   long random = uniform_next() % 100;
   switch(test) {
      case 0: // A F
      case 6:
         return random >= 50;
      case 1: // B D
      case 5:
         return random >= 95;
      case 2: // C
         return 0;
      case 3: // E
         return random >= 95;
      case 4: // WRITE ONLY
         return 1;
   }
   die("Not a valid test\n");
}

/* YCSB A (or D), B, C */
static void _launch_ycsb(int test, size_t tid, int nb_requests, int zipfian, int is_var) {
   declare_periodic_count;
    size_t i;

   for(i = 0; i < nb_requests; i++) {
      struct slab_callback *cb = bench_cb(tid, i);
      if(random_get_put(test)) { // In these tests we update with a given probability
         cb->item = create_unique_item_ycsb(uniform_next(), 1, is_var);
         cb->action = ADD;
         kv_add_async(cb);
      } else { // or we read
         cb->item = create_unique_item_ycsb(uniform_next(), 0, is_var);
         cb->action = READ;
         slice* res  = kv_read_sync(cb);
         free(res->data);
         free(res);
         free_callback(cb, cb->item);
      }

      switch(test) {
      case 0:
          periodic_count_id(1000, tid, "YCSB A Load Injector (%lu%%)", i*100LU/nb_requests);
          break;
      case 4:
          periodic_count_id(1000, tid, "YCSB G Load Injector (%lu%%)", i*100LU/nb_requests);
          break;
      case 1:
          periodic_count_id(1000, tid, "YCSB B Load Injector (%lu%%)", i*100LU/nb_requests);
          break;
      case 2:
          periodic_count_id(1000, tid, "YCSB C Load Injector (%lu%%)", i*100LU/nb_requests);
          break;
      case 5:
          periodic_count_id(1000, tid, "YCSB D Load Injector (%lu%%)", i*100LU/nb_requests);
          break;
      case 6:
          periodic_count_id(1000, tid, "YCSB F Load Injector (%lu%%)", i*100LU/nb_requests);
          break;
      }
   }
}

/* YCSB E */
static void _launch_ycsb_e(int test, size_t tid, int nb_requests, int zipfian) {
   declare_periodic_count;
   random_gen_t rand_next = zipfian?zipf_next:uniform_next;
   size_t i;

   for(i = 0; i < nb_requests; i++) {
      if(random_get_put(test)) { // In this test we update with a given probability
         struct slab_callback *cb = bench_cb(tid, i);
         cb->item = create_unique_item_ycsb(uniform_next(), 1, 0);
         cb->action = ADD;
         kv_add_async(cb);
      } else {  // or we scan
         size_t scan_length = uniform_next() % 100 + 1;
         uint32_t size;
         struct slab_callback *cb = bench_cb(tid, i);
         struct slab_callback *scan_cb = bench_cb(tid, i);
         struct item_metadata *meta = NULL;
         slice* value = NULL;
         char* start_item = create_unique_item_ycsb(uniform_next(), 0, 0);
         cb->item = calloc(1, page_size);
         cb->action = READ;
         meta = (struct item_metadata *)(cb->item);
         char *buf = valloc(page_size);

         scan_cb->action = SCAN;
         size_t j = 0;
         iterator_init();
         add_time_in_payload(scan_cb, 0);
         for (Seek_internal(start_item, key_size); j < scan_length && Valid(); Next()) {
             GetKey(buf, &size);
             meta->seq_id = 0;
             meta->key_size = size;
             meta->value_size = 0;
             memcpy(cb->item + sizeof(*meta), buf, size);
             value = kv_read_sync(cb);
             if (value->data) {
                 free(value->data);
             }
             free(value);
             value = NULL;
             memset(cb->item, 0, sizeof(*meta) + size); 
             ++j;
         }
         if (scan_cb->cb) {
             scan_cb->cb(scan_cb, NULL);
         }
         iterator_del();
         free_callback(cb, cb->item);
         free(scan_cb);
         free(buf);
         free(start_item);
      }
      periodic_count_id(1000, tid, "YCSB E Load Injector (scans) (%lu%%)", i*100LU/nb_requests);
   }
}

static void _launch_ycsb_e_multi_read(int test, size_t tid, int nb_requests, int zipfian) {
    uint32_t size[MAX_READS];
    struct slab_callback *cb_insert;
    struct slab_callback *cb[MAX_READS];
    struct item_metadata *meta[MAX_READS];
    int cnt, j, k;
    size_t i;
    slice **value;
    char* start_item;

    declare_periodic_count;
    random_gen_t rand_next = zipfian?zipf_next:uniform_next;


    for (i = 0; i < MAX_READS; i++) {
        cb[i] = bench_cb(tid, i);
        cb[i]->item = calloc(1, page_size);
        cb[i]->action = READ;
        cb[i]->cb = NULL;
        meta[i] = (struct item_metadata *)(cb[i]->item);
    }
    char *buf = valloc(page_size);

    for(i = 0; i < nb_requests; i++) {
        if(random_get_put(test)) { // In this test we update with a given probability
            cb_insert = bench_cb(tid, i);
            cb_insert->item = create_unique_item_ycsb(uniform_next(), 1, 0);
            cb_insert->action = ADD;
            kv_add_async(cb_insert);
        } else {  // or we scan
            size_t scan_length = uniform_next() % 100 + 1;
            // cb = bench_cb(tid, i);
            // struct item_metadata *meta = NULL;
            // slice* value = NULL;
            start_item = create_unique_item_ycsb(uniform_next(), 0, 0);
            // cb->item = calloc(1, page_size);
            // cb->action = READ;
            // meta = (struct item_metadata *)(cb->item);
            struct slab_callback *scan_cb = bench_cb(tid, i);
            scan_cb->action=SCAN;
            cnt = 0;
            j = 0;
            iterator_init();
            add_time_in_payload(scan_cb, 0);
            for(Seek_internal(start_item, key_size); j < scan_length && Valid(); Next()) {
                GetKey(buf, &size[cnt]);
                meta[cnt]->seq_id = 0;
                meta[cnt]->key_size = size[cnt];
                meta[cnt]->value_size = 0;
                memcpy(cb[cnt]->item + sizeof(struct item_metadata), buf, size[cnt]);

                ++cnt;
                if (cnt < MAX_READS && j < scan_length - 1) {
                    ++j;
                    continue;
                }

                value = kv_multi_read(cb, cnt);

                for (k = 0; k < cnt; k++) {
                    if (value[k]->data) {
                        free(value[k]->data);
                    }
                    free(value[k]);
                    value[k] = NULL;
                    memset(cb[k]->item, 0, sizeof(struct item_metadata) + size[k]);
                }
                free(value);
                value = NULL;
                cnt = 0;
                ++j;
            }

            if (scan_cb->cb) {
                scan_cb->cb(scan_cb, NULL);
            }

            if (cnt > 0) {
                value = kv_multi_read(cb, cnt);
                for (k = 0; k < cnt; k++) {
                    if (value[k]->data) {
                        free(value[k]->data);
                    }
                    free(value[k]);
                    value[k] = NULL;
                    memset(cb[k]->item, 0, sizeof(struct item_metadata) + size[k]);
                }
                free(value);
                value = NULL;
                cnt = 0;
            }


            iterator_del();
            free(start_item);
            free(scan_cb);
        }
        periodic_count_id(1000, tid, "YCSB E Load Injector (scans) (%lu%%)", i*100LU/nb_requests);
    }

    for (i = 0; i < MAX_READS; i++) {
        free_callback(cb[i], cb[i]->item);
    }
    free(buf);
}

static void read_all_load(size_t tid, uint64_t nb_requests) {
    declare_periodic_count;
    uint64_t item_found_num = 0;
    uint64_t i;
    for(i = 0; i < nb_requests; ++i) {
        struct slab_callback *cb = bench_cb(tid, i);
        cb->item = create_unique_item_ycsb(i, 0, g_is_var);
        cb->action = READ;
        slice* res =  kv_read_sync(cb);
        if (res->data && (memcmp(cb->item + sizeof(struct item_metadata), res->data, sizeof(uint64_t)) == 0)) {
            ++item_found_num;
        }
        free_callback(cb, cb->item);
        free(res->data);
        free(res);
        periodic_count_id(1000, tid, "READ ALL LOAD (%lu%%)", i*100LU/nb_requests);
    }
    double success_ratio = (double)item_found_num * 100.0 / (double)nb_requests;
    printf("#READ_ALL_LOAD (%s) result: thread %lu found %lu items (load %lu items) (success ratio: %.2lf%%)\n"
            ,__FUNCTION__, tid, item_found_num, nb_requests, success_ratio);
}

static void read_update_read(size_t tid, uint64_t nb_requests, int is_var) {
    declare_periodic_count;
    uint64_t success_num = 0;
    size_t i;
    for(i = 0; i < nb_requests; ++i) {
        uint8_t is_success = 1;
        // read
        size_t seq = i * 3;
        struct slab_callback *cb = bench_cb(tid, seq);
        uint64_t item_id = uniform_next();
        cb->item = create_unique_item_ycsb(item_id, 0, is_var);
        cb->action = READ;
        slice* res_1 = kv_read_sync(cb);
        if (!res_1->data) {
            is_success = 0;
        }
        free(res_1->data);
        free(res_1);
        free_callback(cb, cb->item);

        // update
        seq = i * 3 + 1;
        cb = bench_cb(tid, seq);
        slice item_2 = create_unique_slice_ycsb(item_id, 1, is_var);
        slice res_2;
        res_2.data = malloc(item_2.len);
        memcpy(res_2.data, item_2.data, item_2.len);
        res_2.len  = item_2.len;

        cb->item = item_2.data;
        cb->action = ADD;
        kv_add_async(cb);

        //read
        seq = i * 3 + 2;
        cb = bench_cb(tid, seq);
        cb->item = create_unique_item_ycsb(item_id, 0, is_var);
        cb->action = READ;
        slice* res_3 = kv_read_sync(cb);
        struct item_metadata *meta = (struct item_metadata *)(res_2.data);
        uint64_t cmp_len = meta->value_size  < res_3->len ? meta->value_size : res_3->len;
        if (memcmp(res_2.data + sizeof(struct item_metadata) + meta->key_size, res_3->data, cmp_len)) {
            is_success = 0;
        }
        if (is_success) {
            ++success_num;
        }
        free(res_2.data);
        free(res_3->data);
        free(res_3);
        free_callback(cb, cb->item);
        periodic_count_id(1000, tid, "read-update-read (%lu%%)", i*100LU / nb_requests);
    }
    double success_ratio = (double)success_num * 100.0 / (double)nb_requests;
    printf("#READ_UPDATE_READ (%s) result: thread %lu test successed %lu items (all %lu items) (success ratio: %.2lf%%)\n"
            ,__FUNCTION__, tid, success_num, nb_requests, success_ratio);
}


static void read_delete_read(size_t tid, uint64_t nb_requests, int is_var) {
    declare_periodic_count;
    uint64_t success_num = 0;
    size_t i;
    for(i = 0; i < nb_requests; ++i) {
        uint8_t is_success = 1;
        size_t seq = i * 3;
        struct slab_callback *cb = bench_cb(tid, seq);
        uint64_t item_id = uniform_next();
        cb->item = create_unique_item_ycsb(item_id, 0, is_var);
        cb->action = READ;
        slice* res_1 = kv_read_sync(cb);
        free(res_1->data);
        free(res_1);
        free_callback(cb, cb->item);

        seq = i * 3 + 1;
        cb = bench_cb(tid, seq);
        cb->item = create_unique_item_ycsb(item_id, 0, is_var);
        cb->action = DELETE;
        kv_remove_async(cb);

        seq = i * 3 + 2;
        cb = bench_cb(tid, seq);
        cb->item = create_unique_item_ycsb(item_id, 0, is_var);
        cb->action = READ;
        slice* res_2 = kv_read_sync(cb);
        if (res_2->data) {
            is_success = 0;
        }
        free(res_2->data);
        free(res_2);
        free_callback(cb, cb->item);
        if (is_success) {
            ++success_num;
        }
        periodic_count_id(1000, tid, "read-delete-read (%lu%%)", i*100LU / nb_requests);
    }
    double success_ratio = (double)success_num * 100.0 / (double)nb_requests;
    printf("#READ_DELETE_READ (%s) result: thread %lu test successed %lu items (all %lu items) (success ratio: %.2lf%%)\n"
            , __FUNCTION__, tid, success_num, nb_requests, success_ratio);
}

static void batch_add_read(size_t tid, uint64_t nb_requests, int is_var) {
    declare_periodic_count;
    uint64_t part_requests = nb_requests / 2;
    uint64_t success_num = 0;
    size_t i;
    for(i = 0; i < part_requests; ++i) {
        size_t seq = i;
        struct slab_callback *cb = bench_cb(tid, seq);
        uint64_t item_id = load_kv_num + tid * (nb_requests) + i;
        cb->item = create_unique_item_ycsb(item_id, 1, is_var);
        cb->action = ADD;
        kv_add_async(cb);
        periodic_count_id(1000, tid, "batch-add-read (%lu%%)", i*100LU / nb_requests);
    }

    for(i = 0; i < part_requests; ++i) {
        size_t seq = part_requests + i;
        struct slab_callback *cb = bench_cb(tid, seq);
        uint64_t item_id = load_kv_num + tid * (nb_requests) + i;
        cb->item = create_unique_item_ycsb(item_id, 0, is_var);
        cb->action = READ;
        slice* res_1 = kv_read_sync(cb);
        if (res_1->data && !memcmp(&item_id, res_1->data, sizeof(uint64_t))){
            success_num += 2;
        }
        free(res_1->data);
        free(res_1);
        free_callback(cb, cb->item);
        periodic_count_id(1000, tid, "batch-add-read (%lu%%)", (i + part_requests)*100LU / nb_requests);
    }
    double success_ratio = (double)success_num * 100.0 / (double)nb_requests;
    printf("#BATCH_ADD_READ (%s) result: thread %lu test successed %lu items (all %lu items) (success ratio: %.2lf%%)\n"
            , __FUNCTION__, tid, success_num, nb_requests, success_ratio);
}

static void batch_update_read(size_t tid, uint64_t nb_requests, int is_var) {
    declare_periodic_count;
    uint64_t part_requests = nb_requests / 2;
    uint64_t success_num = 0;
    size_t i;
    for(i = 0; i < part_requests; ++i) {
        size_t seq = i;
        struct slab_callback *cb = bench_cb(tid, seq);
        uint64_t item_id = tid * (nb_requests) + i;
        cb->item = create_unique_item_ycsb(item_id, 1, is_var);
        struct item_metadata *meta = (struct item_metadata *)(cb->item);
        memset(cb->item + sizeof(struct item_metadata) + meta->key_size + sizeof(uint64_t), 0x10, sizeof(uint64_t));
        cb->action = ADD;
        kv_add_async(cb);
        periodic_count_id(1000, tid, "batch-update-read (%lu%%)", i*100LU / nb_requests);
    }

    char padding[8];
    for(i = 0; i < sizeof(uint64_t); ++i) {
        padding[i] = 0x10;
    }

    for(i = 0; i < part_requests; ++i) {
        size_t seq = part_requests + i;
        struct slab_callback *cb = bench_cb(tid, seq);
        uint64_t item_id = tid * (nb_requests) + i;
        cb->item = create_unique_item_ycsb(item_id, 0, is_var);
        cb->action = READ;
        slice* res_1 = kv_read_sync(cb);
        if (res_1->data && !memcmp(&item_id, res_1->data, sizeof(uint64_t)) && !memcmp(padding, res_1->data + sizeof(uint64_t), sizeof(uint64_t))) {
            success_num += 2;
        }
        if (res_1->data) {
            free(res_1->data);
        }
        free(res_1);
        free_callback(cb, cb->item);
        periodic_count_id(1000, tid, "batch-update-read (%lu%%)", (i + part_requests)*100LU / nb_requests);
    }
    double success_ratio = (double)success_num * 100.0 / (double)nb_requests;
    printf("#BATCH_UPDATE_READ (%s) result: thread %lu test successed %lu items (all %lu items) (success ratio: %.2lf%%)\n"
            , __FUNCTION__, tid, success_num, nb_requests, success_ratio);
}

static void batch_delete_read(size_t tid, uint64_t nb_requests, int is_var) {
    declare_periodic_count;
    uint64_t part_requests = nb_requests / 2;
    uint64_t success_num = 0;
    size_t i;

    for(i = 0; i < part_requests; ++i) {
        size_t seq = i;
        struct slab_callback *cb = bench_cb(tid, seq);
        uint64_t item_id = tid * (nb_requests) + i;
        cb->item = create_unique_item_ycsb(item_id, 0, is_var);
        cb->action = DELETE;
        kv_remove_async(cb);
        periodic_count_id(1000, tid, "batch-delete-read (%lu%%)", i*100LU / nb_requests);
    }

    for(i = 0; i < part_requests; ++i) {
        size_t seq = part_requests + i;
        struct slab_callback *cb = bench_cb(tid, seq);
        uint64_t item_id = tid * (nb_requests) + i;
        cb->item = create_unique_item_ycsb(item_id, 0, is_var);
        cb->action = READ;
        slice* res_1 = kv_read_sync(cb);
        if (!res_1->data) {
            success_num += 2;
        }
        free(res_1->data);
        free(res_1);
        free_callback(cb, cb->item);
        periodic_count_id(1000, tid, "batch-delete-read (%lu%%)", (i + part_requests)*100LU / nb_requests);
    }
    double success_ratio = (double)success_num * 100.0 / (double)nb_requests;
    printf("#BATCH_DELETE_READ (%s) result: thread %lu test successed %lu items (all %lu items) (success ratio: %.2lf%%)\n"
            , __FUNCTION__, tid, success_num, nb_requests, success_ratio);
}
static void whole_scan(size_t tid, uint64_t nb_requests, int is_var) {
    declare_periodic_count;
    uint64_t success_num = 0;
    char *buf = valloc(page_size);
    uint32_t size;
    size_t i = 0;
    struct slab_callback *cb = bench_cb(tid, 0);
    struct item_metadata *meta = NULL;
    slice *value = NULL;
    double success_ratio = 0;

    if (!scan) {
        printf("Scan parameter unset!\n");
        exit(1);
    }

    cb->item = calloc(1, page_size);
    meta = (struct item_metadata *)(cb->item);

    // KVs have already been filled during load phase
    iterator_init();
    for (SeekToFirst(); Valid(); Next()) {
        GetKey(buf, &size);
        meta->seq_id = 0;
        meta->key_size = size;
        meta->value_size = 0;
        memcpy(cb->item + sizeof(*meta), buf, size);
        value = kv_read_sync(cb);
        if (value->data) {
            ++success_num;
            free(value->data);
        }
        free(value);
        value = NULL;
        memset(cb->item, 0, sizeof(*meta) + size);
        ++i;
        if (i >= nb_requests) {
            break;
        }
        periodic_count_id(1000, tid, "whole-scan (%lu%%)", i*100LU / nb_requests);
    }
    iterator_del();

    free_callback(cb, cb->item);
    free(buf);
 
    success_ratio = (double)success_num * 100.0 / (double)nb_requests;
    printf("#WHOLE_SCAN (%s) result: thread %lu test successed %lu items (all %lu items) (success ratio: %.2lf%%)\n"
            , __FUNCTION__, tid, success_num, nb_requests, success_ratio);
}

static void whole_scan_multi_read(size_t tid, uint64_t nb_requests, int is_var) {
    declare_periodic_count;
    uint64_t success_num = 0;
    char *buf = valloc(page_size);
    uint32_t size[MAX_READS];
    size_t i = 0;
    // struct slab_callback *cb = bench_cb(tid, 0);
    struct slab_callback *cb[MAX_READS];
    struct item_metadata *meta[MAX_READS];
    slice **value;
    double success_ratio = 0;
    int cnt, k;

    if (!scan) {
        printf("Scan parameter unset!\n");
        exit(1);
    }

    for (i = 0; i < MAX_READS; i++) {
        cb[i] = (struct slab_callback *)malloc(sizeof(struct slab_callback));
        cb[i]->item = calloc(1, page_size);
        meta[i] = (struct item_metadata *)(cb[i]->item);
    }

    // KVs have already been filled during load phase
    iterator_init();
    cnt = 0;
    i = 0;
    for (SeekToFirst(); Valid(); Next()) {
        GetKey(buf, &size[cnt]);
        meta[cnt]->seq_id = 0;
        meta[cnt]->key_size = size[cnt];
        meta[cnt]->value_size = 0;
        memcpy(cb[cnt]->item + sizeof(struct item_metadata), buf, size[cnt]);
        cb[cnt]->cb = NULL;

        ++cnt;
        if (cnt < MAX_READS) {
            continue;
        }

        value = kv_multi_read(cb, cnt);
        for (k = 0; k < cnt; k++) {
            if (value[k]->data) {
                ++success_num;
                free(value[k]->data);
            }
            free(value[k]);
            value[k] = NULL;
            memset(cb[k]->item, 0, sizeof(struct item_metadata) + size[k]);
        }
        free(value);
        value = NULL;
        i += cnt;
        if (i >= nb_requests) {
            break;
        }
        cnt = 0;
        periodic_count_id(1000, tid, "whole-scan (%lu%%)", i*100LU / nb_requests);
    }
    iterator_del();

    for (i = 0; i < MAX_READS; i++) {
        free_callback(cb[i], cb[i]->item);
    }

    free(buf);
    
    success_ratio = (double)success_num * 100.0 / (double)nb_requests;
    if (success_ratio > 100.0) {
        success_ratio = 100.0;
    }
    printf("#WHOLE_SCAN (%s) result: thread %lu test successed %lu items (all %lu items) (success ratio: %.2lf%%)\n"
            , __FUNCTION__, tid, success_num, nb_requests, success_ratio);
}

/* Generic interface */
static void launch_ycsb(struct workload *w, size_t tid, bench_t b) {
   switch(b) {
      case ycsb_a_uniform:
         return _launch_ycsb(0, tid, w->nb_requests_per_thread, 0, g_is_var);
      case ycsb_b_uniform:
         return _launch_ycsb(1, tid, w->nb_requests_per_thread, 0, g_is_var);
      case ycsb_c_uniform:
         return _launch_ycsb(2, tid, w->nb_requests_per_thread, 0, g_is_var);
      case ycsb_e_uniform:
         //return _launch_ycsb_e(3, tid, w->nb_requests_per_thread, 0);
         return _launch_ycsb_e_multi_read(3, tid, w->nb_requests_per_thread, 0);
      case ycsb_g_uniform:
         return _launch_ycsb(4, tid, w->nb_requests_per_thread, 0, g_is_var);
      case ycsb_d_uniform:
         return _launch_ycsb(5, tid, w->nb_requests_per_thread, 0, g_is_var);
      case ycsb_f_uniform:
         return _launch_ycsb(6, tid, w->nb_requests_per_thread, 0, g_is_var);
      case ycsb_a_zipfian:
         return _launch_ycsb(0, tid, w->nb_requests_per_thread, 1, g_is_var);
      case ycsb_b_zipfian:
         return _launch_ycsb(1, tid, w->nb_requests_per_thread, 1, g_is_var);
      case ycsb_c_zipfian:
         return _launch_ycsb(2, tid, w->nb_requests_per_thread, 1, g_is_var);
      case ycsb_e_zipfian:
         return _launch_ycsb_e(3, tid, w->nb_requests_per_thread, 1);
      case ycsb_g_zipfian:
         return _launch_ycsb(4, tid, w->nb_requests_per_thread, 1, g_is_var);
      case be_read_all_load:
          return read_all_load(tid, load_kv_num);
      case be_read_update_read:
          return read_update_read(tid, w->nb_requests_per_thread, g_is_var);
      case be_read_delete_read:
          return read_delete_read(tid, w->nb_requests_per_thread, g_is_var);
      case be_batch_add_read:
          return batch_add_read(tid, w->nb_requests_per_thread, g_is_var);
      case be_batch_update_read:
          return batch_update_read(tid, w->nb_requests_per_thread, g_is_var);
      case be_batch_delete_read:
          return batch_delete_read(tid, w->nb_requests_per_thread, g_is_var);
      case be_whole_scan:
          return whole_scan(tid, w->nb_requests_per_thread, g_is_var);
      case be_whole_scan_multi_read:
          return whole_scan_multi_read(tid, w->nb_requests_per_thread, g_is_var);
      default:
         die("Unsupported workload\n");
   }
}

/* Pretty printing */
static const char *name_ycsb(bench_t w) {
   switch(w) {
      case ycsb_a_uniform:
         return "YCSB A - Uniform";
      case ycsb_b_uniform:
         return "YCSB B - Uniform";
      case ycsb_c_uniform:
         return "YCSB C - Uniform";
      case ycsb_d_uniform:
         return "YCSB D - Uniform";
      case ycsb_e_uniform:
         return "YCSB E - Uniform";
      case ycsb_f_uniform:
         return "YCSB F - Uniform";
      case ycsb_g_uniform:
         return "YCSB G - Uniform";
      case ycsb_a_zipfian:
         return "YCSB A - Zipf";
      case ycsb_b_zipfian:
         return "YCSB B - Zipf";
      case ycsb_c_zipfian:
         return "YCSB C - Zipf";
      case ycsb_e_zipfian:
         return "YCSB E - Zipf";
      case ycsb_g_zipfian:
         return "YCSB G - Zipf";
      case be_read_all_load:
          return "READ ALL LOAD";
      case be_read_update_read:
          return "READ UPDATE READ";
      case be_read_delete_read:
          return "READ DELETE READ";
      case be_batch_add_read:
          return "BATCH ADD READ";
      case be_batch_update_read:
          return "BATCH UPDATE READ";
      case be_batch_delete_read:
          return "BATCH DELETE READ";
      case be_whole_scan:
          return "WHOLE SCAN";
      case be_whole_scan_multi_read:
          return "WHOLE SCAN MULTI READ";
      default:
         return "??";
   }
}

static int handles_ycsb(bench_t w) {
   switch(w) {
      case ycsb_a_uniform:
      case ycsb_b_uniform:
      case ycsb_c_uniform:
      case ycsb_e_uniform:
      case ycsb_g_uniform:
      case ycsb_d_uniform:
      case ycsb_f_uniform:
      case ycsb_a_zipfian:
      case ycsb_b_zipfian:
      case ycsb_c_zipfian:
      case ycsb_e_zipfian:
      case ycsb_g_zipfian:
      case be_read_all_load:
      case be_read_update_read:
      case be_read_delete_read:
      case be_batch_add_read:
      case be_batch_update_read:
      case be_batch_delete_read:
      case be_whole_scan:
      case be_whole_scan_multi_read:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_ycsb(void) {
   return "YCSB";
}

struct workload_api YCSB = {
   .handles = handles_ycsb,
   .launch = launch_ycsb,
   .api_name = api_name_ycsb,
   .name = name_ycsb,
   .create_unique_item = create_unique_item_ycsb,
};
