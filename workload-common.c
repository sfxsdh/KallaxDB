#include "headers.h"

char * unique_chars;
#define unique_chars_len 1048576

extern size_t key_size;
extern size_t value_size;
extern int g_is_var;

/*
 * Create a workload item for the database
 */
char *create_unique_item(size_t item_size, uint64_t uid) {
   char *item = calloc(1, item_size);
   struct item_metadata *meta = (struct item_metadata *)item;
   meta->key_size = key_size;
   meta->value_size = item_size - meta->key_size - sizeof(*meta); 

   char *item_key = &item[sizeof(*meta)];
   char *item_value = &item[sizeof(*meta) + meta->key_size];
   memset(item_key, 0, meta->key_size);
   *(uint64_t*)item_key = uid;
   if (meta->value_size > 0) {
     memcpy(item_value, unique_chars + rand()%unique_chars_len, meta->value_size/2);
     memset(item_value + meta->value_size / 2, 0, meta->value_size/2);
     memcpy(item_value, item_key, sizeof(uint64_t));
   }
   return item;
}

config_option parse_option(char* option){
    config_option cfg_option;
    if (option == NULL || strlen(option) < 5
            || ((option[0] != '-') || (option[1] != '-'))
            ||(strstr((option + 3), "=") == NULL)) {
        die("The option (%s) is not valid", option);
    }
    char* equal = strstr(option, "=");
    size_t opt_len = strlen(option);
    size_t name_len = equal - (option + 2);
    size_t value_len = opt_len - (equal - option);
    cfg_option.name = malloc(name_len + 1);
    cfg_option.value = malloc(value_len + 1);
    strncpy(cfg_option.name, option + 2, name_len);
    strncpy(cfg_option.value, equal + 1, value_len);
    cfg_option.name[name_len] = '\0';
    cfg_option.value[value_len] = '\0';
    return cfg_option;
}

int parse_db_path(char* all_path, char** path_array) {
    if (all_path == NULL) {
        printf("The input db path do not exist !\n");
        return 0;
    }
    char* separator = ",";
    char *p_next;
    int count = 0;

    p_next = (char *)strtok(all_path,separator);
    while(p_next != NULL) {
          int sub_len = strlen(p_next);
          *path_array = malloc(sub_len + 1);
          strcpy(*path_array, p_next);
          (*path_array)[sub_len] = '\0';
          ++path_array;
          ++count;
          p_next = (char *)strtok(NULL,separator);
   }
    return count;
}

void free_db_path(char** path_array, int path_num) {
    int i;
    for (i = 0; i < path_num; ++i) {
        if(path_array[i] != NULL) {
            free(path_array[i]);
        }
    }
    free(path_array);
}

bench_t* parse_bench_name(char* bench_str, int* count){
    (*count) = 0;
    bench_t* bench_array = NULL;
    if (bench_str == NULL) {
        printf("The input db bench name do not exist !\n");
        return 0;
    }

    char* bench_str_cpy = malloc(strlen(bench_str) + 1);
    strcpy(bench_str_cpy, bench_str);

    char* separator = ",";
    char *p_next;

    strtok(bench_str_cpy,separator);
    ++(*count);
    while(strtok(NULL,separator) != NULL) {
          ++(*count);
    }
    free(bench_str_cpy);

    bench_array = malloc(sizeof(bench_t) * (*count));
    p_next = (char *)strtok(bench_str,separator);
    int idx = 0;
    while(p_next != NULL) {
         if (!strcmp(p_next, "ycsba")) {
             bench_array[idx] = ycsb_a_uniform;
         } else if (!strcmp(p_next, "ycsbb")) {
             bench_array[idx] = ycsb_b_uniform;
         } else if (!strcmp(p_next, "ycsbc")) {
             bench_array[idx] = ycsb_c_uniform;
         } else if (!strcmp(p_next, "ycsbd")) {
             bench_array[idx] = ycsb_d_uniform;
         } else if (!strcmp(p_next, "ycsbe")) {
             bench_array[idx] = ycsb_e_uniform;
         } else if (!strcmp(p_next, "ycsbf")) {
             bench_array[idx] = ycsb_f_uniform;
         } else if (!strcmp(p_next, "ycsbg")) {
             bench_array[idx] = ycsb_g_uniform;
         } else if (!strcmp(p_next, "readAllLoad")) {
             bench_array[idx] = be_read_all_load;
         } else if (!strcmp(p_next, "readUpdateRead")) {
             bench_array[idx] = be_read_update_read;
         } else if (!strcmp(p_next, "readDeleteRead")) {
             bench_array[idx] = be_read_delete_read;
         } else if (!strcmp(p_next, "batchAddRead")) {
             bench_array[idx] = be_batch_add_read;
         } else if (!strcmp(p_next, "batchUpdateRead")) {
             bench_array[idx] = be_batch_update_read;
         } else if (!strcmp(p_next, "batchDeleteRead")) {
             bench_array[idx] = be_batch_delete_read;
         } else if (!strcmp(p_next, "wholeScan")) {
             bench_array[idx] = be_whole_scan;
         } else if (!strcmp(p_next, "wholeScanMultiRead")) {
             bench_array[idx] = be_whole_scan_multi_read;
         } else {
             printf("The db bench %s is not supported currently!\n", p_next);
             free(bench_array);
             exit(-1);
         }
         ++idx;
         p_next = (char *)strtok(NULL,separator);
    }

    return bench_array;
}

/* We also store an item in the database that says if the database has been populated for YCSB, PRODUCTION, or another workload. */
char *create_workload_item(struct workload *w, uint64_t seed) {
   const uint64_t key = seed;
   const char *name = "YCSB";//w->api->api_name(); // YCSB or PRODUCTION?

   struct item_metadata *meta;

   char *item = calloc(sizeof(*meta) + key_size + value_size, 1);
   meta = (struct item_metadata *)item;
  
   meta->key_size = key_size;
   meta->value_size = value_size;

   char *item_key = &item[sizeof(*meta)];
   char *item_value = &item[sizeof(*meta) + meta->key_size];
   *(uint64_t*)item_key = key;
   *((uint64_t*)item_key + 1) = 0;
   memcpy(item_value, name, 4);
   return item;
}

static void add_in_tree(struct slab_callback *cb, char *item) {
    die("add in tree died\n");
}

struct rebuild_pdata {
   size_t id;
   size_t *pos;
   size_t start;
   size_t end;
   struct workload *w;
};
void *repopulate_db_worker(void *pdata) {
   declare_periodic_count;
   struct rebuild_pdata *data = pdata;
   size_t i;

   pin_me_on(get_nb_workers() + data->id);
   unique_chars = malloc ( (unique_chars_len + 16*1024) * sizeof(char));
   for(i=0; i< unique_chars_len + 16*1024; i++) { //for compression purpose
       unique_chars[i] = (char) (rand() % 96 + 32);
   }
   size_t *pos = data->pos;
   struct workload *w = data->w;
   struct workload_api *api = w->api;
   size_t start = data->start;
   size_t end = data->end;
   for(i = start; i < end; i++) {
      struct slab_callback *cb = malloc(sizeof(*cb));
      //cb->cb = compute_stats;
      cb->cb = NULL;
      cb->payload = NULL;
      cb->index_added = 0;
      cb->item = api->create_unique_item(pos[i], 1, g_is_var);
      kv_add_async(cb);
      periodic_count(1000, "Repopulating database (%lu%%)", 100LU-(end-i)*100LU/(end - start));
   }

   return NULL;
}

static void *do_read(void *arg) {
    int kv_num = 2000000 - 1;
    int i;
    struct workload *w = (struct workload *)arg;
    for (i = 0; i < kv_num; i++) {
       struct slab_callback *cb = malloc(sizeof(*cb));
       cb->cb = compute_stats;
       cb->payload = NULL;
       cb->id = i;
       cb->tag = 222;
       cb->id = i;
       cb->item = create_workload_item(w, i);
       kv_read_sync(cb);
    }
}

static void* do_insert(void *arg) {
    struct workload *w = (struct workload *)arg;
    srand((101));
    int kv_num = 1000000;
    int i;
    for (i = 0; i < kv_num; i++) {
       struct slab_callback *cb = malloc(sizeof(*cb));
       cb->cb = compute_stats;
       cb->payload = NULL;
       cb->id = i;
       cb->tag = 111;
       cb->item = create_workload_item(w, i);
       kv_add_async(cb);
    }
    srand((101));
    for (i = 0; i < kv_num; i++) {
       struct slab_callback *cb = malloc(sizeof(*cb));
       cb->cb = free_callback;
       cb->payload = NULL;
       cb->id = i;
       cb->tag = 222;
       cb->id = i;
       cb->item = create_workload_item(w, i);
       kv_read_sync(cb);
    }
    printf("Done read\n");
    usleep(1000000);
    return NULL;
}

void repopulate_db(struct workload *w) {
    size_t i;
   declare_timer;
   int64_t nb_inserts = (get_database_size() > w->nb_items_in_db)?0:(w->nb_items_in_db);
   size_t *pos = NULL;
   start_timer {
      printf("Initializing big array to insert elements in random order... This might take a while. (Feel free to comment but then the database will be sorted and scans much faster -- unfair vs other systems)\n");
      pos = malloc(w->nb_items_in_db * sizeof(*pos));
      for(i = 0; i < w->nb_items_in_db; i++)
         pos[i] = i;
      //shuffle(pos, nb_inserts); // To be fair to other systems, we shuffle items in the DB so that the DB is not fully sorted by luck
      printf("nb_inserts %lu\n", nb_inserts);
   } stop_timer("Big array of random positions");

   start_timer {
      struct rebuild_pdata *pdata = malloc(w->nb_load_injectors*sizeof(*pdata));
      pthread_t *threads = malloc(w->nb_load_injectors*sizeof(*threads));
      printf("1nb_inserts %lu\n", nb_inserts);
      for(i = 0; i < w->nb_load_injectors; i++) {
         pdata[i].id = i;
         pdata[i].start = (w->nb_items_in_db / w->nb_load_injectors)*i;
         pdata[i].end = (w->nb_items_in_db / w->nb_load_injectors)*(i+1);
         if(i == w->nb_load_injectors - 1)
            pdata[i].end = w->nb_items_in_db;
         pdata[i].w = w;
         pdata[i].pos = pos;
         if(i)
            pthread_create(&threads[i], NULL, repopulate_db_worker, &pdata[i]);
      }
      printf("2nb_inserts %lu\n", nb_inserts);
      repopulate_db_worker(&pdata[0]);
      for(i = 1; i < w->nb_load_injectors; i++)
         pthread_join(threads[i], NULL);
      free(threads);
      free(pdata);
   } stop_timer("Repopulating %lu elements (%lu req/s)", nb_inserts, nb_inserts*1000000/elapsed);

   free(pos);
}

/*
 *  Print an item stored on disk
 */
void print_item(size_t idx, char* _item) {
   char *item = _item;
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   if(meta->key_size == 0)
      printf("[%lu] Non existant?\n", idx);
   else if(meta->key_size == -1)
      printf("[%lu] Removed\n", idx);
   else
      printf("[%lu] K=%lu V=%s\n", idx, *(uint64_t*)item_key, &item[sizeof(*meta) + meta->key_size]);
}

/*
 * Various callbacks that are called once an item has been read / written
 */
void show_item(struct slab_callback *cb, char *item) {
   print_item(cb->slab_idx, item);
   free(cb->item);
   free(cb);
}

void free_callback(struct slab_callback *cb, char *item) {
   free(cb->item);
   if(DEBUG)
      free_payload(cb);
   free(cb);
}

void compute_stats(struct slab_callback *cb, char *item) {
   uint64_t start, end;
   declare_debug_timer;
   start_debug_timer {
      start = get_time_from_payload(cb, 0);
      rdtscll(end);
      unsigned int option  = 0;
      switch(cb->action) {
      case ADD:
      case UPDATE:
          option = 0;
          break;
      case READ:
          option = 1;
          break;
      case DELETE:
          option = 2;
          break;
      case SCAN:
          option = 4;
      }
      add_timing_stat(end - start, cb->tid, option);
      if(DEBUG && cycles_to_us(end-start) > 10000) { // request took more than 10ms
         printf("Request [%lu: %lu] [%lu: %lu] [%lu: %lu] [%lu: %lu] [%lu: %lu] [%lu: %lu] [%lu: %lu] [%lu]\n",
               get_origin_from_payload(cb, 1), get_time_from_payload(cb, 1) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 1) - start),
               get_origin_from_payload(cb, 2), get_time_from_payload(cb, 2) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 2) - start),
               get_origin_from_payload(cb, 3), get_time_from_payload(cb, 3) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 3) - start),
               get_origin_from_payload(cb, 4), get_time_from_payload(cb, 4) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 4) - start),
               get_origin_from_payload(cb, 5), get_time_from_payload(cb, 5) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 5) - start),
               get_origin_from_payload(cb, 6), get_time_from_payload(cb, 6) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 6) - start),
               get_origin_from_payload(cb, 7), get_time_from_payload(cb, 7) < start ? 0 : cycles_to_us(get_time_from_payload(cb, 7) - start),
               cycles_to_us(end  - start));
      }
      //free(cb->item);
      //if(DEBUG)
      //   free_payload(cb);
      //free(cb);
   } stop_debug_timer(5000, "Callback took more than 5ms???");
}

struct slab_callback *bench_cb(size_t tid, size_t seq) {
   struct slab_callback *cb = malloc(sizeof(*cb));
   cb->cb = compute_stats;
   cb->payload = allocate_payload();
   cb->tid = tid;
   cb->seq = seq;
   cb->index_added = 0;
   return cb;
}

const char* bench_name(bench_t b) {
    switch(b) {
    case ycsb_a_uniform:
        return "YCSB A - Uniform";
        break;
    case ycsb_b_uniform:
        return "YCSB B - Uniform";
        break;
    case ycsb_c_uniform:
        return "YCSB C - Uniform";
        break;
    case ycsb_d_uniform:
        return "YCSB D - Uniform";
        break;
    case ycsb_e_uniform:
        return "YCSB E - Uniform";
        break;
    case ycsb_f_uniform:
        return "YCSB F - Uniform";
        break;
    case ycsb_g_uniform:
        return "YCSB G - Uniform";
        break;
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
        break;
    }
}

/*
 * Generic worklad API.
 */
struct thread_data {
   size_t id;
   struct workload *workload;
   bench_t benchmark;
};

struct workload_api *get_api(bench_t b) {
   if(YCSB.handles(b))
      return &YCSB;
   if(PRODUCTION.handles(b))
      return &PRODUCTION;
   die("Unknown workload for benchmark!\n");
}

static pthread_barrier_t barrier;
void* do_workload_thread(void *pdata) {
   struct thread_data *d = pdata;

   init_seed();
   pin_me_on(get_nb_workers() + d->id);
   pthread_barrier_wait(&barrier);

   d->workload->api->launch(d->workload, d->id, d->benchmark);

   return NULL;
}

void run_workload(struct workload *w, bench_t b) {
    int i;
   struct thread_data *pdata = malloc(w->nb_load_injectors*sizeof(*pdata));

   w->nb_requests_per_thread = w->nb_requests / w->nb_load_injectors;
   pthread_barrier_init(&barrier, NULL, w->nb_load_injectors);

   if(!w->api->handles(b))
      die("The database has not been configured to run this benchmark! (Are you trying to run a production benchmark on a database configured for YCSB?)");
   if(unique_chars == NULL) {
     unique_chars = malloc ( (unique_chars_len + 16*1024) * sizeof(char));
     for(i=0; i< unique_chars_len + 16*1024; i++) { //for compression purpose
         unique_chars[i] = (char) (rand() % 96 + 32);
     }
   }
   declare_timer;
   start_timer {
      pthread_t *threads = malloc(w->nb_load_injectors*sizeof(*threads));
      for(i = 0; i < w->nb_load_injectors; i++) {
         pdata[i].id = i;
         pdata[i].workload = w;
         pdata[i].benchmark = b;
         if(i)
            pthread_create(&threads[i], NULL, do_workload_thread, &pdata[i]);
      }
      do_workload_thread(&pdata[0]);
      for(i = 1; i < w->nb_load_injectors; i++)
         pthread_join(threads[i], NULL);
      free(threads);
   } stop_timer("%s - %lu requests (%lu req/s)", w->api->name(b), w->nb_requests, w->nb_requests*1000000/elapsed);
   const char* test_name =  bench_name(b);
   if (b <= ycsb_g_uniform) {
       print_stats(test_name);
   }

   free(pdata);
}

