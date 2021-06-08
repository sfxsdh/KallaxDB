#include "headers.h"
#include "utils.h"

extern int create_db;
extern int run_test;
extern int nb_paths;
extern int nb_workers;
extern int nb_slabs;

extern uint64_t load_kv_num;
extern uint64_t request_kv_num;

extern int loader_num;
extern int client_num;

extern size_t item_size;
extern size_t key_size;
extern size_t value_size;
extern uint64_t slab_size;
extern uint32_t page_size;

extern int path_num;
extern char** db_path;
extern uint32_t queue_depth;

extern int nb_slabs_per_worker;
extern int nb_slabs_per_path;
extern int use_default_path;
extern int use_default_bench;
extern int nb_rehash;

extern int g_is_var;
extern int scan;

extern volatile uint8_t worker_idle[MAX_WORKER_NUM];
extern volatile uint8_t rehash_idle[MAX_REHASH_NUM];

int main(int argc, char **argv) {
    declare_timer;
    int i, check_success = 0;

    char* config_path;
    int use_default_config_file = 1;
    for (i = 1; i < argc; ++i) {
        config_option option = parse_option(argv[i]);
        if (!strcmp(option.name, "config_path")){
            config_path = malloc(strlen(option.value) + 1);
            strcpy(config_path, option.value);
            use_default_config_file = 0;
            break;
        }
    }
    
    if (use_default_config_file) {
        config_path = "./kallaxdb.cfg";
        if (access(config_path, 0)) {
            printf("No configuration file specified!\n");
            exit(-1);
        }
    }
   
    parse_config_file(config_path);

    struct workload w = {
        .api = &YCSB,
        .nb_items_in_db = load_kv_num, //100M, ITEM_SIZE=400 bytes, 40GB
        .nb_load_injectors = loader_num,
    };

    printf("# \tConfig Option:\n");
    printf("# \tLoad kv number: %ld\n", load_kv_num);
    printf("# \tRequest number: %ld\n", request_kv_num);
    printf("# \tKey size:       %ld\n", key_size);
    printf("# \tValue size:     %ld\n", value_size);
    printf("# \tCreate DB:      %s\n", create_db == 1 ? "YES" : "NO");
    printf("# \tDisk number:    %d\n", nb_paths);
    printf("# \tWorker number:  %d\n", nb_workers);
    printf("# \tSlab number:    %d\n", nb_slabs);
    printf("# \tSlab size:      %ld MB\n", slab_size >> 20);
    printf("# \tPage size:      %d\n", page_size);
    printf("# \tQueue depth:    %d\n", queue_depth);
    printf("# \tData load thread number: %d\n", loader_num);
    printf("# \tClient thread number:    %d\n", client_num);
    for(i = 0; i < db_bench_count; ++i) {
        printf("# \tBench name %d:           %s\n", i, bench_name(db_bench[i]));
    }
    for(i = 0; i < nb_paths; ++i) {
        printf("# \tDisk path %d:            %s\n", i, db_path[i]);
    }

    for (i = 0; i < nb_workers; i++) {
        worker_idle[i] = 0;
    }

    for (i = 0; i < nb_rehash; i++) {
        rehash_idle[i] = 0;
    }
    
    init_timing_stat();
    /* Initialization of random library */
    start_timer {
        printf("Initializing random number generator (Zipf) -- this might take a while for large databases...\n");
        init_zipf_generator(0, w.nb_items_in_db - 1); /* This takes about 3s... not sure why, but this is legacy code :/ */
    } stop_timer("Initializing random number generator (Zipf)");

    // Recover database if applicable
    start_timer {
        kallaxdb_init();
    } stop_timer("Init found %lu elements", get_database_size());
   
  
   /* Add missing items if any */
   if(create_db) {
      repopulate_db(&w);
   }
   
   usleep(1000000);

   printf("YCSB start!\n");

   /* Launch benchs */
   if (run_test) {
       for(i = 0; i < db_bench_count; ++i) {
           bench_t workload = db_bench[i];
           if(workload == ycsb_e_uniform || workload == ycsb_e_zipfian) {
               w.nb_requests = request_kv_num; // requests for YCSB E are longer (scans) so we do less
           } else {
               w.nb_requests = request_kv_num;
               //w.nb_requests = 100LU; //for debug
           }
           w.nb_load_injectors = client_num;

           time_t start = time(NULL);
           run_workload(&w, workload);
           time_t end = time(NULL);
           printf("#CURRENT YCSB run time %ld s\n", (end - start));
       }
   }

    // Exit if all work is done
check_idle:
    for (i = 0; i < nb_workers; i++) {
        if (worker_idle[i] == 0) {
            usleep(2000000);
            check_success = 0;
            goto check_idle;
        }
    }

    for (i = 0; i < nb_rehash; i++) {
        if (rehash_idle[i] == 0) {
            usleep(2000000);
            check_success = 0;
            goto check_idle;
        }
    }

    if (++check_success < 2) {
        goto check_idle;
    }

    printf("Test finished!\n");

    free_timing_stat();
    free_db_path(db_path, nb_paths);
    free(db_bench);
    return 0;
}
