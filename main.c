#include "headers.h"

int main(int argc, char **argv) {
    declare_timer;
    YCSB_start = 0;
    char meta_file_name[512];
    FILE *meta_file;
    int i, check_success;
    int ret;

    create_db = DEFAULT_CREATE_DB;
    run_test = DEFAULT_RUN_TEST;
    nb_paths = DEFAULT_PATH_NUM;
    nb_workers = DEFAULT_WORKER_NUM;
    nb_slabs = DEFAULT_WORKER_NUM * DEFAULT_SLABS_PER_WORKER;
    nb_rehash = DEFAULT_REHASH_NUM;
    load_kv_num = DEFAULT_LOAD_KV_NUM;
    request_kv_num= DEFAULT_REQUEST_KV_NUM;
    loader_num = DEFAULT_LOADER_NUM;
    client_num = DEFAULT_CLIENT_NUM;
    key_size = DEFAULT_KEY_SIZE;
    value_size = DEFAULT_VALUE_SIZE;
    slab_size = DEFAULT_SLAB_SIZE * 1024LU * 1024LU;
    page_size = DEFAULT_PAGE_SIZE;
    use_default_path = 1;
    queue_depth = DEFAULT_QUEUE_DEPTH;
    scan = DEFAULT_SCAN;

    printf("default slab size = %ld, slab size = %llu\n", DEFAULT_SLAB_SIZE, slab_size);

    db_path = malloc(sizeof(char*));
    init_db_path(db_path, DEFAULT_DB_PATH, 0);

    db_bench = malloc(sizeof(bench_t));
    *db_bench = DEFAULT_DB_BENCH;
    use_default_bench = 1;

    g_is_var = DEFAULT_IS_VAR;
    struct workload w = {
        .api = &YCSB,
        .nb_items_in_db = load_kv_num, //100M, ITEM_SIZE=400 bytes, 40GB
        .nb_load_injectors = loader_num,
    };

    char* all_db_path = NULL;
    bench_t* run_bench = NULL;
    int bench_count;
    for (i = 1; i < argc; ++i) {
        config_option option = parse_option(argv[i]);

        if (!strcmp(option.name, "create_db")) {
            create_db = atoi(option.value);
        } else if (!strcmp(option.name, "run_test")) {
            run_test = atoi(option.value);
        } else if (!strcmp(option.name, "nb_paths")){
            nb_paths = atoi(option.value);
        } else if (!strcmp(option.name, "nb_slabs")){
            nb_slabs = atoi(option.value);
        } else if(!strcmp(option.name, "slab_size")){
            slab_size = atof(option.value) * 1024 * 1024LU;
        } else if (!strcmp(option.name, "nb_workers")){
            nb_workers = atoi(option.value);
        } else if (!strcmp(option.name, "loader_num")){
            w.nb_load_injectors = atoi(option.value);
            loader_num = w.nb_load_injectors;
        } else if (!strcmp(option.name, "client_num")){
            client_num = atoi(option.value);;
        } else if (!strcmp(option.name, "load_kv_num")){
            w.nb_items_in_db = strtoull(option.value, NULL, 0);
            load_kv_num = w.nb_items_in_db;
        } else if (!strcmp(option.name, "request_kv_num")){
            w.nb_requests = strtoull(option.value, NULL, 0);
            request_kv_num = w.nb_requests;
        } else if (!strcmp(option.name, "key_size")){
            key_size = strtoull(option.value, NULL, 0);
            if (key_size < 8) {
                key_size = 8;
                printf("Kallax DB minimum key size is 8 byte\n");
            }
        } else if (!strcmp(option.name, "value_size")){
            value_size = strtoull(option.value, NULL, 0);
            if (value_size < 24) {
                value_size = 24;
                printf("Kallax DB minimum value size is 24 byte\n");
            }
        } else if (!strcmp(option.name, "page_size")){
            page_size = strtoul(option.value, NULL, 0);
        } else if (!strcmp(option.name, "queue_depth")) {
            queue_depth = strtoul(option.value, NULL, 0);
        } else if (!strcmp(option.name, "nb_rehash")) {
            nb_rehash = strtoul(option.value, NULL, 0);
        } else if (!strcmp(option.name, "db_path")){
            use_default_path = 0;
            all_db_path = malloc(strlen(option.value) + 1);
            strcpy(all_db_path, option.value);
        } else if (!strcmp(option.name, "db_bench")){
            use_default_bench = 0;
            char* all_db_bench = malloc(strlen(option.value) + 1);
            strcpy(all_db_bench, option.value);
            run_bench = parse_bench_name(all_db_bench, &bench_count);
            free(all_db_bench);
        } else if (!strcmp(option.name, "is_var")){
            g_is_var = atoi(option.value);
        } else if (!strcmp(option.name, "scan")) {
            scan = atoi(option.value);
        } else {
            die("This option(%s) does not exist!\n", option.name);
        }
        free(option.name);
        free(option.value);
    }

    if (nb_slabs < nb_paths) {
        nb_slabs = nb_paths;
    }

    if (!use_default_path) {
        char** option_path_array = malloc(sizeof(char*) * nb_paths);
        int path_num = parse_db_path(all_db_path, option_path_array);

        if (path_num == 0 || path_num != nb_paths) {
            free_db_path(option_path_array, nb_paths);
            free_db_path(db_path, DEFAULT_PATH_NUM);
            die("The db path option config error!\n");
        }
        free_db_path(db_path, DEFAULT_PATH_NUM);
        db_path = option_path_array;
        if (all_db_path) {
            free(all_db_path);
        }
        for (i = 0; i < nb_paths; i++) {
            if (access(db_path[i], R_OK|W_OK) != 0) {
                ret = mkdir(db_path[i], R_OK|W_OK);
                if (ret != 0) {
                    die("Failed to create directory %s!\n", db_path[i]);
                }
            }
        }
    } else {
        die("The db path must be configed!\n");
    }

    if (!use_default_bench) {
        if (bench_count == 0) {
            free(db_bench);
            free(run_bench);
            die("The db bench config error (bench number is 0)!\n");
        }
        free(db_bench);
        db_bench = run_bench;
    } else {
        bench_count = 1;
    }

    // Each path corresponds to at least one worker
    if (nb_workers > nb_paths) {
        nb_workers = ((nb_workers + nb_paths - 1) / nb_paths) * nb_paths;
    } else {
        nb_workers = nb_paths;
    }
/*
    if (create_db) {
        for (i = 0; i < nb_paths; i++) {
            sprintf(meta_file_name, "%s/META", db_path[i]);
            meta_file = fopen(meta_file_name, "w+");
            if (!meta_file) {
                die("Meta file %s open error!\n", meta_file_name);
            }
            fprintf(meta_file, "%d %d %d %ld %d", nb_paths, nb_workers, nb_slabs, slab_size, page_size);
            fsync(fileno(meta_file));
            fclose(meta_file);
        }
    } else {
        // Use previous config
        sprintf(meta_file_name, "%s/META", db_path[0]);
        meta_file = fopen(meta_file_name, "r+");
        fscanf(meta_file, "%d %d %d %ld %d", &nb_paths, &nb_workers, &nb_slabs, &slab_size, &page_size);
        fclose(meta_file);
    }
*/
    printf("# \tConfig Option:\n");
    printf("# \tLoad kv number: %llu\n", load_kv_num);
    printf("# \tRequest number: %llu\n", request_kv_num);
    printf("# \tKey size:       %lu\n", key_size);
    printf("# \tValue size:     %lu\n", value_size);
    printf("# \tCreate DB:      %s\n", create_db == 1 ? "YES" : "NO");
    printf("# \tDisk number:    %d\n", nb_paths);
    printf("# \tWorker number:  %d\n", nb_workers);
    printf("# \tSlab number:    %d\n", nb_slabs);
    printf("# \tSlab size:      %ld MB\n", slab_size >> 20);
    printf("# \tPage size:      %lu\n", page_size);
    printf("# \tQueue depth:    %lu\n", queue_depth);
    printf("# \tData load thread number: %d\n", loader_num);
    printf("# \tClient thread number:    %d\n", client_num);
    for(i = 0; i < bench_count; ++i) {
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
        slab_workers_init();
    } stop_timer("Init found %lu elements", get_database_size());
   
  
   //db_init();

   /* Add missing items if any */
   if(create_db) {
      repopulate_db(&w);
   }
   
   usleep(1000000);

   printf("YCSB start!\n");
   YCSB_start = 1;

   // usleep(100000);
    //exit(1);
   /* Launch benchs */
   bench_t workload, workloads[] = {
//      //ycsb_c_uniform,
//      ycsb_a_uniform,
//      ycsb_b_uniform,
//      ycsb_b_uniform,
//      ycsb_c_uniform,
//      ycsb_d_uniform,
//      ycsb_f_uniform
      ycsb_g_uniform
//      ##ycsb_a_uniform, ycsb_b_uniform, ycsb_c_uniform,
//      ##ycsb_a_zipfian, ycsb_b_zipfian, ycsb_c_zipfian,
//      ## //ycsb_e_uniform, ycsb_e_zipfian, // Scans
   };


   if (run_test) {
       for(i = 0; i < bench_count; ++i) {
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
           //sleep(60);
           time_t end = time(NULL);
           printf("#CURRENT YCSB run time %d s\n", (end - start));
       }
   }

    // Exit if all work is done
check_idle:
    for (i = 0; i < nb_workers; i++) {
        if (worker_idle[i] == 0) {
            usleep(2000000);
            // printf("worker %d busy!\n", i);
            check_success = 0;
            goto check_idle;
        }
    }

    for (i = 0; i < nb_rehash; i++) {
        if (rehash_idle[i] == 0) {
            usleep(2000000);
            // printf("rehash %d busy!\n", i);
            check_success = 0;
            goto check_idle;
        }
    }

    if (++check_success < 2) {
        goto check_idle;
    }

    printf("Test finished!\n");
    // for (int i = 0; i < 10000; i++) {
    //     usleep(10000000);
    // }

    free_timing_stat();
    free_db_path(db_path, nb_paths);
    free(db_bench);
    return 0;
}
