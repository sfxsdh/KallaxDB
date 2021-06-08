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

// extern volatile uint8_t worker_idle[MAX_WORKER_NUM];
// extern volatile uint8_t rehash_idle[MAX_REHASH_NUM];

static uint64_t freq = 0;

static uint64_t get_cpu_freq(void) {
   if(freq)
      return freq;

   FILE *fd;
   float freqf = 0;
   char *line = NULL;
   size_t len = 0;

   fd = fopen("/proc/cpuinfo", "r");
   if (!fd) {
      fprintf(stderr, "failed to get cpu frequency\n");
      perror(NULL);
      return freq;
   }

   while (getline(&line, &len, fd) != EOF) {
      if (sscanf(line, "cpu MHz\t: %f", &freqf) == 1) {
         freqf = freqf * 1000000UL;
         freq = (uint64_t) freqf;
         break;
      }
   }

   fclose(fd);
   return freq;
}


uint64_t cycles_to_us(uint64_t cycles) {
   return cycles*1000000LU/get_cpu_freq();
}

void shuffle(size_t *array, size_t n) {
   if (n > 1) {
      size_t i;
      for (i = 0; i < n - 1; i++) {
         size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
         size_t t = array[j];
         array[j] = array[i];
         array[i] = t;
      }
   }
}

void pin_me_on(int core) {
   if(!PINNING)
      return;

   cpu_set_t cpuset;
   pthread_t thread = pthread_self();

   CPU_ZERO(&cpuset);
   CPU_SET(core, &cpuset);

   int s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
   if (s != 0)
      die("Cannot pin thread on core %d\n", core);

}

void init_db_path(char** path_array, char* path, int idx) {
    int path_len = strlen(path);
    db_path[idx] = malloc(path_len + 1);
    strcpy(db_path[idx], path);
    db_path[path_len] = '\0';
}

void parse_config_file(char* file_path){
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
    
    db_path = malloc(sizeof(char*));
    init_db_path(db_path, DEFAULT_DB_PATH, 0);

    if (file_path == NULL) {
        die("The config file path is invalid");
    } else {
        printf("Conf file path = %s\n", file_path);
    }

    char option[1024];
    FILE *fd = fopen(file_path, "rw");
    if (fd == NULL) {
        die("Config file open error");
    }

    while (fgets(option, 1024, fd) != NULL ) {
        if (strlen(option) < 3 || (strstr((option), "=") == NULL)) {
            die("The config file option (%s) is not valid", option);
        }
        config_option cfg_option;
        char* equal = strstr(option, "=");
        size_t opt_len = strlen(option);
        size_t name_len = equal - option;
        size_t value_len = opt_len - (equal - option) - 2;

        if (name_len == 0 || value_len == 0) {
            die("The config file option (%s) is not valid", option);
        }

        cfg_option.name = malloc(name_len + 1);
        cfg_option.value = malloc(value_len + 1);
        strncpy(cfg_option.name, option, name_len);
        strncpy(cfg_option.value, equal + 1, value_len);
        cfg_option.name[name_len] = '\0';
        cfg_option.value[value_len] = '\0';

        if (!strcmp(cfg_option.name, "create_db")) {
            create_db = atoi(cfg_option.value);
        } else if (!strcmp(cfg_option.name, "run_test")) {
            run_test = atoi(cfg_option.value);
        } else if (!strcmp(cfg_option.name, "nb_paths")){
            nb_paths = atoi(cfg_option.value);
        } else if (!strcmp(cfg_option.name, "nb_slabs")){
            nb_slabs = atoi(cfg_option.value);
        } else if(!strcmp(cfg_option.name, "slab_size")){
            slab_size = atof(cfg_option.value) * 1024 * 1024LU;
        } else if (!strcmp(cfg_option.name, "nb_workers")){
            nb_workers = atoi(cfg_option.value);
        } else if (!strcmp(cfg_option.name, "loader_num")){
            loader_num = atoi(cfg_option.value);
        } else if (!strcmp(cfg_option.name, "client_num")){
            client_num = atoi(cfg_option.value);;
        } else if (!strcmp(cfg_option.name, "load_kv_num")){
            load_kv_num = strtoull(cfg_option.value, NULL, 0);
        } else if (!strcmp(cfg_option.name, "request_kv_num")){
            request_kv_num = strtoull(cfg_option.value, NULL, 0);
        } else if (!strcmp(cfg_option.name, "key_size")){
            key_size = strtoull(cfg_option.value, NULL, 0);
            if (key_size < 8) {
                key_size = 8;
                printf("Kallax DB minimum key size is 8 byte\n");
            }
        } else if (!strcmp(cfg_option.name, "value_size")){
            value_size = strtoull(cfg_option.value, NULL, 0);
            if (value_size < 1) {
                value_size = 1;
                printf("Kallax DB minimum value size is 1 byte\n");
            }
        } else if (!strcmp(cfg_option.name, "page_size")){
            page_size = strtoul(cfg_option.value, NULL, 0);
        } else if (!strcmp(cfg_option.name, "queue_depth")) {
            queue_depth = strtoul(cfg_option.value, NULL, 0);
        } else if (!strcmp(cfg_option.name, "nb_rehash")) {
            nb_rehash = strtoul(cfg_option.value, NULL, 0);
        } else if (!strcmp(cfg_option.name, "db_path")){
            use_default_path = 0;
            char* all_db_path = malloc(strlen(cfg_option.value) + 1);
            strcpy(all_db_path, cfg_option.value);
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
        } else if (!strcmp(cfg_option.name, "db_bench")){
            use_default_bench = 0;
            char* all_db_bench = malloc(strlen(cfg_option.value) + 1);
            strcpy(all_db_bench, cfg_option.value);
            int run_bench_count = 0;
            bench_t* run_bench = parse_bench_name(all_db_bench, &run_bench_count);

            db_bench_count = run_bench_count;
            if (db_bench_count == 0) {
                free(db_bench);
                free(run_bench);
                die("The db bench config error (bench number is 0)!\n");
            }
            free(db_bench);
            db_bench = run_bench;
            free(all_db_bench);
        } else if (!strcmp(cfg_option.name, "is_var")){
            g_is_var = atoi(cfg_option.value);
        } else if (!strcmp(cfg_option.name, "scan")) {
            scan = atoi(cfg_option.value);
        } else {
            die("This option(%s) does not exist!\n", cfg_option.name);
        }
        free(cfg_option.name);
        free(cfg_option.value);
    }
    fclose(fd);

    if (nb_slabs < nb_paths) {
        nb_slabs = nb_paths;
        printf("nb_slabs %ld too small; will change to %ld!\n", nb_slabs, nb_paths);
    }

    // Each path corresponds to at least one worker
    if (nb_workers > nb_paths) {
        nb_workers = ((nb_workers + nb_paths - 1) / nb_paths) * nb_paths;
    } else if (nb_workers < nb_paths) {
        printf("nb_workers will be set to %ld!\n", nb_paths);
        nb_workers = nb_paths;
    }
}

