#ifndef WORKLOAD_COMMON_H
#define WORKLOAD_COMMON_H 1

#include "headers.h"
#include "items.h"
#include "slab.h"
#include "stats.h"
#include "utils.h"
#include "random.h"
#include "slabworker.h"

/*
extern volatile int YCSB_start;
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
*/

struct workload;
typedef enum available_bench {
   ycsb_a_uniform = 0,
   ycsb_b_uniform,
   ycsb_c_uniform,
   ycsb_d_uniform,
   ycsb_e_uniform,
   ycsb_f_uniform,
   ycsb_g_uniform,
   ycsb_a_zipfian,
   ycsb_b_zipfian,
   ycsb_c_zipfian,
   ycsb_e_zipfian,
   ycsb_g_zipfian,
   prod1,
   prod2,
   be_read_all_load,
   be_read_update_read,
   be_read_delete_read,
   be_batch_add_read,
   be_batch_update_read,
   be_batch_delete_read,
   be_whole_scan,
   be_whole_scan_multi_read,
} bench_t;

extern bench_t* db_bench;
extern int db_bench_count;

struct workload_api {
   int (*handles)(bench_t w); // do we handle that workload?
   void (*launch)(struct workload *w, size_t tid, bench_t b); // launch workload
   const char* (*name)(bench_t w); // pretty print the benchmark (e.g., "YCSB A - Uniform")
   const char* (*api_name)(void); // pretty print API name (YCSB or PRODUCTION)
   char* (*create_unique_item)(uint64_t uid, int update, int is_var); // allocate an item in memory and return it
};
extern struct workload_api YCSB;
extern struct workload_api PRODUCTION;

struct workload {
   struct workload_api *api;
   int nb_load_injectors;
   uint64_t nb_requests;
   uint64_t nb_items_in_db;

   const char *db_path;

   // Filled automatically
   int nb_workers;
   uint64_t nb_requests_per_thread;
};

typedef struct config_option {
    char* name;
    char* value;
}config_option;

void repopulate_db(struct workload *w);
void run_workload(struct workload *w, bench_t bench);

char *create_unique_item(size_t item_size, uint64_t uid);
void print_item(size_t idx, char* _item);

void show_item(struct slab_callback *cb, char *item);
void free_callback(struct slab_callback *cb, char *item);
void compute_stats(struct slab_callback *cb, char *item);
struct slab_callback *bench_cb(size_t tid, size_t seq);
const char* bench_name(bench_t b);


struct workload_api *get_api(bench_t b);
config_option parse_option(char* option);
void init_db_path(char** path_array, char* path, int idx);
int parse_db_path(char* all_path, char** path_array);
void free_db_path(char** path_array, int path_num);

bench_t* parse_bench_name(char* bench_str, int* count);
void parse_config_file(char* file_path);
#endif
