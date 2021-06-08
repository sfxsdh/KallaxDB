#ifndef STATS_H
#define STATS_H 1

#define UPDATE_OPTION  0
#define READ_OPTION    1
#define DELETE_OPTION  2
#define ADD_OPTION     3
#define SCAN_OPTION    4

#define OPTION_NUM     5

struct slab_callback;
void init_timing_stat();
void reset_timing_stat();
void free_timing_stat();
void add_timing_stat(uint64_t elapsed, size_t tid, unsigned int option);
void summary_stat();
void print_stats(const char* test_name);

uint64_t cycles_to_us(uint64_t cycles);

void *allocate_payload(void);
void free_payload(struct slab_callback *c);
void add_time_in_payload(struct slab_callback *c, size_t origin);
uint64_t get_time_from_payload(struct slab_callback *c, size_t pos);
uint64_t get_origin_from_payload(struct slab_callback *c, size_t pos);
#endif
