#include "headers.h"
#include "utils.h"
#include "slab.h"

#define WRITE_MAX_STATS 125000LU
#define READ_MAX_STATS 125000LU

#define INTERVAL_A 1LU
#define INTERVAL_B 10LU
#define INTERVAL_C 100LU
#define INTERVAL_D 1000LU
#define INTERVAL_E 10000LU

#define LIMIT_A 1000LU
#define LIMIT_B 10000LU
#define LIMIT_C 100000LU
#define LIMIT_D 1000000LU
#define LIMIT_E 10000000LU

#define TIME_LEN_A ((size_t)(LIMIT_A / INTERVAL_A))
#define TIME_LEN_B ((size_t)((LIMIT_B - LIMIT_A) / INTERVAL_B))
#define TIME_LEN_C ((size_t)((LIMIT_C - LIMIT_B) / INTERVAL_C))
#define TIME_LEN_D ((size_t)((LIMIT_D - LIMIT_C) / INTERVAL_D))
#define TIME_LEN_E ((size_t)((LIMIT_E - LIMIT_D) / INTERVAL_E))

#define LATENCY_NUM 6

extern int client_num;

struct stats {
   uint64_t* time_a;  // 0us < time < 1000us, interval 1us
   uint64_t* time_b;  // 1000us <= time < 10000us, interval 10us
   uint64_t* time_c;  // 10000us <= time < 100000us, interval 100us
   uint64_t* time_d;  // 100000us <= time < 1000000us, interval 1000us
   uint64_t* time_e;   // 1000000us <= time < 10000000us, interval 10000us
   uint64_t  time_f;  // time >= 10000000us

} *stats[OPTION_NUM];

struct stat_summary {
    uint64_t latency[LATENCY_NUM];
    uint64_t avg;
} stat_res[OPTION_NUM];

struct stat_summary all_stat_res;

void init_timing_stat() {
    int i, j;
    for (i = 0; i < OPTION_NUM; ++i) {
        stats[i] = malloc(sizeof(struct stats) * client_num);
        for (j = 0; j < client_num; j++) {
            stats[i][j].time_a = calloc(TIME_LEN_A, sizeof(uint64_t));
            stats[i][j].time_b = calloc(TIME_LEN_B, sizeof(uint64_t) );
            stats[i][j].time_c = calloc(TIME_LEN_C, sizeof(uint64_t));
            stats[i][j].time_d = calloc(TIME_LEN_D, sizeof(uint64_t));
            stats[i][j].time_e = calloc(TIME_LEN_E, sizeof(uint64_t));
            stats[i][j].time_f = 0LU;
        }

        for (j = 0; j < LATENCY_NUM; ++j) {
            stat_res[i].latency[j] = 0LU;
        }
        stat_res[i].avg = 0LU;
    }
}

void summary_stat() {
    double need_precent[LATENCY_NUM] = {50.0, 95.0, 99.0, 99.9, 99.99, 100.0};
    struct stats all_thread_stat[OPTION_NUM];
    struct stats all_stat;
    all_stat.time_a = calloc(TIME_LEN_A, sizeof(uint64_t));
    all_stat.time_b = calloc(TIME_LEN_B, sizeof(uint64_t) );
    all_stat.time_c = calloc(TIME_LEN_C, sizeof(uint64_t));
    all_stat.time_d = calloc(TIME_LEN_D, sizeof(uint64_t));
    all_stat.time_e = calloc(TIME_LEN_E, sizeof(uint64_t));
    all_stat.time_f = 0LU;
    uint64_t all_std_latency[LATENCY_NUM];
    uint64_t all_opt_sum= 0LU;
    uint64_t all_latency_sum = 0LU;
    int i, j, k;

    for(i = 0; i < OPTION_NUM; ++i) {
        all_thread_stat[i].time_a = calloc(TIME_LEN_A, sizeof(uint64_t));
        all_thread_stat[i].time_b = calloc(TIME_LEN_B, sizeof(uint64_t) );
        all_thread_stat[i].time_c = calloc(TIME_LEN_C, sizeof(uint64_t));
        all_thread_stat[i].time_d = calloc(TIME_LEN_D, sizeof(uint64_t));
        all_thread_stat[i].time_e = calloc(TIME_LEN_E, sizeof(uint64_t));
        all_thread_stat[i].time_f = 0LU;

        for (j = 0; j < TIME_LEN_A; ++j) {
            for(k = 0; k < client_num; ++k) {
                all_thread_stat[i].time_a[j] += stats[i][k].time_a[j];
            }
            all_stat.time_a[j] += all_thread_stat[i].time_a[j];
        }

        for (j = 0; j < TIME_LEN_B; ++j) {
            for(k = 0; k < client_num; ++k) {
                all_thread_stat[i].time_b[j] += stats[i][k].time_b[j];
            }
            all_stat.time_b[j] += all_thread_stat[i].time_b[j];
        }

        for (j = 0; j < TIME_LEN_C; ++j) {
            for(k = 0; k < client_num; ++k) {
                all_thread_stat[i].time_c[j] += stats[i][k].time_c[j];
            }
            all_stat.time_c[j] += all_thread_stat[i].time_c[j];
        }

        for (j = 0; j < TIME_LEN_D; ++j) {
            for(k = 0; k < client_num; ++k) {
                all_thread_stat[i].time_d[j] += stats[i][k].time_d[j];
            }
            all_stat.time_d[j] += all_thread_stat[i].time_d[j];
        }

        for (j = 0; j < TIME_LEN_E; ++j) {
            for(k = 0; k < client_num; ++k) {
                all_thread_stat[i].time_e[j] += stats[i][k].time_e[j];
            }
            all_stat.time_e[j] += all_thread_stat[i].time_e[j];
        }

        for(k = 0; k < client_num; ++k) {
             all_thread_stat[i].time_f += stats[i][k].time_f;
        }
        all_stat.time_f += all_thread_stat[i].time_f;

        uint64_t opt_sum = 0;
        uint64_t latency_sum = 0;
        for (j = 0; j < TIME_LEN_A; ++j) {
            opt_sum += all_thread_stat[i].time_a[j];
            latency_sum += (all_thread_stat[i].time_a[j] * INTERVAL_A * j);
        }

        for (j = 0; j < TIME_LEN_B; ++j) {
            opt_sum += all_thread_stat[i].time_b[j];
            latency_sum += (all_thread_stat[i].time_b[j] * (INTERVAL_B * j + LIMIT_A));
        }

        for (j = 0; j < TIME_LEN_C; ++j) {
            opt_sum += all_thread_stat[i].time_c[j];
            latency_sum += (all_thread_stat[i].time_c[j] * (INTERVAL_C * j + LIMIT_B));
        }

        for (j = 0; j < TIME_LEN_D; ++j) {
            opt_sum += all_thread_stat[i].time_d[j];
            latency_sum += (all_thread_stat[i].time_d[j] * (INTERVAL_D * j + LIMIT_C));
        }

        for (j = 0; j < TIME_LEN_E; ++j) {
            opt_sum += all_thread_stat[i].time_e[j];
            latency_sum += (all_thread_stat[i].time_e[j] * (INTERVAL_E * j + LIMIT_D));
        }

        opt_sum += all_thread_stat[i].time_f;
        latency_sum += (all_thread_stat[i].time_f * LIMIT_E);

        uint64_t avg_latency = opt_sum == 0 ? 0 : (uint64_t)(latency_sum / opt_sum);
        stat_res[i].avg = avg_latency;

        uint64_t std_latency[LATENCY_NUM];
        for (j = 0; j < LATENCY_NUM; ++j) {
            std_latency[j] = (uint64_t)(opt_sum * need_precent[j] /100.0);
        }

        if (opt_sum != 0) {
            uint64_t cur_opt_sum = 0;
            for (j = 0; j < TIME_LEN_A; ++j) {
                if (all_thread_stat[i].time_a[j] == 0) {
                    continue;
                }
                cur_opt_sum += all_thread_stat[i].time_a[j];
                uint64_t next_opt_sum = 0;
                if (j == TIME_LEN_A - 1) {
                    next_opt_sum = cur_opt_sum + all_thread_stat[i].time_b[0];
                } else {
                    next_opt_sum = cur_opt_sum + all_thread_stat[i].time_a[j + 1];
                }

                for (k = 0; k < LATENCY_NUM; ++k) {
                    if(std_latency[k] >= cur_opt_sum && std_latency[k] <= next_opt_sum) {
                        stat_res[i].latency[k] = j == 0 ? 1 : j;
                    }
                }
            }

            for (j = 0; j < TIME_LEN_B; ++j) {
                if (all_thread_stat[i].time_b[j] == 0) {
                    continue;
                }
                cur_opt_sum += all_thread_stat[i].time_b[j];
                uint64_t next_opt_sum = 0;
                if (j == TIME_LEN_B - 1) {
                    next_opt_sum = cur_opt_sum + all_thread_stat[i].time_c[0];
                } else {
                    next_opt_sum = cur_opt_sum + all_thread_stat[i].time_b[j + 1];
                }

                for (k = 0; k < LATENCY_NUM; ++k) {
                    if(std_latency[k] >= cur_opt_sum && std_latency[k] <= next_opt_sum) {
                        stat_res[i].latency[k] = j * INTERVAL_B + LIMIT_A;
                    }
                }
            }

            for (j = 0; j < TIME_LEN_C; ++j) {
                if (all_thread_stat[i].time_c[j] == 0) {
                    continue;
                }
                cur_opt_sum += all_thread_stat[i].time_c[j];
                uint64_t next_opt_sum = 0;
                if (j == TIME_LEN_C - 1) {
                    next_opt_sum = cur_opt_sum + all_thread_stat[i].time_d[0];
                } else {
                    next_opt_sum = cur_opt_sum + all_thread_stat[i].time_c[j + 1];
                }

                for (k = 0; k < LATENCY_NUM; ++k) {
                    if(std_latency[k] >= cur_opt_sum && std_latency[k] <= next_opt_sum) {
                        stat_res[i].latency[k] = j * INTERVAL_C + LIMIT_B;
                    }
                }
            }

            for (j = 0; j < TIME_LEN_D; ++j) {
                if (all_thread_stat[i].time_d[j] == 0) {
                    continue;
                }
                cur_opt_sum += all_thread_stat[i].time_d[j];
                uint64_t next_opt_sum = 0;
                if (j == TIME_LEN_D - 1) {
                    next_opt_sum = cur_opt_sum + all_thread_stat[i].time_e[0];
                } else {
                    next_opt_sum = cur_opt_sum + all_thread_stat[i].time_d[j + 1];
                }

                for (k = 0; k < LATENCY_NUM; ++k) {
                    if(std_latency[k] >= cur_opt_sum && std_latency[k] <= next_opt_sum) {
                        stat_res[i].latency[k] = j * INTERVAL_D + LIMIT_C;
                    }
                }
            }

            for (j = 0; j < TIME_LEN_E; ++j) {
                if (all_thread_stat[i].time_e[j] == 0) {
                    continue;
                }
                cur_opt_sum += all_thread_stat[i].time_e[j];
                uint64_t next_opt_sum = 0;
                if (j == TIME_LEN_E - 1) {
                    next_opt_sum = cur_opt_sum + all_thread_stat[i].time_f;
                } else {
                    next_opt_sum = cur_opt_sum + all_thread_stat[i].time_e[j + 1];
                }

                for (k = 0; k < LATENCY_NUM; ++k) {
                    if(std_latency[k] >= cur_opt_sum && std_latency[k] <= next_opt_sum) {
                        stat_res[i].latency[k] = j * INTERVAL_E + LIMIT_D;
                    }
                }
            }

            if (all_thread_stat[i].time_f != 0) {
                cur_opt_sum += all_thread_stat[i].time_f;
                for (k = 0; k < LATENCY_NUM; ++k) {
                    if(std_latency[k] >= cur_opt_sum) {
                        stat_res[i].latency[k] =  LIMIT_E;
                    }
                }
            }
        }

        all_opt_sum += opt_sum;
        all_latency_sum += latency_sum;
    }

    all_stat_res.avg = all_opt_sum == 0 ? 0 : (uint64_t)(all_latency_sum / all_opt_sum);

    for (j = 0; j < LATENCY_NUM; ++j) {
        all_std_latency[j] = (uint64_t)(all_opt_sum * need_precent[j] /100.0);
    }

    if (all_opt_sum != 0) {
        uint64_t cur_opt_sum = 0;
        for (j = 0; j < TIME_LEN_A; ++j) {
            if (all_stat.time_a[j] == 0) {
                continue;
            }
            cur_opt_sum += all_stat.time_a[j];
            uint64_t next_opt_sum = 0;
            if (j == TIME_LEN_A - 1) {
                next_opt_sum = cur_opt_sum + all_stat.time_b[0];
            } else {
                next_opt_sum = cur_opt_sum + all_stat.time_a[j + 1];
            }

            for (k = 0; k < LATENCY_NUM; ++k) {
                if(all_std_latency[k] >= cur_opt_sum && all_std_latency[k] <= next_opt_sum) {
                    all_stat_res.latency[k] = j == 0 ? 1 : j;
                }
            }
        }

        for (j = 0; j < TIME_LEN_B; ++j) {
            if (all_stat.time_b[j] == 0) {
                continue;
            }
            cur_opt_sum += all_stat.time_b[j];
            uint64_t next_opt_sum = 0;
            if (j == TIME_LEN_B - 1) {
                next_opt_sum = cur_opt_sum + all_stat.time_c[0];
            } else {
                next_opt_sum = cur_opt_sum + all_stat.time_b[j + 1];
            }

            for (k = 0; k < LATENCY_NUM; ++k) {
                if(all_std_latency[k] >= cur_opt_sum && all_std_latency[k] <= next_opt_sum) {
                    all_stat_res.latency[k] = j * INTERVAL_B + LIMIT_A;
                }
            }
        }

        for (j = 0; j < TIME_LEN_C; ++j) {
            if (all_stat.time_c[j] == 0) {
                continue;
            }
            cur_opt_sum += all_stat.time_c[j];
            uint64_t next_opt_sum = 0;
            if (j == TIME_LEN_C - 1) {
                next_opt_sum = cur_opt_sum + all_stat.time_d[0];
            } else {
                next_opt_sum = cur_opt_sum + all_stat.time_c[j + 1];
            }

            for (k = 0; k < LATENCY_NUM; ++k) {
                if(all_std_latency[k] >= cur_opt_sum && all_std_latency[k] <= next_opt_sum) {
                    all_stat_res.latency[k] = j * INTERVAL_C + LIMIT_B;
                }
            }
        }

        for (j = 0; j < TIME_LEN_D; ++j) {
            if (all_stat.time_d[j] == 0) {
                continue;
            }
            cur_opt_sum += all_stat.time_d[j];
            uint64_t next_opt_sum = 0;
            if (j == TIME_LEN_D - 1) {
                next_opt_sum = cur_opt_sum + all_stat.time_e[0];
            } else {
                next_opt_sum = cur_opt_sum + all_stat.time_d[j + 1];
            }

            for (k = 0; k < LATENCY_NUM; ++k) {
                if(all_std_latency[k] >= cur_opt_sum && all_std_latency[k] <= next_opt_sum) {
                    all_stat_res.latency[k] = j * INTERVAL_D + LIMIT_C;
                }
            }
        }

        for (j = 0; j < TIME_LEN_E; ++j) {
            if (all_stat.time_e[j] == 0) {
                continue;
            }
            cur_opt_sum += all_stat.time_e[j];
            uint64_t next_opt_sum = 0;
            if (j == TIME_LEN_E - 1) {
                next_opt_sum = cur_opt_sum + all_stat.time_f;
            } else {
                next_opt_sum = cur_opt_sum + all_stat.time_e[j + 1];
            }

            for (k = 0; k < LATENCY_NUM; ++k) {
                if(all_std_latency[k] >= cur_opt_sum && all_std_latency[k] <= next_opt_sum) {
                    all_stat_res.latency[k] = j * INTERVAL_E + LIMIT_D;
                }
            }
        }

        if (all_stat.time_f != 0) {
            cur_opt_sum += all_stat.time_f;
            for (k = 0; k < LATENCY_NUM; ++k) {
                if(all_std_latency[k] >= cur_opt_sum) {
                    all_stat_res.latency[k] =  LIMIT_E;
                }
            }
        }
    }

    for(i = 0; i < OPTION_NUM; ++i) {
        free(all_thread_stat[i].time_a);
        free(all_thread_stat[i].time_b);
        free(all_thread_stat[i].time_c);
        free(all_thread_stat[i].time_d);
        free(all_thread_stat[i].time_e);
    }

    free(all_stat.time_a);
    free(all_stat.time_b);
    free(all_stat.time_c);
    free(all_stat.time_d);
    free(all_stat.time_e);
}

void reset_timing_stat() {
    int i, j;
    size_t k;

    for (i = 0; i < OPTION_NUM; ++i) {
        for (j = 0; j < client_num; j++) {
            for(k = 0; k < TIME_LEN_A; ++k) {
                stats[i][j].time_a[k] = 0LU;
            }

            for(k = 0; k < TIME_LEN_B; ++k) {
                stats[i][j].time_b[k] = 0LU;
            }

            for(k = 0; k < TIME_LEN_C; ++k) {
                stats[i][j].time_c[k] = 0LU;
            }

            for(k = 0; k < TIME_LEN_D; ++k) {
                stats[i][j].time_d[k] = 0LU;
            }

            for(k = 0; k < TIME_LEN_E; ++k) {
                stats[i][j].time_e[k] = 0LU;
            }
            stats[i][j].time_f = 0LU;
        }

        stat_res[i].avg = 0LU;
        for(j = 0; j < LATENCY_NUM; ++j) {
            stat_res[i].latency[j] = 0LU;
        }
    }
    all_stat_res.avg = 0LU;
    for(j = 0; j < LATENCY_NUM; ++j) {
        all_stat_res.latency[j] = 0LU;
    }
}

void free_timing_stat() {
    int i, j;

    for (i = 0; i < OPTION_NUM; ++i) {
        for (j = 0; j < client_num; j++) {
            free(stats[i][j].time_a);
            free(stats[i][j].time_b);
            free(stats[i][j].time_c);
            free(stats[i][j].time_d);
            free(stats[i][j].time_e);
        }
        free(stats[i]);
    }
}


void add_timing_stat(uint64_t elapsed, size_t tid, unsigned int option) {
    uint64_t time_val = cycles_to_us(elapsed);
    if (time_val < LIMIT_A) {
        ++stats[option][tid].time_a[time_val];
    } else if(time_val < LIMIT_B) {
        uint64_t idx = (uint64_t)((time_val - LIMIT_A) / INTERVAL_B);
        ++stats[option][tid].time_b[idx];
    } else if (time_val < LIMIT_C) {
        uint64_t idx = (uint64_t)((time_val - LIMIT_B) / INTERVAL_C);
        ++stats[option][tid].time_c[idx];
    } else if (time_val < LIMIT_D) {
        uint64_t idx = (uint64_t)((time_val - LIMIT_C) / INTERVAL_D);
        ++stats[option][tid].time_d[idx];
    } else if (time_val < LIMIT_E) {
        uint64_t idx = (uint64_t)((time_val - LIMIT_D) / INTERVAL_E);
        ++stats[option][tid].time_e[idx];
    } else  {
        ++stats[option][tid].time_f;
    }
}

int cmp_uint(const void *_a, const void *_b) {
   uint64_t a = *(uint64_t*)_a;
   uint64_t b = *(uint64_t*)_b;
   if(a > b)
      return 1;
   else if(a < b)
      return -1;
   else
      return 0;
}

void print_stats(const char* test_name) {
    int i;
    const char *option[OPTION_NUM] = {"Write", "Read", "Delete", "Add", "Scan"};
    summary_stat();
    for (i = 0; i < OPTION_NUM; ++i) {
        printf("#%s #%s Latency:\t#AVG: %lu us #50p: %lu us #95p: %lu us #99p: %lu us #99.9p: %lu us #99.99p: %lu us #max: %lu us\n", test_name, option[i],
               stat_res[i].avg,
               stat_res[i].latency[0],
               stat_res[i].latency[1],
               stat_res[i].latency[2],
               stat_res[i].latency[3],
               stat_res[i].latency[4],
               stat_res[i].latency[5]);
    }

    printf("#%s #%s Latency:\t#AVG: %lu us #50p: %lu us #95p: %lu us #99p: %lu us #99.9p: %lu us #99.99p: %lu us #max: %lu us\n", test_name, "All",
           all_stat_res.avg,
           all_stat_res.latency[0],
           all_stat_res.latency[1],
           all_stat_res.latency[2],
           all_stat_res.latency[3],
           all_stat_res.latency[4],
           all_stat_res.latency[5]);
    reset_timing_stat();
}



struct timing_s {
   size_t origin;
   size_t time;
};

void *allocate_payload(void) {
#if DEBUG
   return calloc(20, sizeof(struct timing_s));
#else
   return NULL;
#endif
}

void add_time_in_payload(struct slab_callback *c, size_t origin) {
#if DEBUG
   struct timing_s *payload = (struct timing_s *) c->payload;
   if(!payload)
      return;

   uint64_t t, pos = 0;
   rdtscll(t);
   while(pos < 20 && payload[pos].time)
      pos++;
   if(pos == 20)
      die("Too many times added!\n");
   payload[pos].time = t;
   payload[pos].origin = origin;
#else
   if(origin != 0)
      return;
   uint64_t t;
   rdtscll(t);
   c->payload = (void*)t;
#endif
}

uint64_t get_origin_from_payload(struct slab_callback *c, size_t pos) {
#if DEBUG
   struct timing_s *payload = (struct timing_s *) c->payload;
   if(!payload)
      return 0;
   return payload[pos].origin;
#else
   return 0;
#endif
}

uint64_t get_time_from_payload(struct slab_callback *c, size_t pos) {
#if DEBUG
   struct timing_s *payload = (struct timing_s *) c->payload;
   if(!payload)
      return 0;
   return payload[pos].time;
#else
   return (uint64_t)c->payload;
#endif
}

void free_payload(struct slab_callback *c) {
#if DEBUG
   free(c->payload);
#endif
}
