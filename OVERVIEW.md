# OVERVIEW

## Warning note

## Logic and path of a query

The KV pair is stored in a page (4kB by default) in one of the database files (or slabs). The number of slabs nb_slabs is 64 by default.

The slab number is computed in get_slab function in slabworker.cc (ctx->slabs[(hash / ctx->slab_size) % ctx->nb_slabs]).

The page id within a slab is computed in get_slab_idx function in slabworker.cc (hash % ctx->slab_size).

TenvisDB has 2 distinct types of threads. Load injector threads, and worker threads.

* Requests are sent by the  load injector threads. Load injector threads basically:
  * Create a request `struct slab_callback *cb`. A request contains an `item = { key, value }` and a callback that is called when the request has been processed.
  * Enqueue the request in the KV, using one of the `kv_xx_xx(cb)` function (e.g., `kv_read_async(cb)` ). Requests partionned amongst workers based on the hash of the key modulo number of workers (i.e., hash%get_nb_workers() in the get_slab_context function in slabworker.cc].
  * Main code to generate the callbacks is in [workload-ycsb.c](workload-ycsb.c) and the enqueue code is in [slabworker.cc](slabworker.cc).

* Workers threads do the actual work. In a big loop (`worker_slab_init`):
  * Each worker thread has a slab_context which stores the context of the threads.
  * They dequeue requests and figure out from which file queried item should be read or written (`worker_dequeue_requests` [slabworker.cc](slabworker.cc))
    * Which call functions that compute where the item is in the page (e.g., `read_item_async` [slab.c](slab.c))
       * Which call functions that check if the item is cached or if an IO request should be created (e.g., `read_page_async` [ioengine.c](ioengine.c))
  * After dequeueing enough requests, or when the IO queue is full, or when no request can be dequeued anymore, then IOs are sent to disk (`worker_ioengine_enqueue_ios` [slabworker.c](slabworker.c))
  * We then wait for the disk to process IOs (`worker_ioengine_get_completed_ios`)
  * And finally we call the callbacks of all processed requests (`worker_ioengine_process_completed_ios`)

## Options

[options.h](main.c) contains main configuration options.

Mainly, you want to configure `PATH` to point to a directory that exists.
```c
#define PATH "/scratch%lu/tenvisdb/slab-%d"
#define PATH "/scratch[disk_id]/tenvisdb/slab-[workerid]"
```

You probably want to disable `PINNNING` if the total number of load_injector_threads and worker threads exceeds the total number of threads available on the server.

And on small machines, you should reduce `PAGE_CACHE_SIZE`.


## Workload parameters

[main.c](main.c) contains workloads parameters.

```c
struct workload w = {
   .api = &YCSB,
   .nb_items_in_db = 100000000LU, // Size of the DB, if you change that, you must delete the DB before rerunning a benchmark
   .nb_load_injectors = 4,
};

bench_t workload, workloads[] = {
      ycsb_c_uniform,
}
```



ycsb_c_uniform is a read only workload;
ycsb_g_uniform is a write only workload;
ycsb_a_uniform is a mixed workload;


## Launch a bench
* To create a new database 
```bash
rm /scratch0/tenvisdb/*

./main <create_db> <number of disks> <number of workers per disk>
e.g. ./main 1 1 16 # will use a total of 16 workers + 4 load injectors if using the workload definition above = 20 threads in total
```
* To run on a existing database

```bash
./main <create_db> <number of disks> <number of workers per disk>
e.g. ./main 0 1 16 # will use a total of 16 workers + 4 load injectors if using the workload definition above = 20 threads in total
or run a script
./scripts/run-ssd.sh
```


Note only support a single disk right now, so number of disks is fixed to 1. The number of workers per disk is expected to be a power-of-2 in the range [1,64].

## Good to know
* 

## Common errors
If you get this error then the page cache doesn't fit in memory:
```c
main: pagecache.c:27: void page_cache_init(struct pagecache *): Assertion `p->cached_data' failed.
```
