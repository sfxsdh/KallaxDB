#include "headers.h"

extern uint32_t page_size;

static __thread char *disk_data;
void *safe_pread(int fd, off_t offset) {
   if(!disk_data)
      disk_data = aligned_alloc(page_size, page_size);
   int r = pread(fd, disk_data, page_size, offset);
   if(r != page_size)
      perr("pread failed! Read %d instead of %u (offset %ld)\n", r, page_size, offset);
   return disk_data;
}

void *safe_pwrite(int fd, off_t offset, char * disk_data) {
   int r = pwrite(fd, disk_data, page_size, offset);
   if(r != page_size)
      perr("pwrite failed! Write %d instead of %d (offset %lu)\n", r, page_size, offset);
   return disk_data;
}

static int io_setup(unsigned nr, aio_context_t *ctxp) {
	return syscall(__NR_io_setup, nr, ctxp);
}

static int io_submit(aio_context_t ctx, long nr, struct iocb **iocbpp) {
	return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

static inline int io_getevents(aio_context_t ctx, long min_nr, long max_nr,
		struct io_event *events, struct timespec *timeout) {
	return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}

/*
size_t io_getevents_wrapper(struct slab_context *ctx, long min_nr, long max_nr) {
    return io_getevents(&(ctx->io_ctx->ctx), min_nr, max_nr, &ctx->io_ctx->events[0], NULL);
}
*/
size_t io_getevents_wrapper(struct io_context *ctx, long min_nr, long max_nr) {
    return io_getevents(ctx->ctx, min_nr, max_nr, &ctx->events[0], NULL);
}

/*
 * Definition of the context of an IO worker thread
 */
struct linked_callbacks {
   struct slab_callback *callback;
   struct linked_callbacks *next;
};

/*
 * After completing IOs we need to call all the "linked callbacks", i.e., reads done to a page that was already in the process of being fetched.
 */
static void process_linked_callbacks(struct io_context *ctx) {
   declare_debug_timer;

   size_t nb_linked = 0;
   start_debug_timer {
      struct linked_callbacks *linked_cb;
      linked_cb = ctx->linked_callbacks;
      ctx->linked_callbacks = NULL; // reset the list

      while(linked_cb) {
         struct linked_callbacks *next = linked_cb->next;
         struct slab_callback *callback = linked_cb->callback;
         if(callback->lru_entry->contains_data) {
            callback->io_cb(callback);
            free(linked_cb);
         } else { // page has not been prefetched yet, it's likely in the list of pages that will be read during the next kernel call
            linked_cb->next = ctx->linked_callbacks;
            ctx->linked_callbacks = linked_cb; // re-link our callback
         }
         linked_cb = next;
         nb_linked++;
      }
   } stop_debug_timer(10000, "%lu linked callbacks\n", nb_linked);
}

/*
 * Loop executed by worker threads
 */
static void worker_do_io(struct io_context *ctx) {
   size_t i;
   size_t pending = ctx->sent_io - ctx->processed_io;
   if(pending == 0) {
      ctx->ios_sent_to_disk = 0;
      return;
   }

   for(i = 0; i < pending; i++) {
      struct slab_callback *callback;
      ctx->iocbs[i] = &ctx->iocb[(ctx->processed_io + i)%ctx->max_pending_io];
      callback = (void*)ctx->iocbs[i]->aio_data;

      add_time_in_payload(callback, 3);
   }

   // Submit requests to the kernel
   int ret = io_submit(ctx->ctx, pending, ctx->iocbs);
   if (ret != pending)
      perr("Couldn't submit all io requests! %d submitted / %lu (%lu sent, %lu processed)\n", ret, pending, ctx->sent_io, ctx->processed_io);
   ctx->ios_sent_to_disk = ret;
}


/*
 * do_io = wait for all requests to be completed
 */
void do_io(void) {
   // TODO
}

/* We need a unique hash for each page for the page cache */
static uint64_t get_hash_for_page(int fd, uint64_t page_num) {
   return (((uint64_t)fd)<<40LU)+page_num; // Works for files less than 40EB
}

void read_page_async(struct slab_callback *callback) {
    int ret;
    int slot_idx = callback->slot_idx;
    uint64_t page_idx = callback->page_idx;
    struct io_context *ctx = callback->io_ctx;

    struct iocb *_iocb = &ctx->iocb[slot_idx];
    memset(_iocb, 0, sizeof(*_iocb));
    _iocb->aio_fildes = callback->fd;
    _iocb->aio_lio_opcode = IOCB_CMD_PREAD;
    _iocb->aio_buf = (uint64_t)callback->page_buffer;
    _iocb->aio_data = (uint64_t)callback;
    _iocb->aio_offset = page_idx * page_size;
    _iocb->aio_nbytes = page_size;

    ctx->iocbs[slot_idx] = &ctx->iocb[slot_idx];
    ret = io_submit(ctx->ctx, 1, &(ctx->iocbs[slot_idx]));
    assert(ret == 1);
}

void write_page_async(struct slab_callback *callback) {
    int ret;
    int slot_idx = callback->slot_idx;
    struct io_context *ctx = callback->io_ctx;
    void *disk_page = callback->page_buffer;
    uint64_t page_idx = callback->page_idx;

    // TODO: maybe no need to refill some fields?
    struct iocb *_iocb = &ctx->iocb[slot_idx];
    memset(_iocb, 0, sizeof(*_iocb));
    _iocb->aio_fildes = callback->fd;
    _iocb->aio_lio_opcode = IOCB_CMD_PWRITE;
    _iocb->aio_buf = (uint64_t)disk_page;
    _iocb->aio_data = (uint64_t)callback;
    _iocb->aio_offset = page_idx * page_size;
    _iocb->aio_nbytes = page_size;

    ctx->iocbs[slot_idx] = &ctx->iocb[slot_idx];
    ret = io_submit(ctx->ctx, 1, &(ctx->iocbs[slot_idx]));
    assert(ret == 1);
}

/*
 * Init an IO worker
 */
struct io_context *worker_ioengine_init(size_t depth) {
    int ret;
    struct io_context *ctx = calloc(1, sizeof(struct io_context));
    
    ctx->iocb = calloc(depth, sizeof(struct iocb));
    ctx->iocbs = calloc(depth, sizeof(struct iocb *));
    ctx->events = calloc(depth, sizeof(struct io_event));

    ret = io_setup(depth, &ctx->ctx);
    if(ret < 0) {
        perr("Cannot create aio setup\n");
    }

    return ctx;
}

/* Enqueue requests */
void worker_ioengine_enqueue_ios(struct io_context *ctx) {
   worker_do_io(ctx); // Process IO queue
}

/* Get processed requests from disk and call callbacks */
void worker_ioengine_get_completed_ios(struct io_context *ctx) {
   int ret = 0;
   declare_debug_timer;

   if(ctx->ios_sent_to_disk == 0)
      return;

   //printf("worker_ioengine_get_completed_ios...\n");

   start_debug_timer {
      ret = io_getevents(ctx->ctx, ctx->ios_sent_to_disk - ret, ctx->ios_sent_to_disk - ret, &ctx->events[ret], NULL);
      if(ret != ctx->ios_sent_to_disk)
         die("Problem: only got %d answers out of %lu enqueued IO requests\n", ret, ctx->ios_sent_to_disk);
   } stop_debug_timer(10000, "io_getevents took more than 10ms!!");
}


void worker_ioengine_process_completed_ios(struct io_context *ctx) {
   int ret = ctx->ios_sent_to_disk;
   declare_debug_timer;
   size_t i;

   if(ctx->ios_sent_to_disk == 0)
      return;

   start_debug_timer {
      // Enqueue completed IO requests
      for(i = 0; i < ret; i++) {
         struct iocb *cb = (void*)ctx->events[i].obj;
         struct slab_callback *callback = (void*)cb->aio_data;
         assert(ctx->events[i].res == 4096);
         callback->io_cb(callback);
      }

      // We might have "linked callbacks" so process them
      // process_linked_callbacks(ctx);
   } stop_debug_timer(10000, "rest of worker_ioengine_process_completed_ios (%d requests)", ret);

   // Ok, now the main thread can push more requests
   ctx->processed_io += ctx->ios_sent_to_disk;
}

int io_pending(struct io_context *ctx) {
   return ctx->sent_io - ctx->processed_io;
}
