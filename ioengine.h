#ifndef IOENGINE_H
#define IOENGINE_H 1

struct io_context {
   aio_context_t ctx __attribute__((aligned(64)));
   volatile size_t sent_io;
   volatile size_t processed_io;
   size_t max_pending_io;
   size_t ios_sent_to_disk;
   struct iocb *iocb;
   struct iocb **iocbs;
   struct io_event *events;
   struct linked_callbacks *linked_callbacks;
};

struct io_context *worker_ioengine_init(size_t nb_callbacks);
//size_t io_getevents_wrapper(struct slab_context *ctx, long min_nr, long max_nr);
size_t io_getevents_wrapper(struct io_context *ctx, long min_nr, long max_nr);

void *safe_pread(int fd, off_t offset);
void *safe_pwrite(int fd, off_t offset, char * disk_page);

typedef void (io_cb_t)(struct slab_callback *);
//char *read_page_async(struct slab_callback *cb);
//char *write_page_async(struct slab_callback *cb);
void read_page_async(struct slab_callback *cb);
void write_page_async(struct slab_callback *cb);

int io_pending(struct io_context *ctx);

void worker_ioengine_enqueue_ios(struct io_context *ctx);
void worker_ioengine_get_completed_ios(struct io_context *ctx);
void worker_ioengine_process_completed_ios(struct io_context *ctx);



#endif
