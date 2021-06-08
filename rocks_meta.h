#ifndef ROCKS_META_H
#define ROCKS_META_H 1

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#if defined (__cplusplus)
extern "C" {
#endif

#include <stdint.h>

void init_rocks(char *path);
void write_into_rocks(char *item, uint64_t key_size, uint64_t value_size);
void write_into_rocks_batch(char *item, uint64_t size);

void *rocks_iterator_init();
void rocks_Seek(void *iter, char *buf, uint32_t size);
void rocks_SeekToFirst(void *iter);
int rocks_Valid(void *iter);
char *rocks_GetKey(void *iter, uint32_t *size);
void rocks_Next(void *iter);
void rocks_iterator_del(void *iter);

#if defined (__cplusplus)
}
#endif

#endif
