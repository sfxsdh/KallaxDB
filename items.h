#ifndef ITEMS_H
#define ITEMS_H

#include "headers.h"

/*
 * Items are the data that is persisted on disk.
 * The metadata structs should not contain any pointer because they are persisted on disk.
 */

struct item_metadata {
   uint64_t seq_id;
   uint64_t key_size;
   uint64_t value_size;
};

#endif
