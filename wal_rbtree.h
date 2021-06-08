/* Copyright (c) 2011 the authors listed at the following URL, and/or
the authors of referenced articles or incorporated external code:
http://en.literateprograms.org/Red-black_tree_(C)?action=history&offset=20090121005050

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Retrieved from: http://en.literateprograms.org/Red-black_tree_(C)?oldid=16016
... and modified for even more speed and awesomeness...
*/

#ifndef _RBTREE_H_
#define _RBTREE_H_ 1
#include <unistd.h>
#include <stdint.h>
enum rb_node_color { RED, BLACK };
typedef enum rb_node_color color;

typedef struct slice_t {
    char* data;
    uint64_t len;
} slice;

typedef struct rb_node_t {
    slice key;
    slice value;
    struct rb_node_t* left;
    struct rb_node_t* right;
    struct rb_node_t* parent;
    enum rb_node_color color;
} rb_node;

typedef int (*my_compare_func)(slice left, slice right);

typedef struct rbtree_t {
    rb_node* root;
    rb_node* last_visited_node;
    int nb_elements;
    my_compare_func compare;
} rbtree;

int normalCompare(slice left, slice right);
int userKeyCompare(slice left, slice right);
int seqCompare(slice left, slice right);

rbtree* createRBtree(my_compare_func compare);
void deleteRBtree(rbtree* t);
rb_node* lookupRBNode(rbtree* t, slice key);
void lookupMinRBNode(rbtree* t, slice* res_key, slice* res_value);
void insertRBNode(rbtree* t, slice key, slice value);
void removeRBNode(rbtree* t, slice key);

#endif
