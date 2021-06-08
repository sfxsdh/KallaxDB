#include "wal_rbtree.h"
#include <assert.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>


//typedef rb_node node;



static rb_node* grandparent(rb_node* n);
static rb_node* sibling(rb_node* n);
static rb_node* uncle(rb_node* n);
static void verifyProperties(rbtree* t);
static color nodeColor(rb_node* n);

static void replaceNode(rbtree* t, rb_node* oldn, rb_node* newn) ;
static void rotateLeft(rbtree* t, rb_node* n);
static void rotateRight(rbtree* t, rb_node* n);

static rb_node* createRBNode(slice key, slice value, color node_color, rb_node* left, rb_node* right);
static rb_node* createDefaultRBNode(slice key, slice value);

static void deleteRBtreeImpl(rb_node* root);

static void insertCase1(rbtree* t, rb_node* n);
static void insertCase2(rbtree* t, rb_node* n);
static void insertCase3(rbtree* t, rb_node* n);
static void insertCase4(rbtree* t, rb_node* n);
static void insertCase5(rbtree* t, rb_node* n);

static rb_node* maxNode(rb_node* root);

static void deleteCase1(rbtree* t, rb_node* n);
static void deleteCase2(rbtree* t, rb_node* n);
static void deleteCase3(rbtree* t, rb_node* n);
static void deleteCase4(rbtree* t, rb_node* n);
static void deleteCase5(rbtree* t, rb_node* n);
static void deleteCase6(rbtree* t, rb_node* n);

static uint64_t decodeFixed64(const char* ptr, int littleEndian);
static uint32_t decodeFixed32(const char* ptr, int littleEndian);
static uint64_t parseKeySeqnumber(const char* ptr);



#ifdef VERIFY_RBTREE
static void verifyProperty1(rb_node* root);
static void verifyProperty2(rb_node* root);
static void verifyProperty4(rb_node* root);
static void verifyProperty5(rb_node* root);
static void verifyProperty5_helper(rb_node* n, int black_count, int* black_count_path);
#endif

rb_node* grandparent(rb_node* n) {
   assert (n != NULL);
   assert (n->parent != NULL); /* Not the root node */
   assert (n->parent->parent != NULL); /* Not child of root */
   return n->parent->parent;
}

rb_node* sibling(rb_node* n) {
   assert (n != NULL);
   assert (n->parent != NULL); /* Root node has no sibling */
   if (n == n->parent->left)
      return n->parent->right;
   else
      return n->parent->left;
}

rb_node* uncle(rb_node* n) {
   assert (n != NULL);
   assert (n->parent != NULL); /* Root node has no uncle */
   assert (n->parent->parent != NULL); /* Children of root have no uncle */
   return sibling(n->parent);
}

color nodeColor(rb_node* n) {
    return n == NULL ? BLACK : n->color;
}

uint64_t decodeFixed64(const char* ptr, int littleEndian) {
    if (littleEndian) {
      uint64_t result;
      memcpy(&result, ptr, sizeof(result));
      return result;
    } else {
      uint64_t lo = decodeFixed32(ptr, littleEndian);
      uint64_t hi = decodeFixed32(ptr + 4, littleEndian);
      return (hi << 32) | lo;
    }
}
uint32_t decodeFixed32(const char* ptr, int littleEndian) {
    if (littleEndian) {
      // Load the raw bytes
      uint32_t result;
      memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
      return result;
    } else {
      return ((uint32_t)((unsigned char)(ptr[0])))
          | (((uint32_t)(unsigned char)(ptr[1])) << 8)
          | (((uint32_t)(unsigned char)(ptr[2])) << 16)
          | (((uint32_t)(unsigned char)(ptr[3])) << 24);
    }
}
uint64_t parseKeySeqnumber(const char* ptr){
    uint64_t num = decodeFixed64(ptr, 0);
    //printf("parse sequence num:%ld\n", num >> 8);
    return (num >> 8);
}


int normalCompare(slice left, slice right) {
     assert(left.data != NULL);
     assert(right.data != NULL);
     //assert(left.len > 8);
     //assert(right.len > 8);
     int data_ret = memcmp(left.data, right.data, left.len);
     return data_ret;
}

int userKeyCompare(slice left, slice right) {
    assert(left.data != NULL);
    assert(right.data != NULL);
    assert(left.len > 8);
    assert(right.len > 8);
    int data_ret = memcmp(left.data, right.data, left.len - 8);
    return data_ret;
}

int seqCompare(slice left, slice right) {
    assert(left.data != NULL);
    assert(right.data != NULL);
    assert(left.len > 8);
    assert(right.len > 8);

    //int data_ret = memcmp(left.data + left.len - 8, right.data + right.len - 8, 8);
    uint64_t left_seq = *(uint64_t*)(left.data + left.len - 8);
    uint64_t right_seq = *(uint64_t*)(right.data + right.len - 8);
    if (left_seq < right_seq) {
        return -1;
    } else if(left_seq == right_seq) {
        return 0;
    } else {
        return 1;
    }
}

rbtree* createRBtree(my_compare_func compare) {
   rbtree* t = malloc(sizeof(struct rbtree_t));
   t->root = NULL;
   t->last_visited_node = NULL;
   t->nb_elements = 0;
   t->compare = compare;
   return t;
}

rb_node* createRBNode(slice key, slice value, color node_color, rb_node* left, rb_node* right) {
   rb_node* result = malloc(sizeof(struct rb_node_t));
   result->key = key;
   result->value = value;
   result->color = node_color;
   result->left = left;
   result->right = right;
   if (left  != NULL)  {
       left->parent = result;
   }

   if (right != NULL) {
       right->parent = result;
   }
   result->parent = NULL;
   return result;
}

rb_node* createDefaultRBNode(slice key, slice value){
    return createRBNode(key, value, RED, NULL, NULL);
}

void deleteRBtree(rbtree* t) {
    rb_node* root = t->root;
    deleteRBtreeImpl(root);
    free(t);
    t = NULL;
}

void deleteRBtreeImpl(rb_node* root) {
    if (root == NULL) {
        return;
    }
    if (root->left) {
        deleteRBtreeImpl(root->left);
    }
    if (root->right) {
        deleteRBtreeImpl(root->right);
    }

    if (root->key.data != NULL) {
        free(root->key.data);
        root->key.data = NULL;
    }

    if (root->value.data != NULL) {
        free(root->value.data);
        root->value.data = NULL;
    }

    free(root);
    root = NULL;
    return;
}


rb_node* lookupRBNode(rbtree* t, slice key) {
    t->compare = normalCompare;
    rb_node* n = t->root;
    while (n != NULL) {
       int comp_result = (t->compare)(key, n->key);
       if (comp_result == 0) {
          t->last_visited_node = n;
          return n;
       } else if (comp_result < 0) {
          n = n->left;
       } else {
          assert(comp_result > 0);
          n = n->right;
       }
    }

    return n;
}

void lookupMinRBNode(rbtree* t, slice* res_key, slice* res_value) {
    t->compare = normalCompare;
    if (t == NULL || t->root == NULL) {
      res_key->data = NULL;
      res_key->len = 0;
      res_value->data = NULL;
      res_value->len = 0;
      return;
    }
    rb_node* n = t->root;

    while (n->left != NULL) {
        n = n->left;
    }

    assert(res_key != NULL);
    assert(res_value != NULL);
    res_key->len = n->key.len;
    res_key->data = malloc(res_key->len);
    memcpy(res_key->data, n->key.data, res_key->len);

    res_value->len = n->value.len;
    res_value->data = malloc(res_value->len);
    memcpy(res_value->data, n->value.data, res_value->len);

    // delete the min value node
    rb_node* child;
    slice free_key = n->key;
    slice free_value = n->value;
    t->nb_elements--;
    if (n->left != NULL && n->right != NULL) {
       /* Copy key/value from predecessor and then delete it instead */
       rb_node* pred = maxNode(n->left);
       n->key   = pred->key;
       n->value = pred->value;
       n = pred;
    }
    assert(n->left == NULL || n->right == NULL);
    child = n->right == NULL ? n->left  : n->right;
    if (nodeColor(n) == BLACK) {
       n->color = nodeColor(child);
       deleteCase1(t, n);
    }
    replaceNode(t, n, child);
    if (n->parent == NULL && child != NULL) {
        child->color = BLACK;
    }

    if (free_key.data != NULL) {
        free(free_key.data);
        free_key.data = NULL;
    }

    if (free_value.data != NULL) {
        free(free_value.data);
        free_value.data = NULL;
    }

    free(n);
    n = NULL;
    verifyProperties(t);
    t->last_visited_node = NULL;
}

void replaceNode(rbtree* t, rb_node* oldn, rb_node* newn) {
   if (oldn->parent == NULL) {
      t->root = newn;
   } else {
      if (oldn == oldn->parent->left) {
          oldn->parent->left = newn;
      } else {
          oldn->parent->right = newn;
      }
   }

   if (newn != NULL) {
      newn->parent = oldn->parent;
   }
}

void rotateLeft(rbtree* t, rb_node* n) {
    rb_node* r = n->right;
    replaceNode(t, n, r);
    n->right = r->left;
    if (r->left != NULL) {
       r->left->parent = n;
    }
    r->left = n;
    n->parent = r;
}


void rotateRight(rbtree* t, rb_node* n) {
    rb_node* l = n->left;
    replaceNode(t, n, l);
    n->left = l->right;
    if (l->right != NULL) {
       l->right->parent = n;
    }
    l->right = n;
    n->parent = l;
}

void insertRBNode(rbtree* t, slice key, slice value) {
    rb_node* inserted_node = createDefaultRBNode(key, value);
    /* Classic hack to speed up the find & insert case */
    t->compare = userKeyCompare;
    if (t->last_visited_node && (t->compare)(key, t->last_visited_node->key) == 0) {
        int seq_res = seqCompare(key, t->last_visited_node->key);
        if (seq_res >= 0) {
            free(t->last_visited_node->key.data);
            free(t->last_visited_node->value.data);
            t->last_visited_node->key = key;
            t->last_visited_node->value = value;
        } else {
            free(key.data);
            free(value.data);
        }
        free(inserted_node);
       return;
    } else if (t->root == NULL) {
       t->root = inserted_node;
       t->nb_elements = 1;
    } else {
       rb_node* n = t->root;
       while (1) {
          int comp_result = (t->compare)(key, n->key);
          if (comp_result == 0) {
             int seq_res = seqCompare(key, n->key);
             if (seq_res >= 0) {
                 free(n->key.data);
                 free(n->value.data);
                 n->key = key;
                 n->value = value;
             } else {
                 free(key.data);
                 free(value.data);
             }
             free(inserted_node);
             return;
          } else if (comp_result < 0) {
             if (n->left == NULL) {
                n->left = inserted_node;
                t->nb_elements++;
                break;
             } else {
                n = n->left;
             }
          } else {
             assert (comp_result > 0);
             if (n->right == NULL) {
                n->right = inserted_node;
                t->nb_elements++;
                break;
             } else {
                n = n->right;
             }
          }
       }
       inserted_node->parent = n;
    }
    insertCase1(t, inserted_node);
    verifyProperties(t);
}

void insertCase1(rbtree* t, rb_node* n) {
   if (n->parent == NULL)
      n->color = BLACK;
   else
      insertCase2(t, n);
}

void insertCase2(rbtree* t, rb_node* n) {
   if (nodeColor(n->parent) == BLACK)
      return; /* Tree is still valid */
   else
      insertCase3(t, n);
}

void insertCase3(rbtree* t, rb_node* n) {
   if (nodeColor(uncle(n)) == RED) {
      n->parent->color = BLACK;
      uncle(n)->color = BLACK;
      grandparent(n)->color = RED;
      insertCase1(t, grandparent(n));
   } else {
      insertCase4(t, n);
   }
}

void insertCase4(rbtree* t, rb_node* n) {
   if (n == n->parent->right && n->parent == grandparent(n)->left) {
      rotateLeft(t, n->parent);
      n = n->left;
   } else if (n == n->parent->left && n->parent == grandparent(n)->right) {
      rotateRight(t, n->parent);
      n = n->right;
   }
   insertCase5(t, n);
}

void insertCase5(rbtree* t, rb_node* n) {
   n->parent->color = BLACK;
   grandparent(n)->color = RED;
   if (n == n->parent->left && n->parent == grandparent(n)->left) {
      rotateRight(t, grandparent(n));
   } else {
      assert (n == n->parent->right && n->parent == grandparent(n)->right);
      rotateLeft(t, grandparent(n));
   }
}

void removeRBNode(rbtree* t, slice key) {
    t->compare = normalCompare;
    rb_node* child;
    rb_node* n = lookupRBNode(t, key);
    if (n == NULL) {
    	return;
    }
    slice free_key = n->key;
    slice free_value = n->value;
    if (n == NULL) return;  /* Key not found, do nothing */
    t->nb_elements--;
    if (n->left != NULL && n->right != NULL) {
       /* Copy key/value from predecessor and then delete it instead */
       rb_node* pred = maxNode(n->left);
       n->key   = pred->key;
       n->value = pred->value;
       n = pred;
    }
    assert(n->left == NULL || n->right == NULL);
    child = n->right == NULL ? n->left  : n->right;
    if (nodeColor(n) == BLACK) {
       n->color = nodeColor(child);
       deleteCase1(t, n);
    }
    replaceNode(t, n, child);
    if (n->parent == NULL && child != NULL) {
        child->color = BLACK;
    }

    if (free_key.data != NULL) {
        free(free_key.data);
        free_key.data = NULL;
    }

    if (free_value.data != NULL) {
        free(free_value.data);
        free_value.data = NULL;
    }

    free(n);
    n = NULL;
    verifyProperties(t);
    t->last_visited_node = NULL;

}

rb_node* maxNode(rb_node* n) {
   assert (n != NULL);
   while (n->right != NULL) {
      n = n->right;
   }
   return n;
}

void deleteCase1(rbtree* t, rb_node* n) {
   if (n->parent == NULL)
      return;
   else
      deleteCase2(t, n);
}

void deleteCase2(rbtree* t, rb_node* n) {
   if (nodeColor(sibling(n)) == RED) {
      n->parent->color = RED;
      sibling(n)->color = BLACK;
      if (n == n->parent->left)
         rotateLeft(t, n->parent);
      else
         rotateRight(t, n->parent);
   }
   deleteCase3(t, n);
}

void deleteCase3(rbtree* t, rb_node* n) {
   if (nodeColor(n->parent) == BLACK &&
         nodeColor(sibling(n)) == BLACK &&
         nodeColor(sibling(n)->left) == BLACK &&
         nodeColor(sibling(n)->right) == BLACK)
   {
      sibling(n)->color = RED;
      deleteCase1(t, n->parent);
   }
   else
      deleteCase4(t, n);
}

void deleteCase4(rbtree* t, rb_node* n) {
   if (nodeColor(n->parent) == RED &&
         nodeColor(sibling(n)) == BLACK &&
         nodeColor(sibling(n)->left) == BLACK &&
         nodeColor(sibling(n)->right) == BLACK)
   {
      sibling(n)->color = RED;
      n->parent->color = BLACK;
   }
   else
      deleteCase5(t, n);
}

void deleteCase5(rbtree* t, rb_node* n) {
   if (n == n->parent->left &&
         nodeColor(sibling(n)) == BLACK &&
         nodeColor(sibling(n)->left) == RED &&
         nodeColor(sibling(n)->right) == BLACK)
   {
      sibling(n)->color = RED;
      sibling(n)->left->color = BLACK;
      rotateRight(t, sibling(n));
   }
   else if (n == n->parent->right &&
         nodeColor(sibling(n)) == BLACK &&
         nodeColor(sibling(n)->right) == RED &&
         nodeColor(sibling(n)->left) == BLACK)
   {
      sibling(n)->color = RED;
      sibling(n)->right->color = BLACK;
      rotateLeft(t, sibling(n));
   }
   deleteCase6(t, n);
}

void deleteCase6(rbtree* t, rb_node* n) {
   sibling(n)->color = nodeColor(n->parent);
   n->parent->color = BLACK;
   if (n == n->parent->left) {
      assert (nodeColor(sibling(n)->right) == RED);
      sibling(n)->right->color = BLACK;
        rotateLeft(t, n->parent);
   }
   else
   {
      assert (nodeColor(sibling(n)->left) == RED);
      sibling(n)->left->color = BLACK;
      rotateRight(t, n->parent);
   }
}


void verifyProperties(rbtree* t) {
#ifdef VERIFY_RBTREE
   verifyProperty1(t->root);
   verifyProperty2(t->root);
   /* Property 3 is implicit */
   verifyProperty4(t->root);
   verifyProperty5(t->root);
#endif
}




#ifdef VERIFY_RBTREE
void verifyProperty1(rb_node* n) {
   assert(nodeColor(n) == RED || nodeColor(n) == BLACK);
   if (n == NULL) return;
   verifyProperty1(n->left);
   verifyProperty1(n->right);
}

void verifyProperty2(rb_node* root) {
   assert(nodeColor(root) == BLACK);
}

void verifyProperty4(rb_node* n) {
   if (nodeColor(n) == RED) {
      assert (nodeColor(n->left)   == BLACK);
      assert (nodeColor(n->right)  == BLACK);
      assert (nodeColor(n->parent) == BLACK);
   }
   if (n == NULL) return;
   verifyProperty4(n->left);
   verifyProperty4(n->right);
}

void verifyProperty5(rb_node* root) {
   int black_count_path = -1;
   verifyProperty5Impl(root, 0, &black_count_path);
}

void verifyProperty5Impl(rb_node* n, int black_count, int* path_black_count) {
   if (nodeColor(n) == BLACK) {
      black_count++;
   }
   if (n == NULL) {
      if (*path_black_count == -1) {
         *path_black_count = black_count;
      } else {
         assert (black_count == *path_black_count);
      }
      return;
   }
   verifyProperty5Impl(n->left,  black_count, path_black_count);
   verifyProperty5Impl(n->right, black_count, path_black_count);
}
#endif


