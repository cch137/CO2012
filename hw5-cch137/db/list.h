#ifndef DB_list
#define DB_list

#include "obj.h"
#include "types.h"

void join_dblistnodes(DBListNode *left_node, DBListNode *right_node);
void break_dblistnodes(DBListNode *left_node, DBListNode *right_node);

DBListNode *create_dblistnode(DBObj *data);

// Creates a new node for a doubly linked list with specified data
DBListNode *create_dblistnode_with_string(char *data);

// Initializes a new, empty doubly linked list
DBList *create_dblist();

// Frees a list node
void free_dblistnode(DBListNode *node);

// Free node and return node value
char *extract_dblistnode_string(DBListNode *node);

// Removes andd frees all node in a list
void clear_dblist(DBList *list);

// Frees an entire list and all of its nodes
void free_dblist(DBList *list);

// Pushes elements to the front of a list; last parameter must be NULL
db_uint_t lpush(DBList *list, DBListNode *node);

// Pops a elements from the front of a list
DBListNode *lpop(DBList *list);

// Pops elements from the front of a list
DBList *lpop_n(DBList *list, db_uint_t count);

// Pushes elements to the end of a list; last parameter must be NULL
db_uint_t rpush(DBList *list, DBListNode *node);

// Pops a element from the end of a list
DBListNode *rpop(DBList *list);

// Pops elements from the end of a list
DBList *rpop_n(DBList *list, db_uint_t count);

// Returns a sublist from the specified range of indices
// The `stop` index is inclusive, and if `stop` is -1, the entire list is returned
DBList *lrange(const DBList const *list, db_uint_t start, db_uint_t stop);

#endif
