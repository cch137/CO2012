#ifndef DOUBLY_LINKED_LIST_H
#define DOUBLY_LINKED_LIST_H

#include <stdlib.h>
#include <stdbool.h>

// Node structure for the doubly linked list
typedef struct DLNode
{
  char *data;
  struct DLNode *prev;
  struct DLNode *next;
} DLNode;

// Doubly linked list structure
typedef struct DLList
{
  DLNode *head;
  DLNode *tail;
  size_t length;
} DLList;

/**
 * Creates a new node with specified data, prev, and next pointers.
 * @param data Data for the node.
 * @param prev Pointer to the previous node.
 * @param next Pointer to the next node.
 * @return Pointer to the newly created node.
 */
DLNode *create_DLNode(char *data, DLNode *prev, DLNode *next);

/**
 * Initializes a new, empty doubly linked list.
 * @return Pointer to the newly created empty list.
 */
DLList *create_DList();

/**
 * Creates a duplicate of an existing doubly linked list.
 * @param list Pointer to the list to duplicate.
 * @return Pointer to the duplicated list.
 */
DLList *duplicate_DList(DLList *list);

/**
 * Frees a single node and its associated data.
 * @param node Pointer to the node to free.
 * @return True if successful, false if the node was NULL.
 */
bool DL_free_node(DLNode *node);

/**
 * Frees an entire doubly linked list and all of its nodes.
 * @param list Pointer to the list to free.
 * @return True if successful, false if the list was NULL.
 */
bool DL_free_list(DLList *list);

/**
 * Pushes one or more elements to the front of the list.
 * @param list Pointer to the list.
 * @param args_len Number of elements to push.
 * @param ... Variable arguments for each element to push.
 * @return The new length of the list.
 */
size_t DL_lpush(DLList *list, int args_len, ...);

/**
 * Removes and returns the first node from the list.
 * @param list Pointer to the list.
 * @param count Number of nodes to remove from the front.
 * @return Pointer to the removed node, or NULL if the list is empty.
 */
DLNode *DL_lpop(DLList *list, size_t count);

/**
 * Pushes one or more elements to the end of the list.
 * @param list Pointer to the list.
 * @param args_len Number of elements to push.
 * @param ... Variable arguments for each element to push.
 * @return The new length of the list.
 */
size_t DL_rpush(DLList *list, int args_len, ...);

/**
 * Removes and returns the last node from the list.
 * @param list Pointer to the list.
 * @param count Number of nodes to remove from the end.
 * @return Pointer to the removed node, or NULL if the list is empty.
 */
DLNode *DL_rpop(DLList *list, size_t count);

/**
 * Returns the number of nodes in the list.
 * @param list Pointer to the list.
 * @return The length of the list.
 */
size_t DL_llen(DLList *list);

/**
 * Returns a sublist from the specified range of indices.
 * @param list Pointer to the list.
 * @param start Starting index of the range.
 * @param stop Ending index of the range. If -1, the sublist will extend to the end of the list.
 * @return Pointer to a new list containing the specified range.
 */
DLList *DL_lrange(DLList *list, size_t start, size_t stop);

#endif
