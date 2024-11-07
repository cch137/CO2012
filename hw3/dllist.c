#include <limits.h>
#include <stdarg.h>
#include "interface.h"
#include "dllist.h"

typedef struct DLNode
{
  char *data;
  struct DLNode *prev;
  struct DLNode *next;
} DLNode;

typedef struct DLList
{
  DLNode *head;
  DLNode *tail;
  size_t length;
} DLList;

DLNode *create_DLNode(const char *data, DLNode *prev, DLNode *next)
{
  DLNode *node = malloc(sizeof(DLNode));
  if (!node)
    memory_error_handler(__FILE__, __LINE__, __func__);
  node->data = helper_strdup(data);
  node->prev = prev;
  node->next = next;
  return node;
}

DLList *create_DList()
{
  DLList *list = malloc(sizeof(DLList));
  if (!list)
    memory_error_handler(__FILE__, __LINE__, __func__);
  list->head = NULL;
  list->tail = NULL;
  list->length = 0;
  return list;
}

DLList *duplicate_DList(DLList *list)
{
  DLList *new_list = create_DList();
  DLNode *curr_node = list->head;

  while (curr_node)
  {
    new_list->head = create_DLNode(curr_node->data, NULL, new_list->head);
    curr_node = curr_node->next;
  }

  new_list->length = list->length;

  return new_list;
}

bool DL_free_node(DLNode *node)
{
  if (!node)
    return false;

  DLNode *curr_neighbour = node->prev;
  DLNode *next_neighbour = NULL;

  while (curr_neighbour)
  {
    next_neighbour = curr_neighbour->prev;
    free(curr_neighbour->data);
    free(curr_neighbour);
    curr_neighbour = next_neighbour;
  }

  curr_neighbour = node->next;
  while (curr_neighbour)
  {
    next_neighbour = curr_neighbour->next;
    free(curr_neighbour->data);
    free(curr_neighbour);
    curr_neighbour = next_neighbour;
  }

  free(node->data);
  free(node);

  return true;
}

bool DL_free_list(DLList *list)
{
  if (!list)
    return false;

  DLNode *curr_node = list->head;
  DLNode *next_node;

  while (curr_node)
  {
    next_node = curr_node->next;
    DL_free_node(curr_node);
    curr_node = next_node;
  }

  free(list);

  return true;
}

size_t DL_lpush(DLList *list, int args_len, ...)
{
  if (!list)
    return 0;

  va_list args;
  va_start(args, args_len);

  DLNode *node;

  for (int i = 0; i < args_len; i++)
  {
    // `create_DLNode` safely handles the data value;
    // input data checks are unnecessary.
    node = create_DLNode((char *)va_arg(args, char *), NULL, list->head);
    if (list->head)
      list->head->prev = node;
    list->head = node;
    list->length++;
  }

  return list->length;
}

DLNode *DL_lpop(DLList *list, size_t count)
{
  if (!list || count == 0)
    return NULL;

  DLNode *node = list->head;

  if (!node)
    return NULL;

  size_t counted = 1;

  while (counted < count)
  {
    if (!node->next)
      break;
    node = node->next;
    counted++;
  }

  list->head = node->next;

  if (node->next)
  {
    node->next->prev = NULL;
    node->next = NULL;
  }

  list->length -= counted;

  return node;
}

size_t DL_rpush(DLList *list, int args_len, ...)
{
  if (!list)
    return 0;

  va_list args;
  va_start(args, args_len);

  DLNode *node;

  for (int i = 0; i < args_len; i++)
  {
    node = create_DLNode((char *)va_arg(args, char *), list->tail, NULL);
    if (list->tail)
      list->tail->next = node;
    list->tail = node;
    list->length++;
  }

  return list->length;
}

DLNode *DL_rpop(DLList *list, size_t count)
{
  if (!list || count == 0)
    return NULL;

  DLNode *node = list->tail;

  if (!node)
    return NULL;

  size_t counted = 1;

  while (counted < count)
  {
    if (!node->prev)
      break;
    node = node->prev;
    counted++;
  }

  list->tail = node->prev;

  if (node->prev)
  {
    node->prev->next = NULL;
    node->prev = NULL;
  }

  list->length -= counted;

  return node;
}

size_t DL_llen(DLList *list)
{
  return list ? list->length : 0;
}

DLList *DL_lrange(DLList *list, size_t start, size_t stop)
{
  if (!list)
    return NULL;

  if (stop == ULONG_MAX)
    return duplicate_DList(list);

  DLList *new_list = create_DList();

  size_t index = 0;

  DLNode *curr_node = list->head;

  while (index < start)
  {
    curr_node = curr_node->next;
    if (!curr_node)
      break;
    index++;
  }

  while (index < stop)
  {
    if (!curr_node)
      break;
    new_list->head = create_DLNode(helper_strdup(curr_node->data), NULL, new_list->head);
    new_list->length++;
    curr_node = curr_node->next;
    index++;
  }

  return new_list;
}
