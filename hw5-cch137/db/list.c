#include "utils.h"
#include "obj.h"
#include "list.h"

DBListNode *create_dblistnode(DBObj *data)
{
  DBListNode *node = malloc(sizeof(DBListNode));
  if (!node)
    EXIT_ON_MEMORY_ERROR();
  node->data = data;
  node->prev = NULL;
  node->next = NULL;
  return node;
}

DBListNode *create_dblistnode_with_string(char *data)
{
  return create_dblistnode(dbobj_create_string(dbutil_strdup(data)));
}

DBList *create_dblist()
{
  DBList *list = malloc(sizeof(DBList));
  if (!list)
    EXIT_ON_MEMORY_ERROR();
  list->head = NULL;
  list->tail = NULL;
  list->length = 0;
  return list;
}

void free_dblistnode(DBListNode *node)
{
  if (!node)
    return;
  break_dblistnodes(node, node->next);
  break_dblistnodes(node->prev, node);
  free_dbobj(node->data);
  free(node);
}

char *extract_dblistnode_string(DBListNode *node)
{
  if (!node)
    return NULL;

  char *data = dbobj_extract_string(node->data);
  node->data = NULL;
  free_dblistnode(node);

  return data;
}

void clear_dblist(DBList *list)
{
  if (!list)
    return;

  DBListNode *curr = list->head;

  while (curr)
  {
    list->head = curr->next;
    free_dblistnode(curr);
    curr = list->head;
  }

  list->tail = NULL;
  list->length = 0;
}

void free_dblist(DBList *list)
{
  if (!list)
    return;
  clear_dblist(list);
  free(list);
}

void join_dblistnodes(DBListNode *left_node, DBListNode *right_node)
{
  if (left_node)
    left_node->next = right_node;
  if (right_node)
    right_node->prev = left_node;
}

void break_dblistnodes(DBListNode *left_node, DBListNode *right_node)
{
  if (left_node)
    left_node->next = NULL;
  if (right_node)
    right_node->prev = NULL;
}

db_uint_t lpush(DBList *list, DBListNode *node)
{
  if (!list)
    return 0;

  while (node)
  {
    join_dblistnodes(node, list->head);
    list->head = node;
    list->length++;
    node = node->prev;
  }

  if (list->head)
  {
    list->head->prev = NULL;
  }

  if (!list->tail)
  {
    list->tail = list->head;
  }

  return list->length;
}

DBListNode *lpop(DBList *list)
{
  if (!list || !list->head)
    return NULL;

  DBListNode *removed_node = list->head;

  list->head = removed_node->next;
  break_dblistnodes(removed_node, list->head);
  --list->length;

  if (!list->head)
  {
    list->tail = NULL;
  }

  return removed_node;
}

DBList *lpop_n(DBList *list, db_uint_t count)
{
  if (!list || !list->head || !count)
    return NULL;

  DBListNode *curr = NULL;
  DBList *reply_list = create_dblist();

  while (count--)
  {
    curr = lpop(list);
    if (curr)
    {
      rpush(reply_list, curr);
    }
    else
    {
      break;
    }
  }

  return reply_list;
}

db_uint_t rpush(DBList *list, DBListNode *node)
{
  if (!list || !node)
    return 0;

  while (node)
  {
    join_dblistnodes(list->tail, node);
    list->tail = node;
    list->length++;
    node = node->next;
  }

  if (!list->head)
  {
    list->head = list->tail;
  }

  return list->length;
}

DBListNode *rpop(DBList *list)
{
  if (!list || !list->tail)
    return NULL;

  DBListNode *removed_node = list->tail;

  list->tail = removed_node->prev;
  break_dblistnodes(list->tail, removed_node);
  --list->length;

  if (!list->tail)
  {
    list->head = NULL;
  }

  return removed_node;
}

DBList *rpop_n(DBList *list, db_uint_t count)
{
  if (!list || !list->tail || !count)
    return NULL;

  DBListNode *curr = NULL;
  DBList *reply_list = create_dblist();

  while (count--)
  {
    curr = rpop(list);
    if (curr)
    {
      rpush(reply_list, curr);
    }
    else
    {
      break;
    }
  }

  return reply_list;
}

DBList *lrange(const DBList const *list, db_uint_t start, db_uint_t stop)
{
  DBList *reply_list = create_dblist();

  if (!list)
  {
    return reply_list;
  }

  if (stop == DB_UINT_MAX || stop > list->length - 1)
  {
    stop = list->length - 1;
  }

  if (start > stop || start >= list->length)
  {
    return NULL;
  }

  // The new node must be initialized to NULL,
  // as it will be assigned to the reply list regardless of whether it has been created.
  DBListNode *new_node = NULL;
  DBListNode *last_new_node = NULL;
  DBListNode *curr_node;
  db_uint_t index;

  if (start > list->length - 1 - stop)
  {
    curr_node = list->tail;
    index = list->length - 1;
    while (index != stop && curr_node)
    {
      curr_node = curr_node->prev;
      --index;
    }
    while (index >= start && curr_node)
    {
      new_node = create_dblistnode_with_string(curr_node->data->value.string);
      join_dblistnodes(new_node, last_new_node);
      last_new_node = new_node;
      if (!reply_list->tail)
        reply_list->tail = new_node;
      curr_node = curr_node->prev;
      --index;
    }
    reply_list->head = new_node;
  }
  else
  {
    curr_node = list->head;
    index = 0;
    while (index != start && curr_node)
    {
      curr_node = curr_node->next;
      ++index;
    }
    while (index <= stop && curr_node)
    {
      new_node = create_dblistnode_with_string(curr_node->data->value.string);
      join_dblistnodes(last_new_node, new_node);
      last_new_node = new_node;
      if (!reply_list->head)
        reply_list->head = new_node;
      curr_node = curr_node->next;
      ++index;
    }
    reply_list->tail = new_node;
  }

  reply_list->length = stop - start + 1;

  return reply_list;
}
