#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "types.h"
#include "utils.h"
#include "hash.h"
#include "list.h"
#include "zset.h"

#define SKIPLIST_MAXLEVEL 32
#define SKIPLIST_P 0.25

static int compare_zset_ele(const DBZSetElement *a, const DBZSetElement *b)
{
  if (!a)
    return -1;
  if (!b)
    return 1;
  int s_cmp = a->score - b->score;
  if (s_cmp)
    return s_cmp;
  return strcmp(a->member, b->member);
}

static DBZSetElement *lookup_previos_element_in_level(const DBZSet *zset, const db_uint8_t level, const DBZSetElement *element, DBZSetElement *start_ele)
{
  if (!zset || !element)
    EXIT_ON_ERROR("Invalid ZSet or ZSetElement");
  if (zset->level < level)
    EXIT_ON_ERROR("Invalid ZSet Level");
  DBZSetElement *current = start_ele ? start_ele : zset->sentinel_forward[level];
  if (!current || compare_zset_ele(element, current) < 0)
    return NULL;
  while (current->forward[level] && compare_zset_ele(element, current->forward[level]) > 0)
    current = current->forward[level];
  return current;
}

static DBZSetElement *lookup_first_element_with_score(
    DBZSet *zset,
    db_double_t score,
    bool included_score)
{
  if (!zset)
    EXIT_ON_ERROR("Invalid ZSet");

  if (included_score && zset->sentinel_forward[0]->score == score)
    return zset->sentinel_forward[0];

  DBZSetElement *current = NULL;

  for (int lvl = zset->level - 1; lvl >= 0; --lvl)
  {
    if (!current && zset->sentinel_forward[lvl] && (included_score ? zset->sentinel_forward[lvl]->score < score : zset->sentinel_forward[lvl]->score <= score))
      current = zset->sentinel_forward[lvl];
    while (current && current->forward[lvl] &&
           (included_score ? current->forward[lvl]->score < score
                           : current->forward[lvl]->score <= score))
    {
      current = current->forward[lvl];
    }
  }

  return current && current->forward[0] ? current->forward[0] : current;
}

static DBZSetElement *lookup_last_element_with_score(
    DBZSet *zset,
    db_double_t score,
    bool included_score)
{
  if (!zset)
    EXIT_ON_ERROR("Invalid ZSet");

  DBZSetElement *current = NULL;

  for (int lvl = zset->level - 1; lvl >= 0; --lvl)
  {
    if (!current && zset->sentinel_forward[lvl] &&
        (included_score ? zset->sentinel_forward[lvl]->score <= score
                        : zset->sentinel_forward[lvl]->score < score))
      current = zset->sentinel_forward[lvl];

    while (current && current->forward[lvl] &&
           (included_score ? current->forward[lvl]->score <= score
                           : current->forward[lvl]->score < score))
    {
      current = current->forward[lvl];
    }
  }
  return current;
}

static DBZSetElement *create_zset_ele(db_double_t score, char *member)
{
  db_uint8_t level = 1;
  while (level < SKIPLIST_MAXLEVEL && (rand() & 0xFFFF) < (SKIPLIST_P * 0xFFFF))
  {
    ++level;
  }
  DBZSetElement *new_el = (DBZSetElement *)malloc(sizeof(DBZSetElement));
  if (!new_el)
    EXIT_ON_MEMORY_ERROR();
  new_el->score = score;
  new_el->member = member;
  new_el->level = level;
  new_el->forward = calloc(level, sizeof(DBZSetElement *));
  if (!new_el->forward)
    EXIT_ON_MEMORY_ERROR();
  return new_el;
}

static db_bool_t zset_has_member(DBZSet *zset, const char *member)
{
  if (!zset || !member)
    return false;
  DBHashEntry *element = hget(zset->dict, member, NULL);
  return element ? true : false;
}

DBZSet *zset_create()
{
  DBZSet *zset = (DBZSet *)malloc(sizeof(DBZSet));
  if (!zset)
    EXIT_ON_MEMORY_ERROR();
  zset->dict = ht_create();
  zset->level = 0;
  zset->sentinel_forward = NULL;
  return zset;
}

void free_dbzset(DBZSet *zset)
{
  if (!zset)
    return;
  DBZSetElement *curr = zset->sentinel_forward[0];
  DBZSetElement *next;
  while (curr)
  {
    next = curr->forward[0];
    // we don't free member, because it will be free when free dict
    free(curr->forward);
    free(curr);
    curr = next;
  }
  free(zset->sentinel_forward);
  ht_free(zset->dict);
  free(zset);
}

db_uint_t zadd(DBZSet *zset, db_double_t score, const char *member)
{
  if (!member || !zset)
    return 0;

  zrem(zset, member);
  char *duplicated_member = dbutil_strdup(member);
  DBZSetElement *element = create_zset_ele(score, duplicated_member);
  hset(zset->dict, duplicated_member, _dbobj_create_zsetele(element), NULL);

  // zset up level
  while (zset->level < element->level)
  {
    DBZSetElement **new_sentinel_forward = (DBZSetElement **)realloc(zset->sentinel_forward, (++zset->level) * sizeof(DBZSetElement *));
    if (!new_sentinel_forward)
      EXIT_ON_MEMORY_ERROR();
    zset->sentinel_forward = new_sentinel_forward;
    zset->sentinel_forward[zset->level - 1] = NULL;
  }

  // insert element
  DBZSetElement *previous = NULL;
  for (int lvl = element->level; --lvl >= 0;)
  {
    previous = lookup_previos_element_in_level(zset, lvl, element, previous);
    if (previous == element)
      continue;
    if (previous)
      element->forward[lvl] = previous->forward[lvl],
      previous->forward[lvl] = element;
    else
      element->forward[lvl] = zset->sentinel_forward[lvl],
      zset->sentinel_forward[lvl] = element;
    if (lvl == 0)
      element->backward = previous;
  }
  if (element->forward[0])
    element->forward[0]->backward = element;
  else
    zset->tail = element;

  return zcard(zset);
}

DBObj *zscore(DBZSet *zset, const char *member)
{
  if (!zset || !member)
    return dbobj_create_null();
  DBHashEntry *entry = hget(zset->dict, member, NULL);
  if (!entry)
    return dbobj_create_null();
  return dbobj_create_double(entry->data->value._zsetele->score);
}

db_uint_t zcard(DBZSet *zset)
{
  if (!zset)
    return 0;
  return zset->dict->count0 + zset->dict->count1;
}

db_uint_t zcount(DBZSet *zset, db_double_t min, db_bool_t included_min, db_double_t max, db_bool_t included_max)
{
  if (min >= max)
    return 0;
  DBZSetElement *curr = lookup_first_element_with_score(zset, min, included_min);
  DBZSetElement *last = lookup_last_element_with_score(zset, max, included_max);
  db_uint_t count = 1;
  while (curr && curr != last)
  {
    ++count;
    curr = curr->forward[0];
  }
  return count;
}

DBObj *zinterstore(DBList *zsets, DBList *weights, db_aggregate_t aggregate)
{
  if (!zsets)
    return dbobj_create_error(DB_ERR_WRONGTYPE);

  DBZSet *smallest_set = NULL;

  DBListNode *curr_zset_node = zsets->head;
  DBZSet *curr_zset;
  DBZSetElement *curr_zset_ele;
  db_double_t curr_zset_ele_score;

  DBListNode *curr_weight_node = weights ? weights->head : NULL;
  db_double_t curr_weight;

  DBObj *new_zset_ele_score_obj;
  db_double_t new_zset_ele_score;

  // find smallest set
  while (curr_zset_node)
  {
    if (!dbobj_is_zset(curr_zset_node->data))
      return dbobj_create_error(DB_ERR_WRONGTYPE);
    curr_zset = curr_zset_node->data->value.zset;
    if (!smallest_set || zcard(smallest_set) > zcard(curr_zset))
      smallest_set = curr_zset;
    curr_zset_node = curr_zset_node->next;
  }
  if (!smallest_set)
    return dbobj_create_error(DB_ERR_ARG_ERROR);

  DBZSet *new_zset = zset_create();
  DBList *members = create_dblist();
  char *curr_member;
  bool has_member;

  // makesure all sets has this member
  curr_zset_ele = smallest_set->sentinel_forward[0];
  while (curr_zset_ele)
  {
    has_member = true;
    DBListNode *curr_zset_node = zsets->head;
    while (curr_zset_node)
    {
      curr_zset = curr_zset_node->data->value.zset;
      if (curr_zset != smallest_set && !zset_has_member(curr_zset, curr_zset_ele->member))
      {
        has_member = false;
        break;
      }
      curr_zset_node = curr_zset_node->next;
    }
    if (has_member)
      rpush(members, create_dblistnode_with_string(dbutil_strdup(curr_zset_ele->member)));
    curr_zset_ele = curr_zset_ele->forward[0];
  }

  // aggregate
  curr_member = extract_dblistnode_string(lpop(members));
  while (curr_member)
  {
    curr_zset_node = zsets->head;
    while (curr_zset_node)
    {
      curr_zset = curr_zset_node->data->value.zset;
      curr_weight = curr_weight_node ? curr_weight_node->data->value.double_value : 1;

      new_zset_ele_score_obj = zscore(new_zset, curr_member);
      curr_zset_ele_score = dbobj_extract_double(zscore(curr_zset, curr_member)) * curr_weight;
      switch (aggregate)
      {
      case DB_AGG_SUM:
        new_zset_ele_score = dbobj_extract_double(new_zset_ele_score_obj) + curr_zset_ele_score;
        break;
      case DB_AGG_MIN:
        if (!dbobj_is_double(new_zset_ele_score_obj))
        {
          new_zset_ele_score = curr_zset_ele_score;
          free_dbobj(new_zset_ele_score_obj);
          break;
        }
        new_zset_ele_score = dbobj_extract_double(new_zset_ele_score_obj);
        new_zset_ele_score = new_zset_ele_score < curr_zset_ele_score
                                 ? new_zset_ele_score
                                 : curr_zset_ele_score;
        break;
      case DB_AGG_MAX:
        if (!dbobj_is_double(new_zset_ele_score_obj))
        {
          new_zset_ele_score = curr_zset_ele_score;
          free_dbobj(new_zset_ele_score_obj);
          break;
        }
        new_zset_ele_score = dbobj_extract_double(new_zset_ele_score_obj);
        new_zset_ele_score = new_zset_ele_score > curr_zset_ele_score
                                 ? new_zset_ele_score
                                 : curr_zset_ele_score;
        break;
      default:
        free_dbobj(new_zset_ele_score_obj);
        free_dblist(members);
        free_dbzset(new_zset);
        free(curr_member);
        return dbobj_create_error(DB_ERR_SYNTAX_ERROR);
      }
      zadd(new_zset, new_zset_ele_score, curr_member);

      curr_zset_node = curr_zset_node->next;
      curr_weight_node = curr_weight_node ? curr_weight_node->next : NULL;
    }
    free(curr_member);
    curr_member = extract_dblistnode_string(lpop(members));
  }
  free_dblist(members);
  return dbobj_create_zset(new_zset);
}

DBObj *zunionstore(DBList *zsets, DBList *weights, db_aggregate_t aggregate)
{
  if (!zsets)
    return dbobj_create_error(DB_ERR_WRONGTYPE);

  DBZSet *new_zset = zset_create();

  DBListNode *curr_zset_node = zsets->head;
  DBZSet *curr_zset;
  DBZSetElement *curr_zset_ele;
  db_double_t curr_zset_ele_score;

  DBListNode *curr_weight_node = weights ? weights->head : NULL;
  db_double_t curr_weight;

  DBObj *new_zset_ele_score_obj;
  db_double_t new_zset_ele_score;

  while (curr_zset_node)
  {
    if (!dbobj_is_zset(curr_zset_node->data))
      return free_dbzset(new_zset), dbobj_create_error(DB_ERR_WRONGTYPE);
    curr_zset = curr_zset_node->data->value.zset;
    curr_weight = curr_weight_node ? curr_weight_node->data->value.double_value : 1;
    curr_zset_ele = curr_zset->sentinel_forward[0];
    while (curr_zset_ele)
    {
      new_zset_ele_score_obj = zscore(new_zset, curr_zset_ele->member);
      curr_zset_ele_score = curr_zset_ele->score * curr_weight;
      switch (aggregate)
      {
      case DB_AGG_SUM:
        new_zset_ele_score = dbobj_extract_double(new_zset_ele_score_obj) + curr_zset_ele_score;
        break;
      case DB_AGG_MIN:
        if (!dbobj_is_double(new_zset_ele_score_obj))
        {
          new_zset_ele_score = curr_zset_ele_score;
          free_dbobj(new_zset_ele_score_obj);
          break;
        }
        new_zset_ele_score = dbobj_extract_double(new_zset_ele_score_obj);
        new_zset_ele_score = new_zset_ele_score < curr_zset_ele_score
                                 ? new_zset_ele_score
                                 : curr_zset_ele_score;
        break;
      case DB_AGG_MAX:
        if (!dbobj_is_double(new_zset_ele_score_obj))
        {
          new_zset_ele_score = curr_zset_ele_score;
          free_dbobj(new_zset_ele_score_obj);
          break;
        }
        new_zset_ele_score = dbobj_extract_double(new_zset_ele_score_obj);
        new_zset_ele_score = new_zset_ele_score > curr_zset_ele_score
                                 ? new_zset_ele_score
                                 : curr_zset_ele_score;
        break;
      default:
        free_dbzset(new_zset);
        return dbobj_create_error(DB_ERR_SYNTAX_ERROR);
      }
      zadd(new_zset, new_zset_ele_score, curr_zset_ele->member);
      curr_zset_ele = curr_zset_ele->forward[0];
    }
    curr_zset_node = curr_zset_node->next;
    curr_weight_node = curr_weight_node ? curr_weight_node->next : NULL;
  }

  return dbobj_create_zset(new_zset);
}

DBList *zrange(DBZSet *zset, db_uint_t start, db_uint_t stop, db_bool_t withscores)
{
  if (!zset)
    return NULL;
  db_uint_t index = 0;
  DBZSetElement *curr = zset->sentinel_forward[0];
  DBList *list = create_dblist();
  while (curr)
  {
    if (index >= start && index <= stop)
    {
      rpush(list, create_dblistnode(dbobj_create_string(dbutil_strdup(curr->member))));
      if (withscores)
        rpush(list, create_dblistnode(dbobj_create_double(curr->score)));
    }
    ++index;
    curr = curr->forward[0];
  }
  return list;
}

DBList *zrangebyscore(DBZSet *zset, db_double_t min, db_bool_t included_min, db_double_t max, db_bool_t included_max, db_bool_t withscores)
{
  if (!zset || min >= max)
    return NULL;
  DBZSetElement *curr = lookup_first_element_with_score(zset, min, included_min);
  DBZSetElement *last = lookup_last_element_with_score(zset, max, included_max);
  if (last)
    last = last->forward[0];
  DBList *list = create_dblist();
  while (curr && curr != last)
  {
    rpush(list, create_dblistnode(dbobj_create_string(dbutil_strdup(curr->member))));
    if (withscores)
      rpush(list, create_dblistnode(dbobj_create_double(curr->score)));
    curr = curr->forward[0];
  }
  return list;
}

DBObj *zrank(DBZSet *zset, const char *member, const db_bool_t withscores)
{
  if (!zset || !member)
    return dbobj_create_null();

  DBHashEntry *entry = hget(zset->dict, member, NULL);

  if (!entry)
    return dbobj_create_null();

  DBZSetElement *element = entry->data->value._zsetele;
  DBZSetElement *current = zset->sentinel_forward[0];
  db_int_t rank = 0;

  while (current != element)
  {
    current = current->forward[0];
    ++rank;
  }

  if (!withscores)
    return dbobj_create_int(rank);

  DBList *list = create_dblist();
  rpush(list, create_dblistnode(dbobj_create_int(rank)));
  rpush(list, create_dblistnode(dbobj_create_double(element->score)));
  return dbobj_create_list(list);
}

db_uint_t zrem(DBZSet *zset, const char *member)
{
  if (!zset || !member)
    return 0;

  // remove element from zset dict
  DBZSetElement *element = _dbobj_extract_zsetele(ht_extract_entry(ht_remove(zset->dict, member, NULL)));

  if (!element)
    return 0;

  // rebuild element member, because it freed when extract hash entry
  element->member = dbutil_strdup(member);

  // remove element from zset skip list
  DBZSetElement *previous = NULL;
  if (element->forward[0])
    element->forward[0]->backward = element->backward;
  for (int lvl = element->level; --lvl >= 0;)
  {
    previous = lookup_previos_element_in_level(zset, lvl, element, NULL);
    if (previous && previous != element)
      previous->forward[lvl] = element->forward[lvl];
    else if (zset->sentinel_forward[lvl] == element)
      zset->sentinel_forward[lvl] = element->forward[lvl];
  }
  if (zset->tail == element)
    zset->tail = element->backward;

  // zset down level
  while (zset->level > 1 && zset->sentinel_forward[zset->level - 1] == NULL)
  {
    DBZSetElement **new_sentinel_forward = (DBZSetElement **)realloc(zset->sentinel_forward, (--zset->level) * sizeof(DBZSetElement *));
    if (!new_sentinel_forward)
      EXIT_ON_MEMORY_ERROR();
    zset->sentinel_forward = new_sentinel_forward;
  }

  // free memories, we don't free the member, because it was freed when extracted DBObj
  free(element->forward);
  free(element->member);
  free(element);

  return 1;
}

db_uint_t zremrangebyscore(DBZSet *zset, db_double_t min, db_bool_t included_min, db_double_t max, db_bool_t included_max)
{
  if (!zset || min >= max)
    return 0;

  DBZSetElement *curr = lookup_first_element_with_score(zset, min, included_min);
  DBZSetElement *next;
  DBZSetElement *last = lookup_last_element_with_score(zset, max, included_max);
  if (last)
    last = last->forward[0];
  db_uint_t count = 0;
  while (curr && curr != last)
  {
    ++count;
    next = curr->forward[0];
    zrem(zset, curr->member);
    curr = next;
  }
  return count;
}
