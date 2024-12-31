#include <string.h>
#include <time.h>
#include <malloc.h>
#include <threads.h>
#include <math.h>

#include "deps/cJSON.h"
#include "utils.h"
#include "list.h"
#include "hash.h"
#include "interaction.h"
#include "core.h"

typedef struct DBTask
{
  clock_t created_at;
  DBRequest *request;
  DBReply *reply;
  struct DBTask *next;
} DBTask;

static inline void core_lock_init();

static DBListNode *get_arg_head_node(DBRequest *request);

static int core_worker();

// Retrieves a string by key;
static const char const *core_retrieve_string(const char *key);

// Retrieves a list by key;
static DBList *core_retrieve_list(const char *key, const db_bool_t create_new_if_not_found);

// File path for database persistence
static char *persistence_filepath = NULL;

static db_bool_t is_running = false;
static DBHash *core_ht = NULL;
static mtx_t *lock = NULL;
static thrd_t core_worker_thread = -1;

static DBTask *task_queue_head = NULL;
static DBTask *task_queue_tail = NULL;

static inline void core_lock_init()
{
  if (!lock)
  {
    lock = (mtx_t *)calloc(1, sizeof(mtx_t));
    if (!lock)
      EXIT_ON_MEMORY_ERROR();
    mtx_init(lock, mtx_plain);
  }
}

int core_lock()
{
  core_lock_init();
  return mtx_lock(lock);
}

int core_unlock()
{
  core_lock_init();
  return mtx_unlock(lock);
}

db_bool_t core_trylock_is_success()
{
  core_lock_init();
  return mtx_trylock(lock) == thrd_success;
}

static DBListNode *get_arg_head_node(DBRequest *request)
{
  if (!request || !request->args)
    return NULL;
  return request->args->head;
}

void db_start()
{
  if (is_running)
    return;

  srand(time(NULL));
  is_running = true;

  if (core_ht)
    ht_reset(core_ht);
  else
    core_ht = ht_create();

  db_flushall(NULL, NULL);

  db_config_hash_seed(hash_seed);
  if (!persistence_filepath)
    db_config_persistence_filepath(DEFAULT_PERSISTENCE_FILE);

  // load data
  FILE *file = fopen(persistence_filepath, "r");
  if (file)
  {
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    char *buffer = (char *)malloc(file_size + 1);
    if (!buffer)
      EXIT_ON_MEMORY_ERROR();
    fread(buffer, 1, file_size, file);
    buffer[file_size] = '\0';
    fclose(file);

    char *key = NULL;
    DBList *list;
    cJSON *cjson_cursor = cJSON_Parse(buffer);
    cJSON *cjson_array_cursor = NULL;
    free(buffer);

    if (cjson_cursor)
      cjson_cursor = cjson_cursor->child;

    while (cjson_cursor)
    {
      key = cjson_cursor->string;

      if (!key)
      {
        cjson_cursor = cjson_cursor->next;
        continue;
      }

      if (cJSON_IsString(cjson_cursor))
      {
        ht_add_entry(core_ht, ht_create_string_entry_with_dup(key, cJSON_GetStringValue(cjson_cursor)));
      }

      else if (cJSON_IsArray(cjson_cursor))
      {
        cjson_array_cursor = cjson_cursor->child;

        list = create_dblist();
        while (cjson_array_cursor)
        {
          if (cJSON_IsString(cjson_array_cursor))
          {
            rpush(list, create_dblistnode_with_string(cJSON_GetStringValue(cjson_array_cursor)));
          }

          cjson_array_cursor = cjson_array_cursor->next;
        }
        ht_add_entry(core_ht, ht_create_list_entry(dbutil_strdup(key), list));
      }

      cjson_cursor = cjson_cursor->next;
    }
  }

  thrd_create(&core_worker_thread, core_worker, NULL);
}

db_bool_t db_is_running()
{
  return is_running;
}

void db_config_hash_seed(db_uint_t _hash_seed)
{
  hash_seed = _hash_seed ? _hash_seed : (db_uint_t)time(NULL);
}

void db_config_persistence_filepath(const char *_persistence_filepath)
{
  free(persistence_filepath);
  persistence_filepath = dbutil_strdup(_persistence_filepath);
}

DBReply *db_handle_request(DBRequest *request)
{
  DBReply *reply = create_reply();

  if (!is_running)
  {
    reply_error(reply, DB_ERR_DB_IS_CLOSED);
    return reply;
  }

  DBTask *task = (DBTask *)malloc(sizeof(DBTask));
  if (!task)
    EXIT_ON_MEMORY_ERROR();

  task->created_at = clock();
  task->request = request;
  task->reply = reply;
  task->next = NULL;

  if (!task_queue_head)
  {
    task_queue_head = task;
    task_queue_tail = task;
  }
  else
  {
    task_queue_tail->next = task;
    task_queue_tail = task;
  }

  return reply;
}

static int core_worker()
{
  clock_t t0;
  DBRequest *request;
  DBReply *reply;
  // Calculate the sleep increment to reach 1 second over 5 minutes, in nanoseconds.
  const long sleep_increment_ns = NANOSECONDS_PER_SECOND / (5 * 60 * 1000);
  clock_t idle_start_time = 0;
  long sleep_duration_ns = 0;

  while (is_running)
  {
    if (!core_trylock_is_success())
      continue;

    if (task_queue_head)
    {
      if (task_queue_head->request->action != DB_INFO_DATASET_MEMORY)
      {
        idle_start_time = 0;
        sleep_duration_ns = 0;
      }
      do
      {
        request = task_queue_head->request;
        reply = task_queue_head->reply;
        switch (request->action)
        {
        case DB_GET:
          db_get(request, reply);
          break;
        case DB_SET:
          db_set(request, reply);
          break;
        case DB_RENAME:
          db_rename(request, reply);
          break;
        case DB_DEL:
          db_del(request, reply);
          break;
        case DB_LPUSH:
          db_lpush(request, reply);
          break;
        case DB_LPOP:
          db_lpop(request, reply);
          break;
        case DB_RPUSH:
          db_rpush(request, reply);
          break;
        case DB_RPOP:
          db_rpop(request, reply);
          break;
        case DB_LLEN:
          db_llen(request, reply);
          break;
        case DB_LRANGE:
          db_lrange(request, reply);
          break;
        case DB_KEYS:
          db_keys(request, reply);
          break;
        case DB_FLUSHALL:
          db_flushall(request, reply);
          break;
        // TODO: Returns the memory usage of the current database dataset
        // case DB_INFO_DATASET_MEMORY:
        //   db_get_dataset_memory_usage(request, reply);
        //   break;
        case DB_SAVE:
          db_save(request, reply);
          break;
        case DB_SHUTDOWN:
          db_shutdown(request, reply);
          break;
        default:
          reply_error(reply, DB_ERR_UNKNOWN_COMMAND);
          break;
        }
        reply->done = true;
        free(task_queue_head);
        task_queue_head = task_queue_head->next;
        if (!task_queue_head)
          task_queue_tail = NULL;
      } while (task_queue_head);
      core_unlock();
    }
    else
    {
      core_unlock();
      if (!idle_start_time)
      {
        idle_start_time = clock();
      }
      // If the idle time exceeds 100ms,
      // a progressively increasing sleep period begins.
      if ((clock() - idle_start_time) * 1000 / CLOCKS_PER_SEC > 100)
      {
        if (sleep_duration_ns < NANOSECONDS_PER_SECOND)
          sleep_duration_ns += sleep_increment_ns;
        thrd_sleep(&(struct timespec){.tv_sec = 0, .tv_nsec = sleep_duration_ns}, NULL);
      }
    }
  }

  return 0;
}

static const char const *core_retrieve_string(const char *key)
{
  if (!key)
    return NULL;

  DBHashEntry *entry = ht_get_entry(core_ht, key);

  if (entry && entry->data->type == DB_TYPE_STRING)
  {
    return entry->data->value.string;
  }

  return NULL;
}

static DBList *core_retrieve_list(const char *key, const db_bool_t create_new_if_not_found)
{
  if (!key)
    return NULL;

  DBHashEntry *entry = ht_get_entry(core_ht, key);

  if (entry)
  {
    return entry->data->type == DB_TYPE_LIST ? entry->data->value.list : NULL;
  }

  if (create_new_if_not_found)
  {
    DBList *list = create_dblist();
    ht_add_entry(core_ht, ht_create_list_entry(dbutil_strdup(key), list));

    return list;
  }

  return NULL;
}

void db_get(DBRequest *request, DBReply *reply)
{
  DBListNode *curr_arg_node = get_arg_head_node(request);
  char *key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;

  if (!key || curr_arg_node)
  {
    reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  const char const *value = core_retrieve_string(key);

  if (value)
  {
    // Return the string value
    reply_data(reply, dbobj_create_string_with_dup(value));
  }
  else
  {
    // Not found
    reply_data(reply, dbobj_create_null());
  }
}

void db_set(DBRequest *request, DBReply *reply)
{
  DBListNode *curr_arg_node = get_arg_head_node(request);
  char *key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  char *value = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;

  if (!key || !value || curr_arg_node)
  {
    reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DBHashEntry *entry = ht_get_entry(core_ht, key);

  if (entry)
  {
    free_dbobj(entry->data);
    entry->data = dbobj_create_string_with_dup(value);
  }
  else
  {
    ht_add_entry(core_ht, ht_create_string_entry_with_dup(key, value));
  }

  reply_data(reply, dbobj_create_string_with_dup(OK));
}

void db_rename(DBRequest *request, DBReply *reply)
{
  DBListNode *curr_arg_node = get_arg_head_node(request);
  char *old_key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  char *new_key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;

  if (!old_key || !new_key || curr_arg_node)
  {
    reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DBHashEntry *entry = ht_remove_entry(core_ht, old_key);

  if (!entry)
  {
    reply_error(reply, DB_ERR_NONEXISTENT_KEY);
    return;
  }

  free(entry->key);
  entry->key = dbutil_strdup(new_key);
  ht_add_entry(core_ht, entry);

  reply_data(reply, dbobj_create_string_with_dup(OK));
}

void db_del(DBRequest *request, DBReply *reply)
{
  DBListNode *curr_arg_node = get_arg_head_node(request);
  char *key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;

  if (!key)
  {
    reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DBHashEntry *entry;
  db_uint_t deleted_count = 0;

  while (key)
  {
    entry = ht_remove_entry(core_ht, key);
    if (entry)
      ht_free_entry(entry), ++deleted_count;
    key = get_string_arg(curr_arg_node);
    curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  }

  reply_data(reply, dbobj_create_uint(deleted_count));
}

void db_lpush(DBRequest *request, DBReply *reply)
{
  DBListNode *curr_arg_node = get_arg_head_node(request);
  char *key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  char *member = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;

  if (!key || !member)
  {
    reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DBList *list = core_retrieve_list(key, true);

  if (!list)
  {
    reply_error(reply, DB_ERR_WRONGTYPE);
    return;
  }

  while (member)
  {
    lpush(list, create_dblistnode_with_string(member));
    member = get_string_arg(curr_arg_node);
    curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  }

  reply_data(reply, dbobj_create_uint(list->length));
}

void db_lpop(DBRequest *request, DBReply *reply)
{
  DBListNode *curr_arg_node = get_arg_head_node(request);
  char *key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  db_uint_t count = curr_arg_node ? get_uint_arg(curr_arg_node) : 1;
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;

  if (!key || !count || curr_arg_node)
  {
    reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DBList *list = core_retrieve_list(key, false);

  if (!list)
  {
    reply_data(reply, dbobj_create_null());
    return;
  }

  if (count == 1)
  {
    reply_data(reply, dbobj_create_string(extract_dblistnode_string(lpop(list))));
  }
  else if (count)
  {
    reply_data(reply, dbobj_create_list(lpop_n(list, count)));
  }
}

void db_rpush(DBRequest *request, DBReply *reply)
{
  DBListNode *curr_arg_node = get_arg_head_node(request);
  char *key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  char *member = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;

  if (!key || !member)
  {
    reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DBList *list = core_retrieve_list(key, true);

  if (!list)
  {
    reply_error(reply, DB_ERR_WRONGTYPE);
    return;
  }

  while (member)
  {
    lpush(list, create_dblistnode_with_string(member));
    member = get_string_arg(curr_arg_node);
    curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  }

  reply_data(reply, dbobj_create_uint(list->length));
}

void db_rpop(DBRequest *request, DBReply *reply)
{
  DBListNode *curr_arg_node = get_arg_head_node(request);
  char *key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  db_uint_t count = curr_arg_node ? get_uint_arg(curr_arg_node) : 1;
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;

  if (!key || !count || curr_arg_node)
  {
    reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DBList *list = core_retrieve_list(key, false);

  if (!list)
  {
    reply_data(reply, dbobj_create_null());
    return;
  }

  if (count == 1)
  {
    reply_data(reply, dbobj_create_string(extract_dblistnode_string(rpop(list))));
  }
  else if (count)
  {
    reply_data(reply, dbobj_create_list(rpop_n(list, count)));
  }
}

void db_llen(DBRequest *request, DBReply *reply)
{
  DBListNode *curr_arg_node = get_arg_head_node(request);
  char *key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;

  if (!key || curr_arg_node)
  {
    reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  const DBList const *list = core_retrieve_list(key, false);

  reply_data(reply, dbobj_create_uint(list ? list->length : 0));
}

void db_lrange(DBRequest *request, DBReply *reply)
{
  DBListNode *curr_arg_node = get_arg_head_node(request);
  char *key = get_string_arg(curr_arg_node);
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  db_uint_t start = curr_arg_node ? get_uint_arg(curr_arg_node) : 0;
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;
  db_uint_t stop = curr_arg_node ? get_uint_arg(curr_arg_node) : DB_UINT_MAX;
  curr_arg_node = curr_arg_node ? curr_arg_node->next : NULL;

  if (!key || curr_arg_node)
  {
    reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DBList *list = core_retrieve_list(key, false);

  list = lrange(list, start, stop);

  if (!list)
  {
    reply_error(reply, DB_ERR_WRONGTYPE);
    return;
  }

  reply_data(reply, dbobj_create_list(list));
}

void db_keys(DBRequest *request, DBReply *reply)
{
  DBHashEntry *entry;
  db_uint_t r;
  DBList *reply_list = create_dblist();

  if (core_ht->buckets0)
  {
    for (r = 0; r < core_ht->size0; ++r)
    {
      entry = core_ht->buckets0[r];
      while (entry)
      {
        rpush(reply_list, create_dblistnode_with_string(entry->key));
        entry = entry->next;
      }
    }
  }

  if (core_ht->buckets1)
  {
    for (r = 0; r < core_ht->size1; ++r)
    {
      entry = core_ht->buckets1[r];
      while (entry)
      {
        rpush(reply_list, create_dblistnode_with_string(entry->key));
        entry = entry->next;
      }
    }
  }

  reply_data(reply, dbobj_create_list(reply_list));
}

void db_shutdown(DBRequest *request, DBReply *reply)
{
  if (!is_running)
  {
    reply_error(reply, DB_ERR_DB_IS_CLOSED);
    return;
  }

  is_running = false;
  thrd_join(core_worker_thread, NULL);

  db_save(request, reply);

  ht_reset(core_ht);

  reply_data(reply, dbobj_create_string_with_dup(OK));
}

void db_save(DBRequest *request, DBReply *reply)
{
  if (!persistence_filepath)
  {
    reply_data(reply, dbobj_create_bool(false));
    return;
  }

  cJSON *root = cJSON_CreateObject();
  DBHashEntry *entry;
  DBListNode *dllnode;
  cJSON *cjson_list;

  FILE *file = fopen(persistence_filepath, "w");
  if (!file)
  {
    perror("Failed to open file while saving.");
    reply_data(reply, dbobj_create_bool(false));
    return;
  }

  if (core_ht->buckets0)
  {
    for (db_uint_t i = 0; i < core_ht->size0; ++i)
    {
      entry = core_ht->buckets0[i];
      while (entry)
      {
        switch (entry->data->type)
        {
        case DB_TYPE_STRING:
          cJSON_AddItemToObject(root, entry->key, cJSON_CreateString(entry->data->value.string));
          break;
        case DB_TYPE_LIST:
          cjson_list = cJSON_CreateArray();
          dllnode = entry->data->value.list->head;
          while (dllnode)
          {
            if (dbobj_is_string(dllnode->data))
              cJSON_AddItemToArray(cjson_list, cJSON_CreateString(dllnode->data->value.string));
            dllnode = dllnode->next;
          }
          cJSON_AddItemToObject(root, entry->key, cjson_list);
          cjson_list = NULL;
          dllnode = NULL;
          break;
        default:
          break;
        }
        entry = entry->next;
      }
    }
  }

  if (core_ht->buckets1)
  {
    for (db_uint_t i = 0; i < core_ht->size1; ++i)
    {
      entry = core_ht->buckets1[i];
      while (entry)
      {
        switch (entry->data->type)
        {
        case DB_TYPE_STRING:
          cJSON_AddItemToObject(root, entry->key, cJSON_CreateString(entry->data->value.string));
          break;
        case DB_TYPE_LIST:
          cjson_list = cJSON_CreateArray();
          dllnode = entry->data->value.list->head;
          while (dllnode)
          {
            if (dbobj_is_string(dllnode->data))
              cJSON_AddItemToArray(cjson_list, cJSON_CreateString(dllnode->data->value.string));
            dllnode = dllnode->next;
          }
          cJSON_AddItemToObject(root, entry->key, cjson_list);
          cjson_list = NULL;
          dllnode = NULL;
          break;
        default:
          break;
        }
        entry = entry->next;
      }
    }
  }

  char *json_string = cJSON_PrintUnformatted(root);

  if (!json_string)
    EXIT_ON_MEMORY_ERROR();

  fputs(json_string, file);
  fclose(file);
  free(json_string);
  cJSON_Delete(root);

  reply_data(reply, dbobj_create_string_with_dup(OK));
}

void db_flushall(DBRequest *request, DBReply *reply)
{
  if (reply)
  {
    reply_data(reply, dbobj_create_string_with_dup(OK));
  }

  ht_reset(core_ht);
}
