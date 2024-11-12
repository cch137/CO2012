#include <string.h>
#include <time.h>
#include <malloc.h>
#include <unistd.h>
#include <threads.h>

#include "deps/cJSON.h"
#include "utils.h"
#include "database.h"

// Initial size of the hash table
#define INITIAL_TABLE_SIZE 16

// Load factor threshold for expanding the hash table
#define LOAD_FACTOR_EXPAND 0.7

// Load factor threshold for shrinking the hash table
#define LOAD_FACTOR_SHRINK 0.1

#define DEFAULT_CRON_HZ 10
#define DEFAULT_PERSISTENCE_FILE "db.json"

#define NANOSECONDS_PER_SECOND 1000000000L
#define TIMEOUT_SEC 0.1F

#define DB_ERR_DB_IS_CLOSED "Database is closed"
#define DB_ERR_ARG_ERROR "Argument error"
#define DB_ERR_UNKNOWN_COMMAND "Unknown command"

typedef struct HTEntry
{
  // Key for the entry
  char *key;
  // Type of the stored value (e.g., string or list)
  db_type_t type;
  // Value for the entry
  union DBValue
  {
    char *string;
    DLList *list;
  } value;
  // Pointer to the next entry in case of a hash collision
  struct HTEntry *next;
} HTEntry;

typedef struct HashTable
{
  // Number of slots in the hash table
  db_uint_t size;
  // Current number of entries in the hash table
  db_uint_t count;
  // Array of entries
  HTEntry **entries;
} HashTable;

typedef struct RequestEntry
{
  clock_t created_at;
  DBRequest *request;
  DBReply *reply;
  struct RequestEntry *next;
  bool done;
} RequestEntry;

// Computes the MurmurHash2 hash of a key
static db_uint_t murmurhash2(const void *key, db_uint_t len);

// Returns the memory usage of the current database dataset
static size_t get_dataset_memory_usage();

static int main_thread();

// Executes periodic tasks, including checking if rehashing should start
// Resets recent_tasks_count after each execution
static int cron_thread();

// Executed during each low-level operation and periodic task to maintain the hash table size
static void maintenance();

// Checks if rehashing is needed and performs a rehash step if required
// Returns true if additional rehash steps are required
static bool rehash_step();

// Creates a new hash table with the specified size
static HashTable *create_table(db_uint_t size);

// Frees the memory allocated for a hash table
static void free_table(HashTable *table);

// Creates a new entry with the specified key and type; assigns value directly
static HTEntry *create_entry(char *key, db_type_t type, void *value);

// Frees the memory allocated for a hash table entry
static void free_entry(HTEntry *entry);

// Retrieves an entry by key; returns NULL if not found
static HTEntry *get_entry(const char *key);

// Adds an entry to the hash table
static HTEntry *add_entry(HTEntry *entry);

// Removes an entry by key; returns NULL if not found
static HTEntry *remove_entry(const char *key);

// Creates a new node for a doubly linked list with specified data
static DLNode *create_dlnode(const char *data, DLNode *prev, DLNode *next);

// Initializes a new, empty doubly linked list
static DLList *create_dllist();

// Retrieves a list by key; returns NULL if not found
static DLList *get_dllist(const char *key);

// Retrieves a list by key or creates a new one if it does not exist
static DLList *get_or_create_dllist(const char *key);

// Frees a list node and all its sibling nodes
static void free_dlnode_chain(DLNode *node);

// Frees an entire list and all of its nodes
static void free_dllist(DLList *list);

// Retrieves a string from the database by key; returns NULL if not found or type mismatch
static char *db_get(const char *key);

// Stores a string in the database with the specified key and value
// Updates the value if the key exists, otherwise creates a new entry
// Returns true if successful, false if type mismatch
static bool db_set(const char *key, const char *value);

// Renames an existing key to a new key in the database
// Removes the old entry and inserts the new one with the updated key
// Returns true if successful, false if type mismatch
static bool db_rename(const char *old_key, const char *new_key);

// Deletes an entry by key; Returns the number of successfully deleted keys
static db_uint_t db_del(DBArg *arg);

// Pushes elements to the front of a list; last parameter must be NULL
static db_uint_t db_lpush(const char *key, DBArg *arg);

// Pops elements from the front of a list
static DLList *db_lpop(const char *key, db_uint_t count);

// Pushes elements to the end of a list; last parameter must be NULL
static db_uint_t db_rpush(const char *key, DBArg *arg);

// Pops elements from the end of a list
static DLList *db_rpop(const char *key, db_uint_t count);

// Returns the number of nodes in a list
static db_uint_t db_llen(const char *key);

// Returns a sublist from the specified range of indices
// The `stop` index is inclusive, and if `stop` is -1, the entire list is returned
static DLList *db_lrange(const char *key, db_uint_t start, db_uint_t stop);

const static clock_t TIMEOUT_CLOCK = (clock_t)CLOCKS_PER_SEC * TIMEOUT_SEC;

// Seed for the hash function, affecting hash distribution
static db_uint_t hash_seed = 0;
// Frequency of periodic task execution per second
static uint8_t cron_hz = DEFAULT_CRON_HZ;
static long int cron_sleep_ns = -1;

// File path for database persistence
static char *persistence_filepath = NULL;

// tables[0] is the main table, tables[1] is the rehash table
// During rehashing, entries are first searched and deleted from tables[1], then from tables[0].
// New entries are only written to tables[1] during rehashing.
// After rehashing is complete, tables[0] is freed and tables[1] is moved to the main table.
static HashTable *tables[2] = {NULL, NULL};

// -1 indicates no rehashing; otherwise, it's the current rehashing index
// The occurrence of rehashing is determined by periodic tasks; when rehashing starts, rehashing_index will be the last index of the table size
// Rehashing will be handled during periodic task execution and during db_insert_entry and db_get_entry.
static db_int_t rehashing_index = -1;

static bool is_running = false;
static mtx_t *lock = NULL;
static thrd_t worker = -1;

static RequestEntry *request_queue_head;
static RequestEntry *request_queue_tail;

static db_uint_t murmurhash2(const void *key, db_uint_t len)
{
  const db_uint_t m = 0x5bd1e995;
  const int r = 24;
  db_uint_t h = hash_seed ^ len;

  const unsigned char *data = (const unsigned char *)key;

  while (len >= 4)
  {
    db_uint_t k = *(db_uint_t *)data;
    k *= m, k ^= k >> r, k *= m;
    h *= m, h ^= k;
    data += 4, len -= 4;
  }

  switch (len)
  {
  case 3:
    h ^= data[2] << 16;
  case 2:
    h ^= data[1] << 8;
  case 1:
    h ^= data[0];
    h *= m;
  }

  h ^= h >> 13, h *= m, h ^= h >> 15;

  return h;
}

static size_t get_dataset_memory_usage()
{
  size_t size = 2 * sizeof(HashTable *);
  HTEntry *entry;
  DLNode *dllnode;

  for (int j = 0; j < 2; j++)
  {
    if (!tables[j])
      continue;
    size += malloc_usable_size(tables[j]);
    size += malloc_usable_size(tables[j]->entries);
    for (db_uint_t i = 0; i < tables[j]->size; i++)
    {
      entry = tables[j]->entries[i];
      while (entry)
      {
        size += malloc_usable_size(entry);
        size += malloc_usable_size(entry->key);
        switch (entry->type)
        {
        case DB_TYPE_STRING:
          size += malloc_usable_size(entry->value.string);
          break;
        case DB_TYPE_LIST:
          size += malloc_usable_size(entry->value.list);
          dllnode = entry->value.list->head;
          while (dllnode)
          {
            size += malloc_usable_size(dllnode);
            size += malloc_usable_size(dllnode->data);
            dllnode = dllnode->next;
          }
          break;
        default:
          break;
        }
        entry = entry->next;
      }
    }
  }

  return size;
}

static int main_thread()
{
  clock_t t0;
  thrd_t cron_worker;
  DBRequest *request;
  DBReply *reply;
  DBArg *arg1, *arg2, *arg3;

  thrd_create(&cron_worker, cron_thread, NULL);

  while (is_running)
  {
    if (mtx_trylock(lock) != thrd_success)
      continue;
    maintenance();
    while (request_queue_head)
    {
      request = request_queue_head->request;
      reply = request_queue_head->reply;
      reply->ok = true;
      switch (request->action)
      {
      case DB_GET:
        arg1 = request->arg_head;
        if (!arg1 || arg1->type != DB_TYPE_STRING)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        reply->type = DB_TYPE_STRING;
        reply->value.string = db_get(arg1->value.string);
        break;
      case DB_SET:
        arg1 = request->arg_head;
        arg2 = arg1->next;
        if (!arg1 || arg1->type != DB_TYPE_STRING || !arg2 || arg2->type != DB_TYPE_STRING)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        reply->type = DB_TYPE_BOOL;
        reply->value.boolean = db_set(arg1->value.string, arg2->value.string);
        break;
      case DB_RENAME:
        arg1 = request->arg_head;
        arg2 = arg1->next;
        if (!arg1 || arg1->type != DB_TYPE_STRING || !arg2 || arg2->type != DB_TYPE_STRING)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        reply->type = DB_TYPE_BOOL;
        reply->value.boolean = db_rename(arg1->value.string, arg2->value.string);
        break;
      case DB_DEL:
        reply->type = DB_TYPE_UINT;
        reply->value.unsigned_int = db_del(request->arg_head);
        break;
      case DB_LPUSH:
        arg1 = request->arg_head;
        if (!arg1 || arg1->type != DB_TYPE_STRING)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        arg2 = arg1->next;
        reply->type = DB_TYPE_UINT;
        reply->value.unsigned_int = db_lpush(arg1->value.string, arg1->next);
        break;
      case DB_LPOP:
        arg1 = request->arg_head;
        if (!arg1 || arg1->type != DB_TYPE_STRING)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        arg2 = arg1->next;
        if (!arg2)
          arg2 = db_add_arg_uint(request, 1);
        else if (arg2->type == DB_TYPE_INT)
        {
          arg2->type = DB_TYPE_UINT;
          arg2->value.unsigned_int = arg2->value.signed_int;
        }
        if (arg2->type != DB_TYPE_UINT)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        reply->type = DB_TYPE_LIST;
        reply->value.list = db_lpop(arg1->value.string, arg2->value.unsigned_int);
        break;
      case DB_RPUSH:
        arg1 = request->arg_head;
        if (!arg1 || arg1->type != DB_TYPE_STRING)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        arg2 = arg1->next;
        reply->type = DB_TYPE_UINT;
        reply->value.unsigned_int = db_rpush(arg1->value.string, arg2);
        break;
      case DB_RPOP:
        arg1 = request->arg_head;
        if (!arg1 || arg1->type != DB_TYPE_STRING)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        arg2 = arg1->next;
        if (!arg2)
          arg2 = db_add_arg_uint(request, 1);
        else if (arg2->type == DB_TYPE_INT)
        {
          arg2->type = DB_TYPE_UINT;
          arg2->value.unsigned_int = arg2->value.signed_int;
        }
        if (arg2->type != DB_TYPE_UINT)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        reply->type = DB_TYPE_LIST;
        reply->value.list = db_rpop(arg1->value.string, arg2->value.unsigned_int);
        break;
      case DB_LLEN:
        arg1 = request->arg_head;
        if (!arg1 || arg1->type != DB_TYPE_STRING)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        reply->type = DB_TYPE_UINT;
        reply->value.unsigned_int = db_llen(arg1->value.string);
        break;
      case DB_LRANGE:
        arg1 = request->arg_head;
        if (!arg1 || arg1->type != DB_TYPE_STRING)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        arg2 = arg1->next;
        arg3 = arg2 ? arg2->next : NULL;
        if (arg2 && arg2->type == DB_TYPE_INT)
        {
          arg2->type = DB_TYPE_UINT;
          arg2->value.unsigned_int = arg2->value.signed_int;
        }
        if (arg3 && arg3->type == DB_TYPE_INT)
        {
          arg3->type = DB_TYPE_UINT;
          arg3->value.unsigned_int = arg3->value.signed_int;
        }
        if (!arg2 || arg2->type != DB_TYPE_UINT || !arg3 || arg3->type != DB_TYPE_UINT)
        {
          reply->ok = false;
          reply->type = DB_TYPE_ERROR;
          reply->value.string = helper_strdup(DB_ERR_ARG_ERROR);
          break;
        }
        reply->type = DB_TYPE_LIST;
        reply->value.list = db_lrange(arg1->value.string, arg2->value.unsigned_int, arg3->value.unsigned_int);
        break;
      case DB_FLUSHALL:
        db_flushall();
        reply->type = DB_TYPE_BOOL;
        reply->value.boolean = true;
        break;
      case DB_INFO_DATASET_MEMORY:
        reply->type = DB_TYPE_UINT;
        reply->value.unsigned_int = get_dataset_memory_usage();
        break;
      default:
        reply->ok = false;
        reply->type = DB_TYPE_ERROR;
        reply->value.string = helper_strdup(DB_ERR_UNKNOWN_COMMAND);
        break;
      }
      request_queue_head->done = true;
      request_queue_head = request_queue_head->next;
      if (!request_queue_head)
        request_queue_tail = NULL;
    }
    mtx_unlock(lock);
  }

  thrd_join(cron_worker, NULL);

  return 0;
}

static int cron_thread()
{
  while (is_running)
  {
    mtx_lock(lock);
    // TODO: scheduled task
    mtx_unlock(lock);
    thrd_sleep(&(struct timespec){.tv_sec = 0, .tv_nsec = cron_sleep_ns}, NULL);
  }
  return 0;
}

static void maintenance()
{
  if (!tables[1])
  {
    if (tables[0]->count > LOAD_FACTOR_EXPAND * tables[0]->size)
    {
      rehashing_index = tables[0]->size - 1;
      tables[1] = create_table(tables[0]->size * 2);
    }
    else if (tables[0]->size > INITIAL_TABLE_SIZE && tables[0]->count < LOAD_FACTOR_SHRINK * tables[0]->size)
    {
      rehashing_index = tables[0]->size - 1;
      tables[1] = create_table(tables[0]->size / 2);
    }
  }
  else
    rehash_step();
}

static bool rehash_step()
{
  if (!tables[1])
    return false; // Not rehashing

  // Move entries from tables[0] to tables[1]
  db_uint_t index;
  HTEntry *curr_entry = tables[0]->entries[rehashing_index];
  HTEntry *next_entry;

  while (curr_entry)
  {
    next_entry = curr_entry->next;
    index = murmurhash2(curr_entry->key, strlen(curr_entry->key)) % tables[1]->size;
    curr_entry->next = tables[1]->entries[index];
    tables[1]->entries[index] = curr_entry;
    tables[1]->count++;
    tables[0]->count--;
    curr_entry = next_entry;
  }

  tables[0]->entries[rehashing_index] = NULL;
  rehashing_index--;

  if (rehashing_index == (int32_t)(-1))
  {
    free_table(tables[0]);
    tables[0] = tables[1];
    tables[1] = NULL; // Clear the rehash table
    return false;
  }

  return true;
}

static HashTable *create_table(db_uint_t size)
{
  HashTable *table = malloc(sizeof(HashTable));
  if (!table)
    memory_error_handler(__FILE__, __LINE__, __func__);

  table->size = size;
  table->count = 0;
  table->entries = calloc(size, sizeof(HTEntry *));

  if (!table->entries)
    memory_error_handler(__FILE__, __LINE__, __func__);

  return table;
}

static void free_table(HashTable *table)
{
  if (!table)
    return;
  db_uint_t i;

  HTEntry *curr_entry, *next_entry;
  for (i = 0; i < table->size; i++)
  {
    curr_entry = table->entries[i];
    next_entry = NULL;
    while (curr_entry)
    {
      next_entry = curr_entry->next;
      free_entry(curr_entry);
      curr_entry = next_entry;
    }
  }

  free(table->entries);
  free(table);
}

static HTEntry *create_entry(char *key, db_type_t type, void *value)
{
  if (!key || !value)
    return NULL;

  HTEntry *entry = (HTEntry *)malloc(sizeof(HTEntry));

  if (!entry)
    memory_error_handler(__FILE__, __LINE__, __func__);

  entry->key = key;

  switch (type)
  {
  case DB_TYPE_STRING:
    entry->value.string = value;
    break;
  case DB_TYPE_LIST:
    entry->value.list = value;
    break;
  default:
    free(entry);
    return NULL;
  }

  entry->type = type;

  return entry;
}

static void free_entry(HTEntry *entry)
{
  if (!entry)
    return;

  if (entry->key)
    free(entry->key);

  if (entry->type == DB_TYPE_STRING)
    free(entry->value.string);
  else if (entry->type == DB_TYPE_LIST)
    free_dllist(entry->value.list);

  free(entry);
}

static HTEntry *get_entry(const char *key)
{
  if (!key)
    return NULL;

  HTEntry *entry;

  if (tables[1])
  {
    entry = tables[1]->entries[murmurhash2(key, strlen(key)) % tables[1]->size];
    while (entry)
    {
      if (strcmp(entry->key, key) == 0)
        return entry;
      entry = entry->next;
    }
  }

  entry = tables[0]->entries[murmurhash2(key, strlen(key)) % tables[0]->size];
  while (entry)
  {
    if (strcmp(entry->key, key) == 0)
      return entry;
    entry = entry->next;
  }

  return NULL;
}

static HTEntry *add_entry(HTEntry *entry)
{
  if (!entry)
    return NULL;

  db_uint_t index;

  if (tables[1])
  {
    index = murmurhash2(entry->key, strlen(entry->key)) % tables[1]->size;
    entry->next = tables[1]->entries[index];
    tables[1]->entries[index] = entry;
    tables[1]->count++;
    return entry;
  }

  index = murmurhash2(entry->key, strlen(entry->key)) % tables[0]->size;
  entry->next = tables[0]->entries[index];
  tables[0]->entries[index] = entry;
  tables[0]->count++;
  return entry;
}

static HTEntry *remove_entry(const char *key)
{
  if (!key)
    return NULL;

  HTEntry *curr_entry, *prev_entry = NULL;
  db_uint_t index;

  if (tables[1])
  {
    index = murmurhash2(key, strlen(key)) % tables[1]->size;
    curr_entry = tables[1]->entries[index];
    while (curr_entry)
    {
      if (strcmp(curr_entry->key, key) == 0)
      {
        if (prev_entry)
          prev_entry->next = curr_entry->next;
        else
          tables[1]->entries[index] = curr_entry->next;
        tables[1]->count--;
        return curr_entry;
      }
      prev_entry = curr_entry;
      curr_entry = curr_entry->next;
    }
  }

  index = murmurhash2(key, strlen(key)) % tables[0]->size;
  curr_entry = tables[0]->entries[index];
  prev_entry = NULL;
  while (curr_entry)
  {
    if (strcmp(curr_entry->key, key) == 0)
    {
      if (prev_entry)
        prev_entry->next = curr_entry->next;
      else
        tables[0]->entries[index] = curr_entry->next;
      tables[0]->count--;
      return curr_entry;
    }
    prev_entry = curr_entry;
    curr_entry = curr_entry->next;
  }

  return NULL;
}

static DLNode *create_dlnode(const char *data, DLNode *prev, DLNode *next)
{
  DLNode *node = malloc(sizeof(DLNode));
  if (!node)
    memory_error_handler(__FILE__, __LINE__, __func__);
  node->data = helper_strdup(data);
  node->prev = prev;
  node->next = next;
  return node;
}

static DLList *create_dllist()
{
  DLList *list = malloc(sizeof(DLList));
  if (!list)
    memory_error_handler(__FILE__, __LINE__, __func__);
  list->head = NULL;
  list->tail = NULL;
  list->length = 0;
  return list;
}

static DLList *get_dllist(const char *key)
{
  if (!key)
    return NULL;

  HTEntry *entry = get_entry(key);

  if (entry && entry->type == DB_TYPE_LIST)
    return entry->value.list;
  return NULL;
}

static DLList *get_or_create_dllist(const char *key)
{
  if (!key)
    return NULL;

  HTEntry *entry = get_entry(key);

  if (entry)
  {
    if (entry->type == DB_TYPE_LIST)
      return entry->value.list;
    return NULL;
  }

  DLList *list = create_dllist();
  add_entry(create_entry(helper_strdup(key), DB_TYPE_LIST, list));

  return list;
}

static void free_dlnode_chain(DLNode *node)
{
  if (!node)
    return;

  DLNode *curr_neighbour = node->prev;
  DLNode *next_neighbour;

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
}

static void free_dllist(DLList *list)
{
  if (!list)
    return;

  free_dlnode_chain(list->head);
  free(list);
}

static char *db_get(const char *key)
{
  HTEntry *entry = get_entry(key);
  if (entry && entry->type == DB_TYPE_STRING)
    return helper_strdup(entry->value.string); // Return the string value
  return NULL;                                 // Not found
}

static bool db_set(const char *key, const char *value)
{
  HTEntry *entry = get_entry(key);
  if (!entry)
  {
    add_entry(create_entry(helper_strdup(key), DB_TYPE_STRING, helper_strdup(value)));
    return true;
  }
  if (entry->type == DB_TYPE_STRING)
  {
    free(entry->value.string);                  // Free old value
    entry->value.string = helper_strdup(value); // Update with new value
    return true;
  }
  return false;
}

static bool db_rename(const char *old_key, const char *new_key)
{
  HTEntry *entry = remove_entry(old_key);
  if (!entry)
    return false;
  free(entry->key);
  entry->key = helper_strdup(new_key);
  add_entry(entry);
  return true;
}

static db_uint_t db_del(DBArg *arg)
{
  HTEntry *entry;
  db_uint_t deleted_count = 0;

  while (arg && arg->type == DB_TYPE_STRING)
  {
    entry = remove_entry(arg->value.string);
    if (entry)
      free_entry(entry), deleted_count++;
    arg = arg->next;
  }

  return deleted_count;
}

static db_uint_t db_lpush(const char *key, DBArg *arg)
{
  if (!key)
    return 0;

  DLList *list = get_or_create_dllist(key);

  if (!list)
    return 0;

  DLNode *node;

  while (arg && arg->type == DB_TYPE_STRING)
  {
    node = create_dlnode(arg->value.string, NULL, list->head);
    if (list->head)
      list->head->prev = node;
    list->head = node;
    if (!list->tail)
      list->tail = node;
    list->length++;
    arg = arg->next;
  }

  return list->length;
}

static DLList *db_lpop(const char *key, db_uint_t count)
{
  if (!key || count == 0)
    return NULL;

  DLList *list = get_dllist(key);

  if (!list || !list->head || count == 0)
    return NULL;

  DLList *reply_list = create_dllist();
  DLNode *first_removed_node = list->head;
  DLNode *last_removed_node = first_removed_node;
  db_uint_t counted = 0;

  while (++counted < count)
    last_removed_node = last_removed_node->next;

  list->head = last_removed_node->next;
  last_removed_node->next = NULL;
  if (list->head)
    list->head->prev = NULL;
  else
    list->tail = NULL;
  list->length -= count;

  reply_list->head = first_removed_node;
  reply_list->tail = last_removed_node;
  reply_list->length = count;

  return reply_list;
}

static db_uint_t db_rpush(const char *key, DBArg *arg)
{
  if (!key)
    return 0;

  DLList *list = get_or_create_dllist(key);

  if (!list)
    return 0;

  DLNode *node;

  while (arg && arg->type == DB_TYPE_STRING)
  {
    node = create_dlnode(arg->value.string, list->tail, NULL);
    if (list->tail)
      list->tail->next = node;
    list->tail = node;
    if (!list->head)
      list->head = node;
    list->length++;
    arg = arg->next;
  }

  return list->length;
}

static DLList *db_rpop(const char *key, db_uint_t count)
{
  if (!key)
    return NULL;

  DLList *list = get_dllist(key);

  if (!list || !list->tail || count == 0)
    return NULL;

  DLList *reply_list = create_dllist();
  DLNode *first_removed_node = list->tail;
  DLNode *last_removed_node = first_removed_node;
  db_uint_t counted = 0;

  while (++counted < count)
    first_removed_node = first_removed_node->prev;

  list->tail = first_removed_node->prev;
  first_removed_node->prev = NULL;
  if (list->tail)
    list->tail->next = NULL;
  else
    list->head = NULL;
  list->length -= count;

  reply_list->head = first_removed_node;
  reply_list->tail = last_removed_node;
  reply_list->length = count;

  return reply_list;
}

static db_uint_t db_llen(const char *key)
{
  if (!key)
    return 0;

  DLList *list = get_dllist(key);

  if (!list)
    return 0;

  return list->length;
}

static DLList *db_lrange(const char *key, db_uint_t start, db_uint_t stop)
{
  if (!key)
    return NULL;

  DLList *list = get_dllist(key);

  if (!list || list->length == 0)
    return NULL;

  if (stop == DB_SIZE_MAX || stop > list->length - 1)
    stop = list->length - 1;

  if (start > stop)
    start = 0;

  DLList *reply_list = create_dllist();
  DLNode *new_node = NULL;
  DLNode *curr_node;
  db_uint_t index;

  if (start > list->length - 1 - stop)
  {
    curr_node = list->tail;
    index = list->length - 1;
    while (index != stop && curr_node)
    {
      curr_node = curr_node->prev;
      index--;
    }
    while (index >= start && curr_node)
    {
      new_node = create_dlnode(curr_node->data, NULL, new_node);
      if (!reply_list->tail)
        reply_list->tail = new_node;
      if (new_node->next)
        new_node->next->prev = new_node;
      curr_node = curr_node->prev;
      index--;
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
      index++;
    }
    while (index <= stop && curr_node)
    {
      new_node = create_dlnode(curr_node->data, new_node, NULL);
      if (!reply_list->head)
        reply_list->head = new_node;
      if (new_node->prev)
        new_node->prev->next = new_node;
      curr_node = curr_node->next;
      index++;
    }
    reply_list->tail = new_node;
  }

  reply_list->length = stop - start + 1;

  return reply_list;
}

void db_config_hash_seed(db_uint_t _hash_seed)
{
  mtx_lock(lock);
  hash_seed = _hash_seed ? _hash_seed : (db_uint_t)time(NULL);
  mtx_unlock(lock);
}

void db_config_cron_hz(uint8_t _cron_hz)
{
  mtx_lock(lock);
  cron_hz = _cron_hz;
  cron_sleep_ns = (1.0 / cron_hz) * NANOSECONDS_PER_SECOND;
  mtx_unlock(lock);
}

void db_config_persistence_filepath(const char *_persistence_filepath)
{
  mtx_lock(lock);
  free(persistence_filepath);
  persistence_filepath = helper_strdup(_persistence_filepath);
  mtx_unlock(lock);
}

void db_start()
{
  if (!lock)
  {
    lock = (mtx_t *)calloc(1, sizeof(mtx_t));
    if (!lock)
      memory_error_handler(__FILE__, __LINE__, __func__);
    mtx_init(lock, mtx_plain);
  }

  if (is_running)
    return;

  is_running = true;

  free_table(tables[0]);
  free_table(tables[1]);
  tables[0] = create_table(INITIAL_TABLE_SIZE);
  tables[1] = NULL;
  rehashing_index = -1;

  db_config_hash_seed(hash_seed);
  db_config_cron_hz(cron_hz);
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
      memory_error_handler(__FILE__, __LINE__, __func__);
    fread(buffer, 1, file_size, file);
    buffer[file_size] = '\0';
    fclose(file);

    char *key = NULL;
    DLList *list;
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
        add_entry(create_entry(key, DB_TYPE_STRING, helper_strdup(cJSON_GetStringValue(cjson_cursor))));
      }

      else if (cJSON_IsArray(cjson_cursor))
      {
        cjson_array_cursor = cjson_cursor->child;

        list = create_dllist();
        while (cjson_array_cursor)
        {
          if (cJSON_IsString(cjson_array_cursor))
          {
            list->tail = create_dlnode(cJSON_GetStringValue(cjson_array_cursor), list->tail, NULL);
            if (!list->head)
              list->head = list->tail;
          }

          cjson_array_cursor = cjson_array_cursor->next;
        }
        add_entry(create_entry(key, DB_TYPE_LIST, list));
      }

      cjson_cursor = cjson_cursor->next;
    }
  }

  thrd_create(&worker, main_thread, NULL);
}

void db_stop()
{
  if (!is_running)
    return;

  is_running = false;
  thrd_join(worker, NULL);

  mtx_lock(lock);
  db_save(persistence_filepath);
  mtx_unlock(lock);

  rehashing_index = -1;
  free_table(tables[0]);
  free_table(tables[1]);
}

void db_save()
{
  if (!persistence_filepath)
    return;

  cJSON *root = cJSON_CreateObject();
  HTEntry *entry;
  DLNode *dllnode;
  cJSON *cjson_list;

  for (int j = 0; j < 2; j++)
  {
    if (!tables[j])
      continue;

    for (db_uint_t i = 0; i < tables[j]->size; i++)
    {
      entry = tables[j]->entries[i];
      while (entry)
      {
        switch (entry->type)
        {
        case DB_TYPE_STRING:
          cJSON_AddItemToObject(root, entry->key, cJSON_CreateString(entry->value.string));
          break;
        case DB_TYPE_LIST:
          cjson_list = cJSON_CreateArray();
          dllnode = entry->value.list->head;
          while (dllnode)
          {
            cJSON_AddItemToArray(cjson_list, cJSON_CreateString(dllnode->data));
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

  FILE *file = fopen(persistence_filepath, "w");
  if (!file)
  {
    perror("Failed to open file while saving.");
    return;
  }

  char *json_string = cJSON_PrintUnformatted(root);
  fputs(json_string, file);
  fclose(file);
  free(json_string);
  cJSON_Delete(root);
}

void db_flushall()
{
  free_table(tables[0]);
  free_table(tables[1]);
  tables[0] = create_table(INITIAL_TABLE_SIZE);
  tables[1] = NULL;
  rehashing_index = -1;
}

DBRequest *db_create_request(db_action_t action)
{
  DBRequest *request = (DBRequest *)calloc(1, sizeof(DBRequest));
  if (!request)
    memory_error_handler(__FILE__, __LINE__, __func__);
  request->action = action;
  return request;
};

DBRequest *db_reset_request(DBRequest *request, db_action_t action)
{
  if (!request)
    return NULL;
  request->action = action;
  DBArg *arg = request->arg_head;
  while (arg)
  {
    request->arg_head = arg->next;
    free(arg);
    arg = request->arg_head;
  }
  request->arg_head = NULL;
  request->arg_tail = NULL;
  return request;
};

static DBArg *add_arg(DBRequest *request, db_type_t type)
{
  if (!request)
    return NULL;
  DBArg *arg = (DBArg *)calloc(1, sizeof(DBArg));
  if (!arg)
    memory_error_handler(__FILE__, __LINE__, __func__);
  arg->type = type;
  if (!request->arg_head)
    request->arg_head = arg;
  if (request->arg_tail)
    request->arg_tail->next = arg;
  request->arg_tail = arg;
  return arg;
};

DBArg *db_add_arg_uint(DBRequest *request, db_uint_t value)
{
  if (!request)
    return NULL;
  DBArg *arg = add_arg(request, DB_TYPE_UINT);
  arg->value.unsigned_int = value;
  return arg;
};

DBArg *db_add_arg_int(DBRequest *request, db_int_t value)
{
  if (!request)
    return NULL;
  DBArg *arg = add_arg(request, DB_TYPE_INT);
  arg->value.signed_int = value;
  return arg;
};

DBArg *db_add_arg_string(DBRequest *request, const char *value)
{
  if (!request)
    return NULL;
  DBArg *arg = add_arg(request, DB_TYPE_STRING);
  arg->value.string = helper_strdup(value);
  return arg;
};

DBReply *db_send_request(DBRequest *request)
{
  if (!request)
    return NULL;

  DBReply *reply = (DBReply *)calloc(1, sizeof(DBReply));
  if (!reply)
    memory_error_handler(__FILE__, __LINE__, __func__);

  reply->ok = false;

  if (!is_running)
  {
    reply->ok = false;
    reply->type = DB_TYPE_ERROR;
    reply->value.string = helper_strdup(DB_ERR_DB_IS_CLOSED);
    return reply;
  }

  RequestEntry *entry = (RequestEntry *)malloc(sizeof(RequestEntry));
  if (!entry)
    memory_error_handler(__FILE__, __LINE__, __func__);

  entry->created_at = clock();
  entry->request = request;
  entry->reply = reply;
  entry->next = NULL;
  entry->done = false;

  mtx_lock(lock);
  if (!request_queue_head)
  {
    request_queue_head = entry;
    request_queue_tail = entry;
  }
  else
  {
    request_queue_tail->next = entry;
    request_queue_tail = entry;
  }
  mtx_unlock(lock);

  while (!entry->done)
    continue;

  free(entry);

  return reply;
};

void *db_free_request(DBRequest *request)
{
  if (!request)
    return NULL;
  DBArg *arg = request->arg_head;
  DBArg *next_arg;
  while (arg)
  {
    next_arg = arg->next;
    free(arg);
    arg = next_arg;
  }
  free(request);
};

void db_free_reply(DBReply *reply)
{
  if (!reply)
    return;

  switch (reply->type)
  {
  case DB_TYPE_ERROR:
  case DB_TYPE_STRING:
    free(reply->value.string);
    break;
  case DB_TYPE_LIST:
    free_dllist(reply->value.list);
    break;
  case DB_TYPE_UINT:
  default:
    break;
  }

  free(reply);
}

DBReply *db_print_reply(DBReply *res)
{
  if (!res)
  {
    printf("(nil)\n");
    return res;
  }

  switch (res->type)
  {
  case DB_TYPE_ERROR:
    printf("(error) %s\n", res->value.string ? res->value.string : "");
    break;
  case DB_TYPE_STRING:
    printf("%s\n", res->value.string ? res->value.string : "");
    break;
  case DB_TYPE_UINT:
    printf("(uint) %u\n", res->value.unsigned_int);
    break;
  case DB_TYPE_LIST:
    printf("(list) count: %u\n", res->value.list ? res->value.list->length : 0);
    if (!res->value.list)
      break;
    db_uint_t i = 1;
    DLNode *node = res->value.list->head;
    while (node)
      printf("  %u) %s\n", i++, node->data), node = node->next;
    break;
  default:
    printf("(unknown)\n");
    break;
  }

  return res;
}
