#include <string.h>
#include <time.h>
#include <ctype.h>
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

#define DEFAULT_PERSISTENCE_FILE "db.json"

#define NANOSECONDS_PER_SECOND 1000000000L

#define DB_ERR_DB_IS_CLOSED "ERR database is closed"
#define DB_ERR_ARG_ERROR "ERR wrong arguments "
#define DB_ERR_WRONGTYPE "WRONGTYPE Operation against a key holding the wrong kind of value"
#define DB_ERR_NONEXISTENT_KEY "ERR no such key"
#define DB_ERR_UNKNOWN_COMMAND "ERR unknown command"

typedef enum db_action_t
{
  DB_UNKNOWN_COMMAND,
  DB_SAVE,
  DB_START,
  DB_SET,
  DB_GET,
  DB_RENAME,
  DB_DEL,
  DB_LPUSH,
  DB_LPOP,
  DB_RPUSH,
  DB_RPOP,
  DB_LLEN,
  DB_LRANGE,
  DB_KEYS,
  DB_FLUSHALL,
  DB_INFO_DATASET_MEMORY,
  DB_SHUTDOWN
} db_action_t;

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

typedef struct DBArg
{
  db_type_t type;
  union
  {
    char *string;
    db_uint_t unsigned_int;
    db_int_t signed_int;
  } value;
  struct DBArg *next;
} DBArg;

typedef struct DBRequest
{
  db_action_t action;
  DBArg *arg_head;
  DBArg *arg_tail;
} DBRequest;

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

static void set_entry_value(HTEntry *entry, db_type_t type, void *value);

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
static void db_get(DBRequest *request, DBReply *reply);

// Stores a string in the database with the specified key and value
// Updates the value if the key exists, otherwise creates a new entry
// Returns true if successful, false if type mismatch
static void db_set(DBRequest *request, DBReply *reply);

// Renames an existing key to a new key in the database
// Removes the old entry and inserts the new one with the updated key
// Returns true if successful, false if type mismatch
static void db_rename(DBRequest *request, DBReply *reply);

// Deletes an entry by key; Returns the number of successfully deleted keys
static void db_del(DBRequest *request, DBReply *reply);

// Pushes elements to the front of a list; last parameter must be NULL
static void db_lpush(DBRequest *request, DBReply *reply);

// Pops elements from the front of a list
static void db_lpop(DBRequest *request, DBReply *reply);

// Pushes elements to the end of a list; last parameter must be NULL
static void db_rpush(DBRequest *request, DBReply *reply);

// Pops elements from the end of a list
static void db_rpop(DBRequest *request, DBReply *reply);

// Returns the number of nodes in a list
static void db_llen(DBRequest *request, DBReply *reply);

// Returns a sublist from the specified range of indices
// The `stop` index is inclusive, and if `stop` is -1, the entire list is returned
static void db_lrange(DBRequest *request, DBReply *reply);

static void db_keys(DBRequest *request, DBReply *reply);

// Stops the database and saves data to a specified file
static void db_shutdown();

// Saves the current state of the database to persistent storage
static void db_save();

// Deletes all item from all databases.
static void db_flushall();

static DBRequest *create_request(db_action_t action);
static DBRequest *reset_request(DBRequest *request, db_action_t action);
static DBArg *add_arg_uint(DBRequest *request, db_uint_t uint_value);
static DBArg *add_arg_string(DBRequest *request, const char *string_value);
static DBReply *send_request(DBRequest *request);
static void free_request(DBRequest *request);

static DBArg *arg_string_to_uint(DBArg *arg);
static DBReply *set_reply_error(DBReply *arg, const char *message);

// Parses command string into DBRequest structure
static DBRequest *parse_command(const char *command);

// Seed for the hash function, affecting hash distribution
static db_uint_t hash_seed = 0;

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

static RequestEntry *request_queue_head = NULL;
static RequestEntry *request_queue_tail = NULL;

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

  for (int j = 0; j < 2; ++j)
  {
    if (!tables[j])
      continue;
    size += malloc_usable_size(tables[j]);
    size += malloc_usable_size(tables[j]->entries);
    for (db_uint_t i = 0; i < tables[j]->size; ++i)
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
  DBRequest *request;
  DBReply *reply;
  DBArg *arg1, *arg2, *arg3;
  // Calculate the sleep increment to reach 1 second over 5 minutes, in nanoseconds.
  const long sleep_increment_ns = NANOSECONDS_PER_SECOND / (5 * 60 * 1000);
  clock_t idle_start_time = 0;
  long sleep_duration_ns = 0;

  printf("Welcome to cch137's database!\n");
  printf("Please use commands to interact with the database.\n");

  while (is_running)
  {
    if (mtx_trylock(lock) != thrd_success)
      continue;

    if (request_queue_head)
    {
      if (request_queue_head->request->action != DB_INFO_DATASET_MEMORY)
      {
        idle_start_time = 0;
        sleep_duration_ns = 0;
      }
      do
      {
        maintenance();
        request = request_queue_head->request;
        reply = request_queue_head->reply;
        reply->ok = true;
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
          db_flushall();
          reply->type = DB_TYPE_BOOL;
          reply->value.boolean = true;
          break;
        case DB_INFO_DATASET_MEMORY:
          reply->type = DB_TYPE_UINT;
          reply->value.unsigned_int = get_dataset_memory_usage();
          break;
        case DB_SAVE:
          db_save();
          reply->type = DB_TYPE_BOOL;
          reply->value.boolean = true;
          break;
        case DB_SHUTDOWN:
          db_shutdown();
          reply->type = DB_TYPE_BOOL;
          reply->value.boolean = true;
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
      } while (request_queue_head);
      mtx_unlock(lock);
    }
    else
    {
      maintenance();
      mtx_unlock(lock);
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
    ++tables[1]->count;
    --tables[0]->count;
    curr_entry = next_entry;
  }

  tables[0]->entries[rehashing_index] = NULL;
  --rehashing_index;

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
  HashTable *table = (HashTable *)malloc(sizeof(HashTable));
  if (!table)
    memory_error_handler(__FILE__, __LINE__, __func__);

  table->size = size;
  table->count = 0;
  table->entries = (HTEntry **)calloc(size, sizeof(HTEntry *));

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
  for (i = 0; i < table->size; ++i)
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
  entry->next = NULL;

  set_entry_value(entry, type, value);

  return entry;
}

static void free_entry(HTEntry *entry)
{
  if (!entry)
    return;

  if (entry->key)
    free(entry->key);

  set_entry_value(entry, DB_TYPE_NULL, NULL);

  free(entry);
}

static void set_entry_value(HTEntry *entry, db_type_t type, void *value)
{
  if (!entry)
    return;

  if (entry->type != type)
  {
    switch (entry->type)
    {
    case DB_TYPE_STRING:
      free(entry->value.string);
      entry->value.string = NULL;
      break;
    case DB_TYPE_LIST:
      free_dllist(entry->value.list);
      entry->value.list = NULL;
      break;
    default:
      break;
    }
    entry->type = type;
  }

  if (!value)
    return;

  switch (type)
  {
  case DB_TYPE_STRING:
    entry->value.string = value;
    break;
  case DB_TYPE_LIST:
    entry->value.list = value;
    break;
  default:
    break;
  }
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
    ++tables[1]->count;
    return entry;
  }

  index = murmurhash2(entry->key, strlen(entry->key)) % tables[0]->size;
  entry->next = tables[0]->entries[index];
  tables[0]->entries[index] = entry;
  ++tables[0]->count;
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
        --tables[1]->count;
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
      --tables[0]->count;
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
  if (prev)
    prev->next = node;
  node->next = next;
  if (next)
    next->prev = node;
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

static void db_get(DBRequest *request, DBReply *reply)
{
  DBArg *arg1 = request->arg_head;

  if (!arg1)
  {
    set_reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  reply->type = DB_TYPE_STRING;
  HTEntry *entry = get_entry(arg1->value.string);

  if (entry && entry->type == DB_TYPE_STRING)
  {
    // Return the string value
    reply->type = DB_TYPE_STRING;
    reply->value.string = helper_strdup(entry->value.string);
  }
  else
  {
    // Not found
    reply->type = DB_TYPE_NULL;
  }
}

static void db_set(DBRequest *request, DBReply *reply)
{
  DBArg *arg1 = request->arg_head;
  DBArg *arg2 = arg1 ? arg1->next : NULL;

  if (!arg1 || !arg2)
  {
    set_reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  HTEntry *entry = get_entry(arg1->value.string);

  if (entry)
    set_entry_value(entry, DB_TYPE_STRING, helper_strdup(arg2->value.string));
  else
    add_entry(create_entry(helper_strdup(arg1->value.string), DB_TYPE_STRING, helper_strdup(arg2->value.string)));

  reply->type = DB_TYPE_BOOL;
  reply->value.boolean = true;
}

static void db_rename(DBRequest *request, DBReply *reply)
{
  DBArg *arg1 = request->arg_head;
  DBArg *arg2 = arg1 ? arg1->next : NULL;

  if (!arg1 || !arg2)
  {
    set_reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  HTEntry *entry = remove_entry(arg1->value.string);

  if (!entry)
  {
    set_reply_error(reply, DB_ERR_NONEXISTENT_KEY);
    return;
  }

  free(entry->key);
  entry->key = helper_strdup(arg2->value.string);
  add_entry(entry);

  reply->type = DB_TYPE_BOOL;
  reply->value.boolean = true;
}

static void db_del(DBRequest *request, DBReply *reply)
{
  DBArg *arg = request->arg_head;

  if (!arg)
  {
    set_reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  HTEntry *entry;
  db_uint_t deleted_count = 0;

  while (arg)
  {
    entry = remove_entry(arg->value.string);
    if (entry)
      free_entry(entry), ++deleted_count;
    arg = arg->next;
  }

  reply->type = DB_TYPE_UINT;
  reply->value.unsigned_int = deleted_count;
}

static void db_lpush(DBRequest *request, DBReply *reply)
{
  DBArg *arg1 = request->arg_head;
  DBArg *arg2 = arg1 ? arg1->next : NULL;

  if (!arg1 || !arg2)
  {
    set_reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DLList *list = get_or_create_dllist(arg1->value.string);

  if (!list)
  {
    set_reply_error(reply, DB_ERR_WRONGTYPE);
    return;
  }

  while (arg2)
  {
    list->head = create_dlnode(arg2->value.string, NULL, list->head);
    if (!list->tail)
      list->tail = list->head;
    ++list->length;
    arg2 = arg2->next;
  }

  reply->type = DB_TYPE_UINT;
  reply->value.unsigned_int = list->length;
}

static void db_lpop(DBRequest *request, DBReply *reply)
{
  DBArg *arg1 = request->arg_head;
  DBArg *arg2 = arg1 ? arg1->next ? arg_string_to_uint(arg1->next) : add_arg_uint(request, 1) : NULL;

  if (!arg1 || !arg2)
  {
    set_reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DLList *list = get_dllist(arg1->value.string);

  if (!list)
  {
    reply->type = DB_TYPE_NULL;
    return;
  }

  DLList *reply_list = create_dllist();
  DLNode *first_removed_node = list->head;
  DLNode *last_removed_node = first_removed_node;
  db_uint_t count = arg2->value.unsigned_int;

  reply->type = DB_TYPE_LIST;
  reply->value.list = reply_list;

  if (!count || !last_removed_node)
    return;

  db_uint_t counted = 0;

  while (++counted < count && last_removed_node)
    last_removed_node = last_removed_node->next;

  if (last_removed_node)
  {
    list->head = last_removed_node->next;
    last_removed_node->next = NULL;
  }

  if (list->head)
    list->head->prev = NULL;
  else
    list->tail = NULL;

  list->length -= counted;

  reply_list->head = first_removed_node;
  reply_list->tail = last_removed_node;
  reply_list->length = counted;
}

static void db_rpush(DBRequest *request, DBReply *reply)
{
  DBArg *arg1 = request->arg_head;
  DBArg *arg2 = arg1 ? arg1->next : NULL;

  if (!arg1 || !arg2)
  {
    set_reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DLList *list = get_or_create_dllist(arg1->value.string);

  if (!list)
  {
    set_reply_error(reply, DB_ERR_WRONGTYPE);
    return;
  }

  while (arg2)
  {
    list->tail = create_dlnode(arg2->value.string, list->tail, NULL);
    if (!list->head)
      list->head = list->tail;
    ++list->length;
    arg2 = arg2->next;
  }

  reply->type = DB_TYPE_UINT;
  reply->value.unsigned_int = list->length;
}

static void db_rpop(DBRequest *request, DBReply *reply)
{
  DBArg *arg1 = request->arg_head;
  DBArg *arg2 = arg1 ? arg1->next ? arg_string_to_uint(arg1->next) : add_arg_uint(request, 1) : NULL;

  if (!arg1 || !arg2)
  {
    set_reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DLList *list = get_dllist(arg1->value.string);

  if (!list)
  {
    reply->type = DB_TYPE_NULL;
    return;
  }

  DLList *reply_list = create_dllist();
  DLNode *first_removed_node = list->tail;
  DLNode *last_removed_node = first_removed_node;
  db_uint_t count = arg2->value.unsigned_int;

  reply->type = DB_TYPE_LIST;
  reply->value.list = reply_list;

  if (!count || !last_removed_node)
    return;

  db_uint_t counted = 0;

  while (++counted < count && first_removed_node)
    first_removed_node = first_removed_node->prev;

  if (first_removed_node)
  {
    list->tail = first_removed_node->prev;
    first_removed_node->prev = NULL;
  }

  if (list->tail)
    list->tail->next = NULL;
  else
    list->head = NULL;

  list->length -= counted;

  reply_list->head = first_removed_node;
  reply_list->tail = last_removed_node;
  reply_list->length = counted;
}

static void db_llen(DBRequest *request, DBReply *reply)
{
  DBArg *arg1 = request->arg_head;

  if (!arg1)
  {
    set_reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  DLList *list = get_dllist(arg1->value.string);

  reply->type = DB_TYPE_UINT;
  reply->value.unsigned_int = list ? list->length : 0;
}

static void db_lrange(DBRequest *request, DBReply *reply)
{
  DBArg *arg1 = request->arg_head;

  if (!arg1)
  {
    set_reply_error(reply, DB_ERR_ARG_ERROR);
    return;
  }

  db_uint_t start = arg1->next ? arg_string_to_uint(arg1->next)->value.unsigned_int : 0;
  db_uint_t stop = arg1->next ? arg1->next->next ? arg_string_to_uint(arg1->next->next)->value.unsigned_int : DB_UINT_MAX : DB_UINT_MAX;
  DLList *list = get_dllist(arg1->value.string);
  DLList *reply_list = create_dllist();

  reply->type = DB_TYPE_LIST;
  reply->value.list = reply_list;

  if (!list || list->length == 0 || start > stop)
    return;

  if (stop == DB_UINT_MAX || stop > list->length - 1)
    stop = list->length - 1;

  // The new node must be initialized to NULL,
  // as it will be assigned to the reply list regardless of whether it has been created.
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
      --index;
    }
    while (index >= start && curr_node)
    {
      new_node = create_dlnode(curr_node->data, NULL, new_node);
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
      new_node = create_dlnode(curr_node->data, new_node, NULL);
      if (!reply_list->head)
        reply_list->head = new_node;
      curr_node = curr_node->next;
      ++index;
    }
    reply_list->tail = new_node;
  }

  reply_list->length = stop - start + 1;
}

static void db_keys(DBRequest *request, DBReply *reply)
{
  HTEntry *entry;
  db_uint_t r;
  DLList *reply_list = create_dllist();

  reply->type = DB_TYPE_LIST;
  reply->value.list = reply_list;

  for (db_uint_t t = 0; t < 2; ++t)
  {
    if (!tables[t])
      continue;
    for (r = 0; r < tables[t]->size; ++r)
    {
      entry = tables[t]->entries[r];
      while (entry)
      {
        reply_list->tail = create_dlnode(entry->key, reply_list->tail, NULL);
        if (!reply_list->head)
          reply_list->head = reply_list->tail;
        ++reply_list->length;
        entry = entry->next;
      }
    }
  }
}

void db_config_hash_seed(db_uint_t _hash_seed)
{
  mtx_lock(lock);
  hash_seed = _hash_seed ? _hash_seed : (db_uint_t)time(NULL);
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

  db_flushall();

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

      maintenance();

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
            ++list->length;
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

static void db_shutdown()
{
  if (!is_running)
    return;

  is_running = false;
  thrd_join(worker, NULL);

  db_save();

  rehashing_index = -1;
  free_table(tables[0]);
  free_table(tables[1]);
}

static void db_save()
{
  if (!persistence_filepath)
    return;

  cJSON *root = cJSON_CreateObject();
  HTEntry *entry;
  DLNode *dllnode;
  cJSON *cjson_list;

  for (int j = 0; j < 2; ++j)
  {
    if (!tables[j])
      continue;

    for (db_uint_t i = 0; i < tables[j]->size; ++i)
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

static void db_flushall()
{
  free_table(tables[0]);
  free_table(tables[1]);

  tables[0] = create_table(INITIAL_TABLE_SIZE);
  tables[1] = NULL;

  rehashing_index = -1;
}

static DBRequest *create_request(db_action_t action)
{
  DBRequest *request = (DBRequest *)calloc(1, sizeof(DBRequest));
  if (!request)
    memory_error_handler(__FILE__, __LINE__, __func__);
  request->action = action;
  return request;
};

static DBRequest *reset_request(DBRequest *request, db_action_t action)
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

static DBArg *arg_string_to_uint(DBArg *arg)
{
  if (arg && arg->type == DB_TYPE_STRING)
  {
    arg->type = DB_TYPE_UINT;
    char *s = arg->value.string;
    if (s)
      arg->value.unsigned_int = strtoul(s, NULL, 10),
      free(s);
  }
  return arg;
}

static DBReply *set_reply_error(DBReply *reply, const char *message)
{
  if (!reply)
    return NULL;
  reply->ok = false;
  reply->type = DB_TYPE_ERROR;
  reply->value.string = helper_strdup(message);
  return reply;
}

static DBArg *add_arg_uint(DBRequest *request, db_uint_t value)
{
  if (!request)
    return NULL;
  DBArg *arg = add_arg(request, DB_TYPE_UINT);
  arg->value.unsigned_int = value;
  return arg;
};

static DBArg *add_arg_string(DBRequest *request, const char *value)
{
  if (!request)
    return NULL;
  DBArg *arg = add_arg(request, DB_TYPE_STRING);
  arg->value.string = helper_strdup(value);
  return arg;
};

static DBReply *send_request(DBRequest *request)
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

  while (true)
  {
    mtx_lock(lock);
    if (entry->done)
    {
      mtx_unlock(lock);
      break;
    }
    mtx_unlock(lock);
  }

  free(entry);

  return reply;
};

static void free_request(DBRequest *request)
{
  if (!request)
    return;
  DBArg *arg = request->arg_head;
  while (arg)
  {
    request->arg_head = arg->next;
    if (arg->type == DB_TYPE_STRING)
      free(arg->value.string);
    free(arg);
    arg = request->arg_head;
  }
  free(request);
};

static DBRequest *parse_command(const char *command)
{
  DBRequest *request = (DBRequest *)calloc(1, sizeof(DBRequest));
  if (!request)
    memory_error_handler(__FILE__, __LINE__, __func__);

  // Duplicate command for tokenization
  char *command_copy = helper_strdup(command);
  if (!command_copy)
    memory_error_handler(__FILE__, __LINE__, __func__);

  char *token = strtok(command_copy, " ");

  // Parse action string into db_action_t
  to_uppercase(token);
  if (!token)
    request->action = DB_UNKNOWN_COMMAND;
  if (strcmp(token, "SAVE") == 0)
    request->action = DB_SAVE;
  else if (strcmp(token, "START") == 0)
    request->action = DB_START;
  else if (strcmp(token, "SET") == 0)
    request->action = DB_SET;
  else if (strcmp(token, "GET") == 0)
    request->action = DB_GET;
  else if (strcmp(token, "RENAME") == 0)
    request->action = DB_RENAME;
  else if (strcmp(token, "DEL") == 0)
    request->action = DB_DEL;
  else if (strcmp(token, "LPUSH") == 0)
    request->action = DB_LPUSH;
  else if (strcmp(token, "LPOP") == 0)
    request->action = DB_LPOP;
  else if (strcmp(token, "RPUSH") == 0)
    request->action = DB_RPUSH;
  else if (strcmp(token, "RPOP") == 0)
    request->action = DB_RPOP;
  else if (strcmp(token, "LLEN") == 0)
    request->action = DB_LLEN;
  else if (strcmp(token, "LRANGE") == 0)
    request->action = DB_LRANGE;
  else if (strcmp(token, "KEYS") == 0)
    request->action = DB_KEYS;
  else if (strcmp(token, "FLUSHALL") == 0)
    request->action = DB_FLUSHALL;
  else if (strcmp(token, "INFO_DATASET_MEMORY") == 0)
    request->action = DB_INFO_DATASET_MEMORY;
  else if (strcmp(token, "SHUTDOWN") == 0)
    request->action = DB_SHUTDOWN;
  else
    request->action = DB_UNKNOWN_COMMAND;

  // Move past action in original command string
  const char *pos = command + strlen(token);

  while (*pos != '\0')
  {
    // Skip extra whitespace
    while (isspace(*pos))
      ++pos;

    if (*pos == '\0')
      break;

    DBArg *arg = NULL;
    if (*pos == '"')
    {
      // Parse quoted string
      ++pos;
      const char *start = pos;
      size_t length = 0;
      char *string_value = NULL;

      // Continue until end of string or until reaching an unescaped quote
      while (*pos != '\0' && (*pos != '"' || (*(pos - 1) == '\\' && pos != start)))
      {
        // Handle escape
        if (*pos == '\\' && *(pos + 1) == '"')
          ++pos;
        ++pos;
        ++length;
      }

      if (*pos == '"')
      {
        string_value = (char *)malloc(length + 1);
        if (!string_value)
          memory_error_handler(__FILE__, __LINE__, __func__);

        // Remove escape sequences
        size_t i = 0;
        const char *src = start;
        while (src < pos)
        {
          if (*src == '\\' && *(src + 1) == '"')
          {
            string_value[i++] = '"';
            src += 2;
          }
          else
          {
            string_value[i++] = *src++;
          }
        }
        string_value[i] = '\0';
        ++pos;

        arg = add_arg_string(request, string_value);
        free(string_value);
      }
    }
    else
    {
      // Parse signed or unsigned integer, or treat as a string
      char *endptr = NULL;
      const char *start = pos;
      while (*pos != '\0' && !isspace(*pos))
        ++pos;
      size_t length = pos - start;

      char *string_value = (char *)malloc(length + 1);
      if (!string_value)
        memory_error_handler(__FILE__, __LINE__, __func__);

      strncpy(string_value, start, length);
      string_value[length] = '\0';

      arg = add_arg_string(request, string_value);
      free(string_value);
    }

    if (!arg)
      memory_error_handler(__FILE__, __LINE__, __func__);
  }

  free(command_copy);
  return request;
}

DBReply *db_command(const char *command)
{
  if (!command)
    return NULL;
  DBRequest *request = parse_command(command);
  if (!request)
    return NULL;
  DBReply *reply = send_request(request);
  free_request(request);
  return reply;
}

bool db_is_running()
{
  mtx_lock(lock);
  bool _is_running = is_running;
  mtx_unlock(lock);
  return _is_running;
}

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

DBReply *db_print_reply(DBReply *reply)
{
  if (!reply)
  {
    printf("(nil)\n");
    return reply;
  }

  switch (reply->type)
  {
  case DB_TYPE_NULL:
    printf("(nil)\n");
    break;
  case DB_TYPE_ERROR:
    printf("(error) %s\n", reply->value.string ? reply->value.string : "");
    break;
  case DB_TYPE_STRING:
    printf("%s\n", reply->value.string ? reply->value.string : "");
    break;
  case DB_TYPE_UINT:
    printf("(uint) %lu\n", reply->value.unsigned_int);
    break;
  case DB_TYPE_LIST:
    printf("(list) count: %u\n", reply->value.list ? reply->value.list->length : 0);
    if (!reply->value.list)
      break;
    db_uint_t i = 0;
    DLNode *node = reply->value.list->head;
    while (node)
      printf("  %u) %s\n", ++i, node->data), node = node->next;
    break;
  case DB_TYPE_BOOL:
    printf("(bool) %s\n", reply->ok ? "true" : "false");
    break;
  default:
    printf("(unknown) type=%lu\n", reply->type);
    break;
  }

  return reply;
}
