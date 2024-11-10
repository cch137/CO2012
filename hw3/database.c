#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <malloc.h>
#include <unistd.h>

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
#define DEFAULT_REHASH_IDLE_THRESHOLD 10000
#define DEFAULT_PERSISTENCE_FILE "db.json"

// Seed for the hash function, affecting hash distribution
static db_size_t hash_seed = 0;
// Threshold of idle tasks before rehashing starts
static db_size_t rehash_idle_threshold = DEFAULT_REHASH_IDLE_THRESHOLD;
// Frequency of periodic task execution per second
static uint8_t cron_hz = DEFAULT_CRON_HZ;
// File path for database persistence
static char *persistence_filepath = NULL;
// Task function to run periodically at a rate set by cron_hz
static void (*scheduled_task)() = NULL;

// tables[0] is the main table, tables[1] is the rehash table
// During rehashing, entries are first searched and deleted from tables[1], then from tables[0].
// New entries are only written to tables[1] during rehashing.
// After rehashing is complete, tables[0] is freed and tables[1] is moved to the main table.
static HashTable *tables[2] = {NULL, NULL};

// -1 indicates no rehashing; otherwise, it's the current rehashing index
// The occurrence of rehashing is determined by periodic tasks; when rehashing starts, rehashing_index will be the last index of the table size
// Rehashing will be handled during periodic task execution and during db_insert_entry and db_get_entry.
static db_count_t rehashing_index = -1;

// Records the count of recent tasks
static db_size_t recent_tasks_count = 0;

static pthread_mutex_t *global_mutex = NULL;
static pthread_t cron_thread;
static bool cron_is_running = false;

static db_size_t murmurhash2(const void *key, db_size_t len)
{
  const db_size_t m = 0x5bd1e995;
  const int r = 24;
  db_size_t h = hash_seed ^ len;

  const unsigned char *data = (const unsigned char *)key;

  while (len >= 4)
  {
    db_size_t k = *(db_size_t *)data;
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

static pthread_mutex_t *init_pthread_mutex(pthread_mutex_t *mutex)
{
  if (!mutex)
  {
    mutex = (pthread_mutex_t *)calloc(1, sizeof(pthread_mutex_t));
    if (!mutex)
      memory_error_handler(__FILE__, __LINE__, __func__);
    pthread_mutex_init(mutex, NULL);
  }
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE_NP);
  pthread_mutex_init(mutex, &attr);
  pthread_mutexattr_destroy(&attr);
  return mutex;
}

void db_start(DBConfig *config)
{
  if (!global_mutex)
    global_mutex = init_pthread_mutex(NULL);

  pthread_mutex_lock(global_mutex);

  if (config && config->hash_seed)
    hash_seed = config->hash_seed;
  else
    hash_seed = (db_size_t)time(NULL);

  if (config && config->rehash_idle_threshold)
    rehash_idle_threshold = config->rehash_idle_threshold;
  else
    rehash_idle_threshold = DEFAULT_REHASH_IDLE_THRESHOLD;

  if (config && config->cron_hz)
    cron_hz = config->cron_hz;
  else
    cron_hz = DEFAULT_CRON_HZ;

  if (persistence_filepath)
    free(persistence_filepath);
  if (config && config->persistence_filepath)
    persistence_filepath = helper_strdup(config->persistence_filepath);
  else
    persistence_filepath = helper_strdup(DEFAULT_PERSISTENCE_FILE);

  if (config && config->scheduled_task)
    scheduled_task = config->scheduled_task;
  else
    scheduled_task = NULL;

  free_table(tables[0]);
  free_table(tables[1]);
  tables[0] = create_table(INITIAL_TABLE_SIZE);
  tables[1] = NULL;
  rehashing_index = -1;

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
        db_set(key, cJSON_GetStringValue(cjson_cursor));
      }

      else if (cJSON_IsArray(cjson_cursor))
      {
        cjson_array_cursor = cjson_cursor->child;
        while (cjson_array_cursor)
        {
          if (cJSON_IsString(cjson_array_cursor))
            db_rpush(key, cJSON_GetStringValue(cjson_array_cursor), NULL);
          cjson_array_cursor = cjson_array_cursor->next;
        }
      }

      cjson_cursor = cjson_cursor->next;
    }
  }

  // Start cron_runner thread
  cron_is_running = true;
  if (!cron_thread)
  {
    if (pthread_create(&cron_thread, NULL, (void *(*)(void *))cron_runner, NULL))
    {
      perror("pthread task allocation failed.\n");
      exit(1);
    };
  }

  pthread_mutex_unlock(global_mutex);
}

void db_stop()
{
  pthread_mutex_lock(global_mutex);

  db_save(persistence_filepath);
  rehashing_index = -1;
  free_table(tables[0]);
  free_table(tables[1]);

  // Stop cron_runner thread
  cron_is_running = false;
  if (pthread_join(cron_thread, NULL))
  {
    perror("pthread task join failed.\n");
    exit(1);
  }

  pthread_mutex_unlock(global_mutex);
  pthread_mutex_destroy(global_mutex);
  free(global_mutex);
  global_mutex = NULL;
}

void db_save()
{
  pthread_mutex_lock(global_mutex);

  cJSON *root = cJSON_CreateObject();
  HTEntry *entry;
  DLNode *dllnode;
  cJSON *cjson_list;

  for (int j = 0; j < 2; j++)
  {
    if (!tables[j])
      continue;

    for (db_size_t i = 0; i < tables[j]->size; i++)
    {
      entry = tables[j]->entries[i];
      while (entry)
      {
        switch (entry->type)
        {
        case DB_TYPE_STRING:
          cJSON_AddStringToObject(root, entry->key, entry->value.string);
          break;
        case DB_TYPE_LIST:
          cjson_list = cJSON_CreateArray();
          cJSON_AddItemToObject(root, entry->key, cjson_list);
          dllnode = entry->value.list->head;
          while (dllnode)
          {
            cJSON_AddItemToArray(cjson_list, cJSON_CreateString(dllnode->data));
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

  pthread_mutex_unlock(global_mutex);

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
  pthread_mutex_lock(global_mutex);

  free_table(tables[0]);
  free_table(tables[1]);
  tables[0] = create_table(INITIAL_TABLE_SIZE);
  tables[1] = NULL;
  rehashing_index = -1;

  pthread_mutex_unlock(global_mutex);
}

size_t db_dataset_memory_usage()
{
  pthread_mutex_lock(global_mutex);

  size_t size = 2 * sizeof(HashTable *);
  HTEntry *entry;
  DLNode *dllnode;

  for (int j = 0; j < 2; j++)
  {
    if (!tables[j])
      continue;
    size += malloc_usable_size(tables[j]);
    size += malloc_usable_size(tables[j]->entries);
    for (db_size_t i = 0; i < tables[j]->size; i++)
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

  pthread_mutex_unlock(global_mutex);
  return size;
}

static void cron_runner()
{
  while (cron_is_running)
  {

    pthread_mutex_lock(global_mutex);

    if (recent_tasks_count < rehash_idle_threshold)
      maintenance(recent_tasks_count > rehash_idle_threshold ? 0 : rehash_idle_threshold - recent_tasks_count);

    recent_tasks_count = 0;

    pthread_mutex_unlock(global_mutex);

    usleep(1000000 / DEFAULT_CRON_HZ); // Run periodically
  }
}

static void maintenance(int max_rehash_steps)
{
  pthread_mutex_lock(global_mutex);

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
    while (max_rehash_steps-- > 0 && rehash_step())
      ;

  pthread_mutex_unlock(global_mutex);
}

static bool rehash_step()
{
  pthread_mutex_lock(global_mutex);

  if (!tables[1])
    return pthread_mutex_unlock(global_mutex), false; // Not rehashing

  // Move entries from tables[0] to tables[1]
  db_size_t index;
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
    return pthread_mutex_unlock(global_mutex), false;
  }

  return pthread_mutex_unlock(global_mutex), true;
}

static HashTable *create_table(db_size_t size)
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
  db_size_t i;

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

static HTEntry *create_entry(char *key, DBValueType type, void *value)
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
    db_free_DLList(entry->value.list);

  free(entry);
}

static HTEntry *get_entry(const char *key)
{
  if (!key)
    return NULL;

  pthread_mutex_lock(global_mutex);
  recent_tasks_count++;
  maintenance(1);

  HTEntry *entry;

  if (tables[1])
  {
    entry = tables[1]->entries[murmurhash2(key, strlen(key)) % tables[1]->size];
    while (entry)
    {
      if (strcmp(entry->key, key) == 0)
        return pthread_mutex_unlock(global_mutex), entry;
      entry = entry->next;
    }
  }

  entry = tables[0]->entries[murmurhash2(key, strlen(key)) % tables[0]->size];
  while (entry)
  {
    if (strcmp(entry->key, key) == 0)
      return pthread_mutex_unlock(global_mutex), entry;
    entry = entry->next;
  }

  return pthread_mutex_unlock(global_mutex), NULL;
}

static HTEntry *add_entry(HTEntry *entry)
{
  if (!entry)
    return NULL;

  pthread_mutex_lock(global_mutex);
  recent_tasks_count++;
  maintenance(1);

  db_size_t index;

  if (tables[1])
  {
    index = murmurhash2(entry->key, strlen(entry->key)) % tables[1]->size;
    entry->next = tables[1]->entries[index];
    tables[1]->entries[index] = entry;
    tables[1]->count++;
    return pthread_mutex_unlock(global_mutex), entry;
  }

  index = murmurhash2(entry->key, strlen(entry->key)) % tables[0]->size;
  entry->next = tables[0]->entries[index];
  tables[0]->entries[index] = entry;
  tables[0]->count++;
  return pthread_mutex_unlock(global_mutex), entry;
}

static HTEntry *remove_entry(const char *key)
{
  if (!key)
    return NULL;

  pthread_mutex_lock(global_mutex);
  recent_tasks_count++;
  maintenance(1);

  HTEntry *curr_entry, *prev_entry = NULL;
  db_size_t index;

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
        return pthread_mutex_unlock(global_mutex), curr_entry;
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
      return pthread_mutex_unlock(global_mutex), curr_entry;
    }
    prev_entry = curr_entry;
    curr_entry = curr_entry->next;
  }

  return pthread_mutex_unlock(global_mutex), NULL;
}

char *db_get(const char *key)
{
  HTEntry *entry = get_entry(key);
  if (entry && entry->type == DB_TYPE_STRING)
    return entry->value.string; // Return the string value
  return NULL;                  // Not found
}

bool db_set(const char *key, const char *value)
{
  pthread_mutex_lock(global_mutex);
  HTEntry *entry = get_entry(key);
  if (!entry)
  {
    add_entry(create_entry(helper_strdup(key), DB_TYPE_STRING, helper_strdup(value)));
    return pthread_mutex_unlock(global_mutex), true;
  }
  pthread_mutex_unlock(global_mutex);
  if (entry->type == DB_TYPE_STRING)
  {
    free(entry->value.string);                  // Free old value
    entry->value.string = helper_strdup(value); // Update with new value
    return true;
  }
  return false;
}

bool db_rename(const char *old_key, const char *new_key)
{
  pthread_mutex_lock(global_mutex);
  HTEntry *entry = remove_entry(old_key);
  if (!entry)
    return pthread_mutex_unlock(global_mutex), false;
  free(entry->key);
  entry->key = helper_strdup(new_key);
  add_entry(entry);
  return pthread_mutex_unlock(global_mutex), true;
}

bool db_del(const char *key)
{
  HTEntry *entry = remove_entry(key);
  if (!entry)
    return false;
  free_entry(entry);
  return true;
}

static DLNode *create_DLNode(const char *data, DLNode *prev, DLNode *next)
{
  DLNode *node = malloc(sizeof(DLNode));
  if (!node)
    memory_error_handler(__FILE__, __LINE__, __func__);
  node->data = helper_strdup(data);
  node->prev = prev;
  node->next = next;
  return node;
}

static DLList *create_DLList()
{
  DLList *list = malloc(sizeof(DLList));
  if (!list)
    memory_error_handler(__FILE__, __LINE__, __func__);
  list->head = NULL;
  list->tail = NULL;
  list->length = 0;
  return list;
}

static DLList *get_DLList(const char *key)
{
  if (!key)
    return NULL;

  HTEntry *entry = get_entry(key);

  if (entry && entry->type == DB_TYPE_LIST)
    return entry->value.list;
  return NULL;
}

static DLList *get_or_create_DLList(const char *key)
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

  DLList *list = create_DLList();
  add_entry(create_entry(helper_strdup(key), DB_TYPE_LIST, list));

  return list;
}

static DLList *duplicate_DLList(DLList *list)
{
  DLList *new_list = create_DLList();
  DLNode *curr_node = list->tail;

  while (curr_node)
  {
    new_list->head = create_DLNode(curr_node->data, NULL, new_list->head);
    if (new_list->head->next)
      new_list->head->next->prev = new_list->head;
    if (!new_list->tail)
      new_list->tail = new_list->head;
    curr_node = curr_node->prev;
  }

  new_list->length = list->length;

  return new_list;
}

void db_free_DLNode(DLNode *node)
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

void db_free_DLList(DLList *list)
{
  if (!list)
    return;

  db_free_DLNode(list->head);
  free(list);
}

db_size_t db_lpush(const char *key, ...)
{
  if (!key)
    return 0;

  pthread_mutex_lock(global_mutex);

  DLList *list = get_or_create_DLList(key);

  if (!list)
    return pthread_mutex_unlock(global_mutex), 0;

  va_list items;
  va_start(items, key);

  DLNode *node;

  char *value;
  while ((value = (char *)va_arg(items, char *)) != NULL)
  {
    node = create_DLNode(value, NULL, list->head);
    if (list->head)
      list->head->prev = node;
    list->head = node;
    if (!list->tail)
      list->tail = node;
    list->length++;
  }

  return pthread_mutex_unlock(global_mutex), list->length;
}

DLNode *db_lpop(const char *key, db_size_t count)
{
  if (!key || count == 0)
    return NULL;

  pthread_mutex_lock(global_mutex);

  DLList *list = get_DLList(key);

  if (!list || count == 0)
    return pthread_mutex_unlock(global_mutex), NULL;

  DLNode *head_node = list->head;
  DLNode *tail_node = list->head;

  if (!head_node)
    return pthread_mutex_unlock(global_mutex), NULL;

  db_size_t counted = 1;

  while (counted < count)
  {
    if (!tail_node->next)
      break;
    tail_node = tail_node->next;
    counted++;
  }

  list->head = tail_node->next;
  if (list->head)
    list->head->prev = NULL;
  if (list->tail == tail_node)
    list->tail = NULL;
  tail_node->next = NULL;

  list->length -= counted;

  return pthread_mutex_unlock(global_mutex), head_node;
}

db_size_t db_rpush(const char *key, ...)
{
  if (!key)
    return 0;

  pthread_mutex_lock(global_mutex);

  DLList *list = get_or_create_DLList(key);

  if (!list)
    return pthread_mutex_unlock(global_mutex), 0;

  va_list items;
  va_start(items, key);

  DLNode *node;

  char *value;
  while ((value = (char *)va_arg(items, char *)) != NULL)
  {
    node = create_DLNode(value, list->tail, NULL);
    if (list->tail)
      list->tail->next = node;
    list->tail = node;
    if (!list->head)
      list->head = node;
    list->length++;
  }

  return pthread_mutex_unlock(global_mutex), list->length;
}

DLNode *db_rpop(const char *key, db_size_t count)
{
  if (!key || count == 0)
    return NULL;

  pthread_mutex_lock(global_mutex);

  DLList *list = get_DLList(key);

  if (!list || count == 0)
    return pthread_mutex_unlock(global_mutex), NULL;

  DLNode *head_node = list->tail;
  DLNode *tail_node = list->tail;

  if (!head_node)
    return pthread_mutex_unlock(global_mutex), NULL;

  db_size_t counted = 1;

  while (counted < count)
  {
    if (!tail_node->prev)
      break;
    tail_node = tail_node->prev;
    counted++;
  }

  list->tail = tail_node->prev;
  if (list->tail)
    list->tail->next = NULL;
  if (list->head == tail_node)
    list->head = NULL;
  tail_node->prev = NULL;

  list->length -= counted;

  return pthread_mutex_unlock(global_mutex), head_node;
}

db_size_t db_llen(const char *key)
{
  if (!key)
    return 0;

  pthread_mutex_lock(global_mutex);

  DLList *list = get_DLList(key);

  if (!list)
    return pthread_mutex_unlock(global_mutex), 0;

  db_size_t length = list->length;

  return pthread_mutex_unlock(global_mutex), length;
}

DLList *db_lrange(const char *key, db_size_t start, db_size_t stop)
{
  if (!key)
    return NULL;

  pthread_mutex_lock(global_mutex);

  DLList *list = get_DLList(key);

  if (!list || list->length == 0)
    return pthread_mutex_unlock(global_mutex), NULL;

  if (stop == DB_SIZE_MAX || stop > list->length - 1)
    stop = list->length - 1;

  if (start > stop)
    start = 0;

  printf("%lu %lu\n", start, stop);

  if (start == 0 && stop == list->length - 1)
    return pthread_mutex_unlock(global_mutex), duplicate_DLList(list);

  DLList *new_list = create_DLList();
  DLNode *new_node = NULL;
  DLNode *curr_node;
  db_size_t index;

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
      new_node = create_DLNode(curr_node->data, NULL, new_node);
      if (!new_list->tail)
        new_list->tail = new_node;
      if (new_node->next)
        new_node->next->prev = new_node;
      curr_node = curr_node->prev;
      index--;
    }
    new_list->head = new_node;
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
      new_node = create_DLNode(curr_node->data, new_node, NULL);
      if (!new_list->head)
        new_list->head = new_node;
      if (new_node->prev)
        new_node->prev->next = new_node;
      curr_node = curr_node->next;
      index++;
    }
    new_list->tail = new_node;
  }

  new_list->length = stop - start + 1;

  return pthread_mutex_unlock(global_mutex), new_list;
}
