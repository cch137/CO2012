#ifndef CCH137DB_DATABASE_H
#define CCH137DB_DATABASE_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

typedef uint8_t DBValueType;
typedef int32_t db_count_t;
typedef uint32_t db_size_t;

#define DB_SIZE_MAX UINT32_MAX

#define DB_TYPE_STRING (DBValueType)(1)
#define DB_TYPE_LIST (DBValueType)(2)

typedef struct DLNode
{
  char *data;
  struct DLNode *prev;
  struct DLNode *next;
} DLNode;

typedef struct DLList
{
  // Pointer to the first node in the list
  DLNode *head;
  // Pointer to the last node in the list
  DLNode *tail;
  // Number of nodes in the list
  db_size_t length;
} DLList;

typedef struct HTEntry
{
  // Key for the entry
  char *key;
  // Type of the stored value (e.g., string or list)
  DBValueType type;
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
  db_size_t size;
  // Current number of entries in the hash table
  db_size_t count;
  // Array of entries
  HTEntry **entries;
} HashTable;

typedef struct DBConfig
{
  // Seed for the hash function, affecting hash distribution
  db_size_t hash_seed;
  // Threshold of idle tasks before rehashing starts
  db_size_t rehash_idle_threshold;
  // Frequency of periodic task execution per second
  uint8_t cron_hz;
  // File path for database persistence
  const char *persistence_filepath;
  // Task function to run periodically at a rate set by cron_hz
  void (*scheduled_task)();
} DBConfig;

static db_size_t hash_seed;
static db_size_t rehash_idle_threshold;
static uint8_t cron_hz;
static char *persistence_filepath;
static void (*scheduled_task)();

static HashTable *tables[2];
static int32_t rehashing_index;
static uint32_t recent_tasks_count;

static pthread_mutex_t *global_mutex;
static pthread_t cron_thread;
static bool cron_is_running; // Flag to control db_cron thread

// Computes the MurmurHash2 hash of a key
static db_size_t murmurhash2(const void *key, db_size_t len);

// Initializes a recursive mutex. If passed NULL, allocates and initializes a new mutex.
// Returns the initialized mutex, or NULL if memory allocation fails.
static pthread_mutex_t *init_pthread_mutex(pthread_mutex_t *mutex);

// Starts the database and sets db_seed to a random number
void db_start(DBConfig *config);

// Stops the database and saves data to a specified file
void db_stop();

// Saves the current state of the database to persistent storage
void db_save();

// Deletes all item from all databases.
void db_flushall();

// Returns the memory usage of the current database dataset
size_t db_dataset_memory_usage();

// Executes periodic tasks, including checking if rehashing should start
// Resets recent_tasks_count after each execution
static void cron_runner();

// Executed during each low-level operation and periodic task to maintain the hash table size
static void maintenance(int max_rehash_steps);

// Checks if rehashing is needed and performs a rehash step if required
// Returns true if additional rehash steps are required
static bool rehash_step();

// Creates a new hash table with the specified size
static HashTable *create_table(db_size_t size);

// Frees the memory allocated for a hash table
static void free_table(HashTable *table);

// Creates a new entry with the specified key and type; assigns value directly
static HTEntry *create_entry(char *key, DBValueType type, void *value);

// Frees the memory allocated for a hash table entry
static void free_entry(HTEntry *entry);

// Retrieves an entry by key; returns NULL if not found
static HTEntry *get_entry(const char *key);

// Adds an entry to the hash table
static HTEntry *add_entry(HTEntry *entry);

// Removes an entry by key; returns NULL if not found
static HTEntry *remove_entry(const char *key);

// Retrieves a string from the database by key; returns NULL if not found or type mismatch
char *db_get(const char *key);

// Stores a string in the database with the specified key and value
// Updates the value if the key exists, otherwise creates a new entry
// Returns true if successful, false if type mismatch
bool db_set(const char *key, const char *value);

// Renames an existing key to a new key in the database
// Removes the old entry and inserts the new one with the updated key
bool db_rename(const char *old_key, const char *new_key);

// Deletes an entry by key; returns true if deletion was successful
bool db_del(const char *key);

// Creates a new node for a doubly linked list with specified data
static DLNode *create_DLNode(const char *data, DLNode *prev, DLNode *next);

// Initializes a new, empty doubly linked list
static DLList *create_DLList();

// Retrieves a list by key; returns NULL if not found
static DLList *get_DLList(const char *key);

// Retrieves a list by key or creates a new one if it does not exist
static DLList *get_or_create_DLList(const char *key);

// Creates a duplicate of an existing list
static DLList *duplicate_DLList(DLList *list);

// Frees a list node and all its sibling nodes
void db_free_DLNode(DLNode *node);

// Frees an entire list and all of its nodes
void db_free_DLList(DLList *list);

// Pushes elements to the front of a list; last parameter must be NULL
db_size_t db_lpush(const char *key, ...);

// Pops elements from the front of a list
DLNode *db_lpop(const char *key, db_size_t count);

// Pushes elements to the end of a list; last parameter must be NULL
db_size_t db_rpush(const char *key, ...);

// Pops elements from the end of a list
DLNode *db_rpop(const char *key, db_size_t count);

// Returns the number of nodes in a list
db_size_t db_llen(const char *key);

// Returns a sublist from the specified range of indices
// The `stop` index is inclusive, and if `stop` is -1, the entire list is returned
DLList *db_lrange(const char *key, db_size_t start, db_size_t stop);

#endif
