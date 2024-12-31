#include <string.h>

#include "utils.h"
#include "list.h"
#include "hash.h"

db_uint_t hash_seed = 0;

static inline db_bool_t ht_is_rehashing(DBHash *ht)
{
  return ht->rehashing_index != -1;
}

// Computes the MurmurHash2 hash of a key
static db_uint_t murmurhash2(const void *key, db_uint_t len);

// Executed during each low-level operation and periodic task to maintain the hash table size
static void _ht_maintenance(DBHash *ht);
// Checks if rehashing is needed and performs a rehash step if required
// Returns true if additional rehash steps are required
static db_bool_t _ht_rehash_step(DBHash *ht);

static void _ht_resize_table(DBHash *ht, int ht_index, db_uint_t new_size);

static void _ht_clear(DBHash *ht);

static DBHashEntry *_ht_create_entry(char *key);

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

static void _ht_maintenance(DBHash *ht)
{
  if (!ht_is_rehashing(ht))
  {
    if (ht->count0 > HT_LOAD_FACTOR_EXPAND * ht->size0)
    {
      ht->rehashing_index = ht->size0 - 1;
      _ht_resize_table(ht, 1, ht->size0 * 2);
    }
    else if (ht->size0 > HT_INITIAL_SIZE && ht->count0 < HT_LOAD_FACTOR_SHRINK * ht->size0)
    {
      ht->rehashing_index = ht->size0 - 1;
      _ht_resize_table(ht, 1, ht->size0 / 2);
    }
  }
  else
  {
    _ht_rehash_step(ht);
  }
}

static db_bool_t _ht_rehash_step(DBHash *ht)
{
  if (!ht_is_rehashing(ht))
    return false; // Not rehashing

  // Move entries from tables[0] to tables[1]
  db_uint_t index;
  DBHashEntry *curr_entry = ht->buckets0[ht->rehashing_index];
  DBHashEntry *next_entry;

  while (curr_entry)
  {
    next_entry = curr_entry->next;
    index = murmurhash2(curr_entry->key, strlen(curr_entry->key)) % ht->size1;
    curr_entry->next = ht->buckets1[index];
    ht->buckets1[index] = curr_entry;
    ++ht->count1;
    --ht->count0;
    curr_entry = next_entry;
  }

  ht->buckets0[ht->rehashing_index] = NULL;
  --ht->rehashing_index;

  if (ht->rehashing_index == (int32_t)(-1))
  {
    // swap tables
    ht->size0 = ht->size1;
    ht->count0 = ht->count1;
    ht->buckets0 = ht->buckets1;
    ht->count1 = 0;
    ht->buckets1 = NULL;
    _ht_resize_table(ht, 1, 0);
    return false;
  }

  return true;
}

static void _ht_resize_table(DBHash *ht, int ht_index, db_uint_t new_size)
{
  if (ht_index == 0)
  {
    if (ht->count0 != 0)
      EXIT_ON_ERROR("Hash table is not empty");

    ht->size0 = new_size;
    free(ht->buckets0);
    if (new_size)
    {
      ht->buckets0 = (DBHashEntry **)calloc(new_size, sizeof(DBHashEntry *));
      if (!ht->buckets0)
        EXIT_ON_MEMORY_ERROR();
    }
    else
    {
      ht->buckets0 = NULL;
    }
  }
  else if (ht_index == 1)
  {
    if (ht->count1 != 0)
      EXIT_ON_ERROR("Hash table is not empty");

    ht->size1 = new_size;
    free(ht->buckets1);
    if (new_size)
    {
      ht->buckets1 = (DBHashEntry **)calloc(new_size, sizeof(DBHashEntry *));
      if (!ht->buckets1)
        EXIT_ON_MEMORY_ERROR();
    }
  }
}

static void _ht_clear(DBHash *ht)
{
  if (!ht)
    return;

  db_uint_t i, j;
  DBHashEntry *entry, *next;

  if (ht->buckets0)
  {
    for (i = 0; i < ht->size0; ++i)
    {
      if (!ht->buckets0)
        continue;
      entry = ht->buckets0[i];
      while (entry)
      {
        next = entry->next;
        ht_free_entry(entry);
        entry = next;
      }
    }
    free(ht->buckets0);
    ht->count0 = 0;
    ht->size0 = 0;
    ht->buckets0 = NULL;
  }

  if (ht->buckets1)
  {
    for (i = 0; i < ht->size1; ++i)
    {
      if (!ht->buckets1)
        continue;
      entry = ht->buckets1[i];
      while (entry)
      {
        next = entry->next;
        ht_free_entry(entry);
        entry = next;
      }
    }
    free(ht->buckets1);
    ht->count1 = 0;
    ht->size1 = 0;
    ht->buckets1 = NULL;
  }

  ht->rehashing_index = -1;
}

DBHash *ht_create()
{
  DBHash *ht = (DBHash *)malloc(sizeof(DBHash));

  if (!ht)
    EXIT_ON_MEMORY_ERROR();

  ht->rehashing_index = -1;
  ht->count0 = 0;
  ht->buckets0 = NULL;
  _ht_resize_table(ht, 0, HT_INITIAL_SIZE);
  ht->count1 = 0;
  ht->buckets1 = NULL;
  _ht_resize_table(ht, 1, 0);

  return ht;
}

void ht_free(DBHash *ht)
{
  _ht_clear(ht);
  free(ht);
}

void ht_reset(DBHash *ht)
{
  if (!ht)
    return;
  _ht_clear(ht);
  _ht_resize_table(ht, 0, HT_INITIAL_SIZE);
  _ht_resize_table(ht, 1, 0);
}

static DBHashEntry *_ht_create_entry(char *key)
{
  if (!key)
    return NULL;

  DBHashEntry *entry = (DBHashEntry *)malloc(sizeof(DBHashEntry));

  if (!entry)
    EXIT_ON_MEMORY_ERROR();

  entry->key = key;
  entry->next = NULL;
  entry->data = NULL;

  return entry;
}

DBHashEntry *ht_create_double_entry(char *key, db_double_t value)
{
  if (!key || !value)
    return NULL;
  DBHashEntry *entry = _ht_create_entry(key);
  entry->data = dbobj_create_double(value);
  return entry;
}

DBHashEntry *ht_create_string_entry(char *key, char *value)
{
  if (!key || !value)
    return NULL;
  DBHashEntry *entry = _ht_create_entry(key);
  entry->data = dbobj_create_string(value);
  return entry;
}

DBHashEntry *ht_create_string_entry_with_dup(const char *key, const char *value)
{
  return ht_create_string_entry(dbutil_strdup(key), dbutil_strdup(value));
}

DBHashEntry *ht_create_list_entry(char *key, DBList *value)
{
  if (!key || !value)
    return NULL;
  DBHashEntry *entry = _ht_create_entry(key);
  entry->data = dbobj_create_list(value);
  return entry;
}

DBHashEntry *_ht_create_zsetele_entry(char *key, DBZSetElement *value)
{
  if (!key || !value)
    return NULL;
  DBHashEntry *entry = _ht_create_entry(key);
  entry->data = _dbobj_create_zsetele(value);
  return entry;
}

void ht_free_entry(DBHashEntry *entry)
{
  if (!entry)
    return;

  free(entry->key);
  free_dbobj(entry->data);
  free(entry);
}

DBObj *ht_extract_entry(DBHashEntry *entry)
{
  if (!entry)
    return NULL;

  DBObj *data = entry->data;
  entry->data = NULL;

  free(entry->key);
  free(entry);

  return data;
}

DBHashEntry *ht_get_entry(DBHash *ht, const char *key)
{
  if (!ht || !key)
    return NULL;

  _ht_maintenance(ht);

  DBHashEntry *entry;

  if (ht_is_rehashing(ht))
  {
    entry = ht->buckets1[murmurhash2(key, strlen(key)) % ht->size1];
    while (entry)
    {
      if (strcmp(entry->key, key) == 0)
        return entry;
      entry = entry->next;
    }
  }

  entry = ht->buckets0[murmurhash2(key, strlen(key)) % ht->size0];
  while (entry)
  {
    if (strcmp(entry->key, key) == 0)
      return entry;
    entry = entry->next;
  }

  return NULL;
}

DBHashEntry *ht_add_entry(DBHash *ht, DBHashEntry *entry)
{
  if (!ht || !entry)
    return NULL;

  _ht_maintenance(ht);

  db_uint_t index;

  if (ht_is_rehashing(ht))
  {
    index = murmurhash2(entry->key, strlen(entry->key)) % ht->size1;
    entry->next = ht->buckets1[index];
    ht->buckets1[index] = entry;
    ++ht->count1;
    return entry;
  }

  index = murmurhash2(entry->key, strlen(entry->key)) % ht->size0;
  entry->next = ht->buckets0[index];
  ht->buckets0[index] = entry;
  ++ht->count0;
  return entry;
}

DBHashEntry *ht_remove_entry(DBHash *ht, const char *key)
{
  if (!ht || !key)
    return NULL;

  _ht_maintenance(ht);

  DBHashEntry *curr_entry, *prev_entry = NULL;
  db_uint_t index;

  if (ht_is_rehashing(ht))
  {
    index = murmurhash2(key, strlen(key)) % ht->size1;
    curr_entry = ht->buckets1[index];
    while (curr_entry)
    {
      if (strcmp(curr_entry->key, key) == 0)
      {
        if (prev_entry)
          prev_entry->next = curr_entry->next;
        else
          ht->buckets1[index] = curr_entry->next;
        --ht->count1;
        return curr_entry;
      }
      prev_entry = curr_entry;
      curr_entry = curr_entry->next;
    }
  }

  index = murmurhash2(key, strlen(key)) % ht->size0;
  curr_entry = ht->buckets0[index];
  prev_entry = NULL;
  while (curr_entry)
  {
    if (strcmp(curr_entry->key, key) == 0)
    {
      if (prev_entry)
        prev_entry->next = curr_entry->next;
      else
        ht->buckets0[index] = curr_entry->next;
      --ht->count0;
      return curr_entry;
    }
    prev_entry = curr_entry;
    curr_entry = curr_entry->next;
  }

  return NULL;
}
