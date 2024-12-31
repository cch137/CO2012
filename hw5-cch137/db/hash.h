#include "types.h"

// Initial size of the hash table
#define HT_INITIAL_SIZE 16
// Load factor threshold for expanding the hash table
#define HT_LOAD_FACTOR_EXPAND 0.7
// Load factor threshold for shrinking the hash table
#define HT_LOAD_FACTOR_SHRINK 0.1

// Seed for the hash function, affecting hash distribution
extern db_uint_t hash_seed;

// Creates a new hash table context
DBHash *ht_create();

// Frees the memory allocated for a hash table context
void ht_free(DBHash *ht);

void ht_reset(DBHash *ht);

void ht_maintain_expires(DBHash *ht, DBHash *expires_ht, db_uint8_t index);

DBHashEntry *ht_create_entry(char *key, DBObj *obj);

DBObj *ht_extract_entry(DBHashEntry *entry);

db_bool_t ht_free_entry(DBHashEntry *entry);

// Retrieves an entry by key; returns NULL if not found
DBHashEntry *hget(DBHash *ht, const char *key, DBHash *expires_ht);

db_bool_t hset(DBHash *ht, const char *key, DBObj *value, DBHash *expires_ht);

// Removes an entry by key; returns NULL if not found
DBHashEntry *ht_remove(DBHash *ht, const char *key, DBHash *expires_ht);

db_bool_t hdel(DBHash *ht, const char *key, DBHash *expires_ht);

db_bool_t ht_has(DBHash *ht, const char *key, DBHash *expires_ht);

db_bool_t ht_rename(DBHash *ht, const char *old_key, const char *new_key, DBHash *expires_ht);

DBList *ht_keys(DBHash *ht, DBHash *expires_ht);
