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

DBHashEntry *ht_create_double_entry(char *key, db_double_t value);
DBHashEntry *ht_create_string_entry(char *key, char *value);
DBHashEntry *ht_create_string_entry_with_dup(const char *key, const char *value);
DBHashEntry *ht_create_list_entry(char *key, DBList *value);
DBHashEntry *_ht_create_zsetele_entry(char *key, DBZSetElement *value);

void ht_free_entry(DBHashEntry *entry);

DBObj *ht_extract_entry(DBHashEntry *entry);

// Retrieves an entry by key; returns NULL if not found
DBHashEntry *ht_get_entry(DBHash *ht, const char *key);

// Adds an entry to the hash table
DBHashEntry *ht_add_entry(DBHash *ht, DBHashEntry *entry);

// Removes an entry by key; returns NULL if not found
DBHashEntry *ht_remove_entry(DBHash *ht, const char *key);
