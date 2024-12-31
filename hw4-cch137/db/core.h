#ifndef DB_CORE_H
#define DB_CORE_H

#include "types.h"

#define DEFAULT_PERSISTENCE_FILE "db.json"

#define NANOSECONDS_PER_SECOND 1000000000L

int core_lock();
int core_unlock();
db_bool_t core_trylock_is_success();

// Starts the database and sets db_seed to a random number
void db_start();

db_bool_t db_is_running();

void db_config_hash_seed(db_uint_t _hash_seed);

void db_config_persistence_filepath(const char *_persistence_filepath);

DBReply *db_handle_request(DBRequest *request);

// Retrieves a string from the database by key; returns NULL if not found or type mismatch
void db_get(DBRequest *request, DBReply *reply);

// Stores a string in the database with the specified key and value
// Updates the value if the key exists, otherwise creates a new entry
// Returns true if successful, false if type mismatch
void db_set(DBRequest *request, DBReply *reply);

// Renames an existing key to a new key in the database
// Removes the old entry and inserts the new one with the updated key
// Returns true if successful, false if type mismatch
void db_rename(DBRequest *request, DBReply *reply);

// Deletes an entry by key; Returns the number of successfully deleted keys
void db_del(DBRequest *request, DBReply *reply);

// Pushes elements to the front of a list; last parameter must be NULL
void db_lpush(DBRequest *request, DBReply *reply);

// Pops elements from the front of a list
void db_lpop(DBRequest *request, DBReply *reply);

// Pushes elements to the end of a list; last parameter must be NULL
void db_rpush(DBRequest *request, DBReply *reply);

// Pops elements from the end of a list
void db_rpop(DBRequest *request, DBReply *reply);

// Returns the number of nodes in a list
void db_llen(DBRequest *request, DBReply *reply);

// Returns a sublist from the specified range of indices
// The `stop` index is inclusive, and if `stop` is -1, the entire list is returned
void db_lrange(DBRequest *request, DBReply *reply);

void db_keys(DBRequest *request, DBReply *reply);

// Stops the database and saves data to a specified file
void db_shutdown(DBRequest *request, DBReply *reply);

// Saves the current state of the database to persistent storage
void db_save(DBRequest *request, DBReply *reply);

// Deletes all item from all databases.
void db_flushall(DBRequest *request, DBReply *reply);

#endif
