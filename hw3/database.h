#ifndef CCH137DB_DATABASE_H
#define CCH137DB_DATABASE_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

typedef enum db_type_t
{
  DB_TYPE_ERROR,
  DB_TYPE_STRING,
  DB_TYPE_LIST,
  DB_TYPE_UINT,
  DB_TYPE_INT,
  DB_TYPE_BOOL
} db_type_t;

typedef enum db_action_t
{
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
  DB_STOP
} db_action_t;

typedef int32_t db_int_t;
typedef uint32_t db_uint_t;

#define DB_SIZE_MAX UINT32_MAX

typedef struct DLNode
{
  char *data;
  struct DLNode *prev;
  struct DLNode *next;
} DLNode;

typedef struct DLList
{
  DLNode *head;
  DLNode *tail;
  db_uint_t length;
} DLList;

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

typedef struct DBReply
{
  bool ok;
  db_type_t type;
  union
  {
    char *string;
    DLList *list;
    db_uint_t unsigned_int;
    db_int_t signed_int;
    bool boolean;
  } value;
} DBReply;

void db_config_hash_seed(db_uint_t hash_seed);
void db_config_cron_hz(uint8_t cron_hz);
void db_config_persistence_filepath(const char *persistence_filepath);

// Starts the database and sets db_seed to a random number
void db_start();

// Stops the database and saves data to a specified file
void db_stop();

// Saves the current state of the database to persistent storage
void db_save();

// Deletes all item from all databases.
void db_flushall();

DBRequest *db_create_request(db_action_t action);
DBRequest *db_reset_request(DBRequest *request, db_action_t action);
DBArg *db_add_arg_uint(DBRequest *request, db_uint_t uint_value);
DBArg *db_add_arg_int(DBRequest *request, db_int_t int_value);
DBArg *db_add_arg_string(DBRequest *request, const char *string_value);
DBReply *db_send_request(DBRequest *request);
void *db_free_request(DBRequest *request);

void db_free_reply(DBReply *reply);
DBReply *db_print_reply(DBReply *reply);

#endif
