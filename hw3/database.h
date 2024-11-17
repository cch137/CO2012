#ifndef CCH137DB_DATABASE_H
#define CCH137DB_DATABASE_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

typedef enum db_type_t
{
  DB_TYPE_NULL,
  DB_TYPE_ERROR,
  DB_TYPE_STRING,
  DB_TYPE_LIST,
  DB_TYPE_UINT,
  DB_TYPE_BOOL
} db_type_t;

typedef int32_t db_int_t;
typedef uint32_t db_uint_t;

#define DB_UINT_MAX UINT32_MAX

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
void db_config_persistence_filepath(const char *persistence_filepath);

// Starts the database and sets db_seed to a random number
void db_start();

DBReply *db_command(const char *command);

bool db_is_running();

void db_free_reply(DBReply *reply);

DBReply *db_print_reply(DBReply *reply);

#endif
