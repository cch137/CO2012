#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>

#include "utils.h"
#include "interaction.h"
#include "list.h"
#include "core.h"
#include "api.h"

// Parses command string into DBRequest structure
static DBRequest *parse_command(const char *command);

static db_bool_t reply_is_error(const DBReply *reply);

db_bool_t server_is_running()
{
  core_lock();
  db_bool_t _is_running = db_is_running();
  core_unlock();
  return _is_running;
}

void server_config_hash_seed(db_uint_t hash_seed)
{
  core_lock();
  db_config_hash_seed(hash_seed);
  core_unlock();
}

void server_config_persistence_filepath(const char *persistence_filepath)
{
  core_lock();
  db_config_persistence_filepath(persistence_filepath);
  core_unlock();
}

void dbapi_start_server()
{
  core_lock();
  db_start();
  core_unlock();
}

void dbapi_start_terminal_client()
{
  char *command_buffer = NULL;
  DBRequest *request = NULL;
  DBRequest *reply = NULL;

  dbapi_start_server();
  printf("Welcome to cch137's database!\n");
  printf("Please use commands to interact with the database.\n");

  while (server_is_running())
  {
    printf("> ");
    command_buffer = input_string();
    if (!command_buffer)
      continue;
    request = parse_command(command_buffer);
    free_reply(print_reply(dbapi_request_sync(request)));
    free_request(request);
    free(command_buffer);
  }

  printf("Goodbye! The database has been shut down.\n");
}

void dbapi_run_command(const char *command)
{
  DBRequest *request = parse_command(command);
  free_reply(dbapi_request_sync(request));
  free_request(request);
}

DBReply *dbapi_request_async(DBRequest *request)
{
  if (!request)
    return NULL;

  core_lock();
  DBReply *reply = db_handle_request(request);
  core_unlock();

  return reply;
};

DBReply *dbapi_request_sync(DBRequest *request)
{
  return dbapi_await_reply(dbapi_request_async(request));
};

DBReply *dbapi_await_reply(DBReply *reply)
{
  if (!reply)
    return NULL;

  while (true)
  {
    core_lock();
    if (reply->done)
    {
      core_unlock();
      break;
    }
    core_unlock();
  }

  return reply;
};

static DBRequest *parse_command(const char *command)
{
  if (!command)
    return NULL;

  DBRequest *request = (DBRequest *)calloc(1, sizeof(DBRequest));
  if (!request)
    EXIT_ON_MEMORY_ERROR();

  // Duplicate command for tokenization
  char *command_copy = dbutil_strdup(command);
  if (!command_copy)
    EXIT_ON_MEMORY_ERROR();

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
  else if (strcmp(token, "HGET") == 0)
    request->action = DB_HGET;
  else if (strcmp(token, "HSET") == 0)
    request->action = DB_HSET;
  else if (strcmp(token, "HDEL") == 0)
    request->action = DB_HDEL;
  else if (strcmp(token, "EXPIRE") == 0)
    request->action = DB_EXPIRE;
  else if (strcmp(token, "ZSCORE") == 0)
    request->action = DB_ZSCORE;
  else if (strcmp(token, "ZADD") == 0)
    request->action = DB_ZADD;
  else if (strcmp(token, "ZCARD") == 0)
    request->action = DB_ZCARD;
  else if (strcmp(token, "ZCOUNT") == 0)
    request->action = DB_ZCOUNT;
  else if (strcmp(token, "ZINTERSTORE") == 0)
    request->action = DB_ZINTERSTORE;
  else if (strcmp(token, "ZUNIONSTORE") == 0)
    request->action = DB_ZUNIONSTORE;
  else if (strcmp(token, "ZRANGE") == 0)
    request->action = DB_ZRANGE;
  else if (strcmp(token, "ZRANGEBYSCORE") == 0)
    request->action = DB_ZRANGEBYSCORE;
  else if (strcmp(token, "ZRANK") == 0)
    request->action = DB_ZRANK;
  else if (strcmp(token, "ZREM") == 0)
    request->action = DB_ZREM;
  else if (strcmp(token, "ZREMRANGEBYSCORE") == 0)
    request->action = DB_ZREMRANGEBYSCORE;
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
          EXIT_ON_MEMORY_ERROR();

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

        add_request_arg(request, dbobj_create_string_with_dup(string_value));
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
        EXIT_ON_MEMORY_ERROR();

      strncpy(string_value, start, length);
      string_value[length] = '\0';

      add_request_arg(request, dbobj_create_string_with_dup(string_value));
      free(string_value);
    }
  }

  free(command_copy);
  return request;
}

static db_bool_t reply_is_error(const DBReply *reply)
{
  return reply && reply->data && reply->data->type == DB_TYPE_ERROR;
}

char *dbapi_get(const char *key)
{
  DBRequest *request = create_request(DB_GET);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return NULL;
  }
  char *result = reply->data->value.string;
  reply->data->value.string = NULL;
  free_reply(reply);
  return result;
}

db_bool_t dbapi_set(const char *key, const char *value)
{
  DBRequest *request = create_request(DB_SET);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  add_request_arg(request, dbobj_create_string_with_dup(value));
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return false;
  }
  db_bool_t result = dbobj_is_string(reply->data) && strcmp(reply->data->value.string, OK) == 0;
  free_reply(reply);
  return result;
}

db_uint_t dbapi_del(const char *key)
{
  DBRequest *request = create_request(DB_DEL);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return false;
  }
  db_uint_t result = reply->data->value.uint_value;
  free_reply(reply);
  return result;
}

db_bool_t dbapi_rename(const char *old_key, const char *new_key)
{
  DBRequest *request = create_request(DB_RENAME);
  add_request_arg(request, dbobj_create_string_with_dup(old_key));
  add_request_arg(request, dbobj_create_string_with_dup(new_key));
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return false;
  }
  db_bool_t result = dbobj_is_string(reply->data) && strcmp(reply->data->value.string, OK) == 0;
  free_reply(reply);
  return result;
}

db_uint_t dbapi_lpush(const char *key, const char *value)
{
  DBRequest *request = create_request(DB_LPUSH);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  add_request_arg(request, dbobj_create_string_with_dup(value));
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return 0;
  }
  db_uint_t result = reply->data->value.uint_value;
  free_reply(reply);
  return result;
}

db_uint_t dbapi_lpush_n(const char *key, ...)
{
  DBRequest *request = create_request(DB_LPUSH);
  va_list args;
  va_start(args, key);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  while (true)
  {
    const char *value = va_arg(args, const char *);
    if (value == NULL)
      break;
    add_request_arg(request, dbobj_create_string_with_dup(value));
  }
  va_end(args);
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return 0;
  }
  db_uint_t result = reply->data->value.uint_value;
  free_reply(reply);
  return result;
}

char *dbapi_lpop(const char *key)
{
  DBRequest *request = create_request(DB_LPOP);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return NULL;
  }
  char *result = reply->data->value.string;
  reply->data->value.string = NULL;
  free_reply(reply);
  return result;
}

db_uint_t dbapi_rpush(const char *key, const char *value)
{
  DBRequest *request = create_request(DB_RPUSH);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  add_request_arg(request, dbobj_create_string_with_dup(value));
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return 0;
  }
  db_uint_t result = reply->data->value.uint_value;
  free_reply(reply);
  return result;
}

db_uint_t dbapi_rpush_n(const char *key, ...)
{
  DBRequest *request = create_request(DB_RPUSH);
  va_list args;
  va_start(args, key);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  while (true)
  {
    const char *value = va_arg(args, const char *);
    if (value == NULL)
      break;
    add_request_arg(request, dbobj_create_string_with_dup(value));
  }
  va_end(args);
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return 0;
  }
  db_uint_t result = reply->data->value.uint_value;
  free_reply(reply);
  return result;
}

char *dbapi_rpop(const char *key)
{
  DBRequest *request = create_request(DB_RPOP);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return NULL;
  }
  char *result = reply->data->value.string;
  reply->data->value.string = NULL;
  free_reply(reply);
  return result;
}

db_uint_t dbapi_llen(const char *key)
{
  DBRequest *request = create_request(DB_LLEN);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return 0;
  }
  db_uint_t result = reply->data->value.uint_value;
  free_reply(reply);
  return result;
}

DBList *dbapi_lrange(const char *key, const db_uint_t start, const db_uint_t end)
{
  DBRequest *request = create_request(DB_LRANGE);
  add_request_arg(request, dbobj_create_string_with_dup(key));
  add_request_arg(request, dbobj_create_uint(start));
  add_request_arg(request, dbobj_create_uint(end));
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return NULL;
  }
  DBList *result = reply->data->value.list;
  reply->data->value.list = NULL;
  free_reply(reply);
  return result;
}

DBList *dbapi_keys()
{
  DBRequest *request = create_request(DB_KEYS);
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return NULL;
  }
  DBList *result = reply->data->value.list;
  reply->data->value.list = NULL;
  free_reply(reply);
  return result;
}

db_bool_t dbapi_shutdown()
{
  DBRequest *request = create_request(DB_SHUTDOWN);
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return false;
  }
}

db_bool_t dbapi_save()
{
  DBRequest *request = create_request(DB_SAVE);
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return false;
  }
  db_bool_t result = dbobj_is_string(reply->data) && strcmp(reply->data->value.string, OK) == 0;
  free_reply(reply);
  return result;
}

db_bool_t dbapi_flushall()
{
  DBRequest *request = create_request(DB_FLUSHALL);
  DBReply *reply = dbapi_request_sync(request);
  free_request(request);
  if (reply_is_error(reply))
  {
    free_reply(reply);
    return false;
  }
  db_bool_t result = dbobj_is_string(reply->data) && strcmp(reply->data->value.string, OK) == 0;
  free_reply(reply);
  return result;
}

void dbapi_free(char *s)
{
  free(s);
}

void dbapi_free_list(DBList *list)
{
  free_dblist(list);
}
