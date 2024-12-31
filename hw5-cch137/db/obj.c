#include "types.h"
#include "utils.h"
#include "list.h"
#include "zset.h"

static DBObj *_dbobj_create(db_type_t type);
static void *_dbobj_extract_pointer(DBObj *obj);

db_bool_t dbobj_is_null(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_NULL;
};
db_bool_t dbobj_is_error(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_ERROR;
};
db_bool_t dbobj_is_bool(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_BOOL;
};
db_bool_t dbobj_is_int(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_INT;
};
db_bool_t dbobj_is_uint(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_UINT;
};
db_bool_t dbobj_is_double(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_DOUBLE;
};
db_bool_t dbobj_is_string(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_STRING;
};
db_bool_t dbobj_is_list(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_LIST;
};
db_bool_t dbobj_is_zset(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_ZSET;
};
db_bool_t dbobj_is_hash(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_HASH;
};
db_bool_t _dbobj_is_zsetele(DBObj *obj)
{
  return obj && obj->type == DB_TYPE_ZSETELE;
};

DBObj *dbobj_create_null()
{
  return _dbobj_create(DB_TYPE_NULL);
}

DBObj *dbobj_create_error(char *message)
{
  DBObj *obj = _dbobj_create(DB_TYPE_ERROR);
  obj->value.message = message;
  return obj;
}

DBObj *dbobj_create_bool(db_bool_t value)
{
  DBObj *obj = _dbobj_create(DB_TYPE_BOOL);
  obj->value.bool_value = value;
  return obj;
}

DBObj *dbobj_create_int(db_int_t value)
{
  DBObj *obj = _dbobj_create(DB_TYPE_INT);
  obj->value.int_value = value;
  return obj;
}

DBObj *dbobj_create_uint(db_uint_t value)
{
  DBObj *obj = _dbobj_create(DB_TYPE_UINT);
  obj->value.uint_value = value;
  return obj;
}

DBObj *dbobj_create_double(db_double_t value)
{
  DBObj *obj = _dbobj_create(DB_TYPE_DOUBLE);
  obj->value.double_value = value;
  return obj;
}

DBObj *dbobj_create_string(char *value)
{
  DBObj *obj = _dbobj_create(DB_TYPE_STRING);
  obj->value.string = value;
  return obj;
}

DBObj *dbobj_create_string_with_dup(const char *value)
{
  DBObj *obj = _dbobj_create(DB_TYPE_STRING);
  obj->value.string = dbutil_strdup(value);
  return obj;
}

DBObj *dbobj_create_list(DBList *value)
{
  DBObj *obj = _dbobj_create(DB_TYPE_LIST);
  obj->value.list = value;
  return obj;
}

DBObj *dbobj_create_zset(DBZSet *value)
{
  DBObj *obj = _dbobj_create(DB_TYPE_ZSET);
  obj->value.zset = value;
  return obj;
}

DBObj *dbobj_create_hash(DBHash *value)
{
  DBObj *obj = _dbobj_create(DB_TYPE_HASH);
  obj->value.hash = value;
  return obj;
}

DBObj *_dbobj_create_zsetele(DBZSetElement *value)
{
  DBObj *obj = _dbobj_create(DB_TYPE_ZSETELE);
  obj->value._zsetele = value;
  return obj;
}

void free_dbobj(DBObj *obj)
{
  if (!obj)
    return;
  switch (obj->type)
  {
  case DB_TYPE_STRING:
    free(obj->value.string);
    break;
  case DB_TYPE_LIST:
    free_dblist(obj->value.list);
    break;
  case DB_TYPE_ZSET:
    free_dbzset(obj->value.zset);
    break;
  case DB_TYPE_HASH:
    // free_dbhash(obj->value.hash);
    break;
  case DB_TYPE_ZSETELE:
    // skip, this will process in zset module
    break;
  default:
    break;
  }
  free(obj);
}

void *dbobj_extract_null(DBObj *obj)
{
  free_dbobj(obj);
  return NULL;
}
char *dbobj_extract_error(DBObj *obj)
{
  return dbobj_is_error(obj) ? _dbobj_extract_pointer(obj) : NULL;
}
db_bool_t dbobj_extract_bool(DBObj *obj)
{
  db_bool_t value = obj->value.bool_value;
  free_dbobj(obj);
  return value;
}
db_int_t dbobj_extract_int(DBObj *obj)
{
  db_int_t value = obj->value.int_value;
  free_dbobj(obj);
  return value;
}
db_uint_t dbobj_extract_uint(DBObj *obj)
{
  db_uint_t value = obj->value.uint_value;
  free_dbobj(obj);
  return value;
}
db_double_t dbobj_extract_double(DBObj *obj)
{
  db_double_t value = obj->value.double_value;
  free_dbobj(obj);
  return value;
}
char *dbobj_extract_string(DBObj *obj)
{
  if (!dbobj_is_string(obj))
    return free_dbobj(obj), NULL;
  char *string = obj->value.string;
  obj->value.string = NULL;
  return free_dbobj(obj), string;
}
DBList *dbobj_extract_list(DBObj *obj)
{
  if (!dbobj_is_list(obj))
    return free_dbobj(obj), NULL;
  DBList *list = obj->value.list;
  obj->value.list = NULL;
  return free_dbobj(obj), list;
}
DBZSet *dbobj_extract_zset(DBObj *obj)
{
  if (!dbobj_is_zset(obj))
    return free_dbobj(obj), NULL;
  DBZSet *zset = obj->value.zset;
  obj->value.zset = NULL;
  return free_dbobj(obj), zset;
}
DBHash *dbobj_extract_hash(DBObj *obj)
{
  if (!dbobj_is_hash(obj))
    return free_dbobj(obj), NULL;
  DBHash *hash = obj->value.hash;
  obj->value.hash = NULL;
  return free_dbobj(obj), hash;
}
DBZSetElement *_dbobj_extract_zsetele(DBObj *obj)
{
  if (!_dbobj_is_zsetele(obj))
    return free_dbobj(obj), NULL;
  DBZSetElement *zsetele = obj->value._zsetele;
  obj->value._zsetele = NULL;
  return free_dbobj(obj), zsetele;
}

static DBObj *_dbobj_create(db_type_t type)
{
  DBObj *obj = malloc(sizeof(DBObj));
  if (!obj)
    EXIT_ON_MEMORY_ERROR();
  obj->type = type;
  return obj;
}

static void *_dbobj_extract_pointer(DBObj *obj)
{
  void *pointer = obj->value._pointer;
  free_dbobj(obj);
  return pointer;
}
