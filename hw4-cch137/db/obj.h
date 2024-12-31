#ifndef DB_OBJ_H
#define DB_OBJ_H

#include "types.h"

db_bool_t dbobj_is_null(DBObj *obj);
db_bool_t dbobj_is_error(DBObj *obj);
db_bool_t dbobj_is_bool(DBObj *obj);
db_bool_t dbobj_is_int(DBObj *obj);
db_bool_t dbobj_is_uint(DBObj *obj);
db_bool_t dbobj_is_double(DBObj *obj);
db_bool_t dbobj_is_string(DBObj *obj);
db_bool_t dbobj_is_list(DBObj *obj);
db_bool_t dbobj_is_zset(DBObj *obj);
db_bool_t dbobj_is_hash(DBObj *obj);
db_bool_t _dbobj_is_zsetele(DBObj *obj);

DBObj *dbobj_create_null();
DBObj *dbobj_create_error(char *message);
DBObj *dbobj_create_bool(db_bool_t value);
DBObj *dbobj_create_int(db_int_t value);
DBObj *dbobj_create_uint(db_uint_t value);
DBObj *dbobj_create_double(db_double_t value);
DBObj *dbobj_create_string(char *value);
DBObj *dbobj_create_string_with_dup(const char *value);
DBObj *dbobj_create_list(DBList *value);
DBObj *dbobj_create_zset(DBZSet *value);
DBObj *dbobj_create_hash(DBHash *value);
DBObj *_dbobj_create_zsetele(DBZSetElement *value);

void free_dbobj(DBObj *obj);
void *dbobj_extract_null(DBObj *obj);
char *dbobj_extract_error(DBObj *obj);
db_bool_t dbobj_extract_bool(DBObj *obj);
db_int_t dbobj_extract_int(DBObj *obj);
db_uint_t dbobj_extract_uint(DBObj *obj);
db_double_t dbobj_extract_double(DBObj *obj);
char *dbobj_extract_string(DBObj *obj);
DBList *dbobj_extract_list(DBObj *obj);
DBZSet *dbobj_extract_zset(DBObj *obj);
DBHash *dbobj_extract_hash(DBObj *obj);
DBZSetElement *_dbobj_extract_zsetele(DBObj *obj);

#endif
