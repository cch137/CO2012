#ifndef DB_ZSET_H
#define DB_ZSET_H

#include "types.h"

DBZSet *zset_create();

void free_dbzset(DBZSet *zset);

db_uint_t zadd(DBZSet *zset, db_double_t score, const char *member);

DBObj *zscore(DBZSet *zset, const char *member);

db_uint_t zcard(DBZSet *zset);

db_uint_t zcount(DBZSet *zset, db_double_t min, db_bool_t included_min, db_double_t max, db_bool_t included_max);

DBObj *zinterstore(DBList *zsets, DBList *weights, db_aggregate_t aggregate);

DBObj *zunionstore(DBList *zsets, DBList *weights, db_aggregate_t aggregate);

DBList *zrange(DBZSet *zset, db_uint_t start, db_uint_t stop, db_bool_t withscores);

DBList *zrangebyscore(DBZSet *zset, db_double_t min, db_bool_t included_min, db_double_t max, db_bool_t included_max, db_bool_t withscores);

DBObj *zrank(DBZSet *zset, const char *member, const db_bool_t withscores);

db_uint_t zrem(DBZSet *zset, const char *member);

db_uint_t zremrangebyscore(DBZSet *zset, db_double_t min, db_bool_t included_min, db_double_t max, db_bool_t included_max);

#endif
