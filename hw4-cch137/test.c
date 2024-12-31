// test.c
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h> // for free()

#include "db/api.h"
#include "db/utils.h"
#include "db/list.h"
#include "db/zset.h"
#include "db/obj.h"
#include "db/interaction.h"

#define RESULT_PASS "\033[0;32mPASS\033[0m"
#define RESULT_FAIL "\033[0;31mFAIL\033[0m"

typedef struct
{
  const char *source;
  const char *pattern;
  db_bool_t expected;
} TestCase;

// 提供多種列印方式，方便顯示 (Expected: X, Got: Y)
static void print_detailed_test_result_bool(const char *test_name, bool condition, bool expected, bool actual)
{
  printf("[%s] %s (Expected: %s, Got: %s)\n",
         condition ? RESULT_PASS : RESULT_FAIL,
         test_name,
         expected ? "true" : "false",
         actual ? "true" : "false");
}

static void print_detailed_test_result_int(const char *test_name, bool condition, long expected, long actual)
{
  printf("[%s] %s (Expected: %ld, Got: %ld)\n",
         condition ? RESULT_PASS : RESULT_FAIL,
         test_name, expected, actual);
}

static void print_detailed_test_result_double(const char *test_name, bool condition, double expected, double actual)
{
  printf("[%s] %s (Expected: %.2f, Got: %.2f)\n",
         condition ? RESULT_PASS : RESULT_FAIL,
         test_name, expected, actual);
}

static void print_detailed_test_result_str(const char *test_name, bool condition, const char *expected, const char *actual)
{
  printf("[%s] %s (Expected: \"%s\", Got: \"%s\")\n",
         condition ? RESULT_PASS : RESULT_FAIL,
         test_name, expected, actual ? actual : "(null)");
}

// 專門用於 test_dbutil_match_keys，模仿原本的列印方式
static void test_dbutil_match_keys()
{
  TestCase test_cases[] = {
      {"user:123", "user:*", true},
      {"user:123", "user:?23", true},
      {"user:abc", "user:abc", true},
      {"user:123", "user:1*3", true},
      {"user:xyz", "user:?yz", true},
      {"user:123", "user:123", true},
      {"user:123", "user:12\\3", true},
      {"user:*23", "user:\\*23", true},
      {"user:abc", "admin:*", false},
      {"user:abc", "user:\\?bc", false},
      {"user:abc", "user:a?c", true},
      {"user:abc", "user:a*c", true},
      {"user:abc", "user:*b*", true},
      {"user:abc", "user:??c", true},
      {"user:abc", "*", true},
      {"", "*", true},
      {"", "?", false},
      {"", "", true},
      {"abc", "a\\*c", false},
      {"a*c", "a\\*c", true},
      {"abc", "???", true},
      {"ab", "???", false},
      {"abcd", "a*d", true},
      {"abc", "a\\?c", false},
      {"a?c", "a\\?c", true},
      {"a*c", "a??c", false},
      {"abbbbc", "a*b*c", true},
      {"abbbbc", "a*c*b", false},
      {"abc", "abc\\", false},
      {"abc", "abc\\d", false},
      {"user:??x", "user:??x", true},
      {"user:?x", "user:??x", false},
      {"hello", "h*llo", true},
      {"heeeello", "h*llo", true},
      {"hey", "h*llo", false},
  };
  size_t test_count = sizeof(test_cases) / sizeof(TestCase);

  for (size_t i = 0; i < test_count; ++i)
  {
    db_bool_t result = dbutil_match_keys(test_cases[i].source, test_cases[i].pattern);
    printf("[%s] Source: \"%s\", Pattern: \"%s\" (Expected: %s, Got: %s)\n",
           (result == test_cases[i].expected) ? RESULT_PASS : RESULT_FAIL,
           test_cases[i].source,
           test_cases[i].pattern,
           test_cases[i].expected ? "true" : "false",
           result ? "true" : "false");
  }
}

// 以下為 zset 測試，將結果格式化為類似方式

static void zset_test_zadd()
{
  DBZSet *zset = zset_create();
  zadd(zset, 1, "a");
  zadd(zset, 5, "e");
  zadd(zset, 2, "b");
  db_uint_t card = zcard(zset);

  // 預期: zcard == 3
  print_detailed_test_result_int("zset_test_zadd: zcard == 3", (card == 3), 3, card);

  // 檢查 'a' 是否存在
  DBObj *a_score = zscore(zset, "a");
  bool a_exists = dbobj_is_double(a_score);
  print_detailed_test_result_bool("zset_test_zadd: 'a' exists", a_exists, true, a_exists);

  // 檢查 'e' 是否存在
  DBObj *e_score = zscore(zset, "e");
  bool e_exists = dbobj_is_double(e_score);
  print_detailed_test_result_bool("zset_test_zadd: 'e' exists", e_exists, true, e_exists);

  // 檢查 'b' 是否存在
  DBObj *b_score = zscore(zset, "b");
  bool b_exists = dbobj_is_double(b_score);
  print_detailed_test_result_bool("zset_test_zadd: 'b' exists", b_exists, true, b_exists);

  free_dbobj(a_score);
  free_dbobj(e_score);
  free_dbobj(b_score);
  free_dbzset(zset);
}

static void zset_test_zscore()
{
  DBZSet *zset = zset_create();
  zadd(zset, 1, "a");

  DBObj *score_obj = zscore(zset, "a");
  bool is_double = dbobj_is_double(score_obj);
  double got_score = is_double ? score_obj->value.double_value : 0.0;
  print_detailed_test_result_double("zset_test_zscore: score of 'a' == 1", (is_double && got_score == 1.0), 1.0, got_score);

  DBObj *null_obj = zscore(zset, "no_such_member");
  bool is_null = dbobj_is_null(null_obj);
  print_detailed_test_result_bool("zset_test_zscore: no_such_member is null", is_null, true, is_null);

  free_dbobj(score_obj);
  free_dbobj(null_obj);
  free_dbzset(zset);
}

static void zset_test_zcard()
{
  DBZSet *zset = zset_create();
  print_detailed_test_result_int("zset_test_zcard: empty zset card == 0", (zcard(zset) == 0), 0, zcard(zset));

  zadd(zset, 1, "a");
  zadd(zset, 2, "b");
  print_detailed_test_result_int("zset_test_zcard: after adding 2 elements == 2", (zcard(zset) == 2), 2, zcard(zset));

  free_dbzset(zset);
}

static void zset_test_zcount()
{
  DBZSet *zset = zset_create();
  zadd(zset, 1, "a");
  zadd(zset, 2, "b");
  zadd(zset, 3, "c");
  zadd(zset, 4, "d");
  zadd(zset, 5, "e");

  db_uint_t count_inclusive = zcount(zset, 1, true, 5, true);
  print_detailed_test_result_int("zset_test_zcount: [1,5] should be 5", (count_inclusive == 5), 5, count_inclusive);

  db_uint_t count_exclusive = zcount(zset, 1, false, 5, false);
  print_detailed_test_result_int("zset_test_zcount: (1,5) should be 3", (count_exclusive == 3), 3, count_exclusive);

  db_uint_t count_mixcase = zcount(zset, 2, false, 5, true);
  print_detailed_test_result_int("zset_test_zcount: (2,5] should be 3", (count_mixcase == 3), 3, count_mixcase);

  free_dbzset(zset);
}

static void zset_test_zrange()
{
  DBZSet *zset = zset_create();
  zadd(zset, 1, "a");
  zadd(zset, 2, "b");
  zadd(zset, 3, "c");
  zadd(zset, 4, "d");

  DBList *range_list = zrange(zset, 1, 2, false);
  // 預期取出 b, c
  bool correct = false;
  char *expected_str = "{b,c}";
  if (range_list->length == 2)
  {
    DBListNode *node = range_list->head;
    if (node && node->data && node->data->type == DB_TYPE_STRING &&
        strcmp(node->data->value.string, "b") == 0 &&
        node->next && node->next->data &&
        strcmp(node->next->data->value.string, "c") == 0)
    {
      correct = true;
    }
  }

  char actual_buf[128];
  if (range_list->length == 2)
  {
    snprintf(actual_buf, sizeof(actual_buf), "{%s,%s}",
             range_list->head->data->value.string,
             range_list->head->next->data->value.string);
  }
  else
  {
    snprintf(actual_buf, sizeof(actual_buf), "length=%lu", (unsigned long)range_list->length);
  }

  print_detailed_test_result_str("zset_test_zrange: [1,2] == {b,c}", correct, expected_str, actual_buf);

  free_dblist(range_list);
  free_dbzset(zset);
}

static void zset_test_zrangebyscore()
{
  DBZSet *zset = zset_create();
  zadd(zset, 1, "a");
  zadd(zset, 2, "b");
  zadd(zset, 3, "c");
  zadd(zset, 4, "d");

  DBList *range_list = zrangebyscore(zset, 2, true, 3, true, false);
  // 預期 b, c
  bool correct = false;
  char *expected_str = "{b,c}";
  char actual_buf[128];

  if (range_list->length == 2 &&
      strcmp(range_list->head->data->value.string, "b") == 0 &&
      strcmp(range_list->head->next->data->value.string, "c") == 0)
  {
    correct = true;
    snprintf(actual_buf, sizeof(actual_buf), "{b,c}");
  }
  else
  {
    snprintf(actual_buf, sizeof(actual_buf), "length=%lu", (unsigned long)range_list->length);
  }

  print_detailed_test_result_str("zset_test_zrangebyscore: [2,3] == {b,c}", correct, expected_str, actual_buf);

  free_dblist(range_list);
  free_dbzset(zset);
}

static void zset_test_zrank()
{
  DBZSet *zset = zset_create();
  zadd(zset, 1, "a");
  zadd(zset, 2, "b");
  zadd(zset, 3, "c");

  DBObj *rank_obj = zrank(zset, "b", false);
  bool is_int = dbobj_is_int(rank_obj);
  long got = is_int ? rank_obj->value.int_value : -1;
  print_detailed_test_result_int("zset_test_zrank: rank of 'b' == 1", (got == 1), 1, got);

  free_dbobj(rank_obj);
  free_dbzset(zset);
}

static void zset_test_zrem()
{
  DBZSet *zset = zset_create();
  zadd(zset, 1, "a");
  zadd(zset, 2, "b");
  zadd(zset, 3, "c");
  zrem(zset, "b");

  print_detailed_test_result_int("zset_test_zrem: after removing 'b', zcard == 2", (zcard(zset) == 2), 2, zcard(zset));

  DBObj *b_score = zscore(zset, "b");
  bool b_is_null = dbobj_is_null(b_score);
  print_detailed_test_result_bool("zset_test_zrem: 'b' removed", b_is_null, true, b_is_null);

  free_dbobj(b_score);
  free_dbzset(zset);
}

static void zset_test_zremrangebyscore()
{
  DBZSet *zset = zset_create();
  zadd(zset, 1, "a");
  zadd(zset, 2, "b");
  zadd(zset, 3, "c");
  zadd(zset, 4, "d");

  db_uint_t removed = zremrangebyscore(zset, 1, false, 3, false);
  print_detailed_test_result_int("zset_test_zremrangebyscore: removed count == 1", (removed == 1), 1, removed);

  DBObj *b_score = zscore(zset, "b");
  bool b_is_null = dbobj_is_null(b_score);
  print_detailed_test_result_bool("zset_test_zremrangebyscore: 'b' removed", b_is_null, true, b_is_null);

  db_uint_t c = zcard(zset);
  print_detailed_test_result_int("zset_test_zremrangebyscore: others remain (zcard==3)", (c == 3), 3, c);

  free_dbobj(b_score);
  free_dbzset(zset);
}

static void zset_test_zinterstore()
{
  DBZSet *zset1 = zset_create();
  zadd(zset1, 1, "a");
  zadd(zset1, 2, "b");
  zadd(zset1, 3, "c");

  DBZSet *zset2 = zset_create();
  zadd(zset2, 3, "c");
  zadd(zset2, 4, "b");
  zadd(zset2, 5, "d");

  DBList *zsets = create_dblist();
  rpush(zsets, create_dblistnode(dbobj_create_zset(zset1)));
  rpush(zsets, create_dblistnode(dbobj_create_zset(zset2)));

  DBZSet *res_zset = dbobj_extract_zset(zinterstore(zsets, NULL, DB_AGG_SUM));

  db_uint_t card = zcard(res_zset);
  print_detailed_test_result_int("zset_test_zinterstore: zcard == 2", (card == 2), 2, card);

  DBObj *b_score = zscore(res_zset, "b");
  DBObj *c_score = zscore(res_zset, "c");

  bool b_ok = dbobj_is_double(b_score) && b_score->value.double_value == (2 + 4);
  bool c_ok = dbobj_is_double(c_score) && c_score->value.double_value == (3 + 3);

  print_detailed_test_result_double("zset_test_zinterstore: 'b' score == 6", b_ok, 6.0, dbobj_is_double(b_score) ? b_score->value.double_value : -1);
  print_detailed_test_result_double("zset_test_zinterstore: 'c' score == 6", c_ok, 6.0, dbobj_is_double(c_score) ? c_score->value.double_value : -1);

  free_dbzset(res_zset);
  free_dbobj(b_score);
  free_dbobj(c_score);
  free_dblist(zsets);
}

static void zset_test_zunionstore()
{
  DBZSet *zset1 = zset_create();
  zadd(zset1, 1, "a");
  zadd(zset1, 2, "b");

  DBZSet *zset2 = zset_create();
  zadd(zset2, 3, "b");
  zadd(zset2, 4, "c");

  DBList *zsets = create_dblist();
  rpush(zsets, create_dblistnode(dbobj_create_zset(zset1)));
  rpush(zsets, create_dblistnode(dbobj_create_zset(zset2)));

  DBZSet *res_zset = dbobj_extract_zset(zunionstore(zsets, NULL, DB_AGG_SUM));

  db_uint_t card = zcard(res_zset);
  print_detailed_test_result_int("zset_test_zunionstore: zcard == 3", (card == 3), 3, card);

  DBObj *a_score = zscore(res_zset, "a");
  DBObj *b_score = zscore(res_zset, "b");
  DBObj *c_score = zscore(res_zset, "c");

  double a_val = dbobj_is_double(a_score) ? a_score->value.double_value : -1;
  double b_val = dbobj_is_double(b_score) ? b_score->value.double_value : -1;
  double c_val = dbobj_is_double(c_score) ? c_score->value.double_value : -1;

  print_detailed_test_result_double("zset_test_zunionstore: 'a' score == 1", (a_val == 1.0), 1.0, a_val);
  print_detailed_test_result_double("zset_test_zunionstore: 'b' score == 5", (b_val == 5.0), 5.0, b_val);
  print_detailed_test_result_double("zset_test_zunionstore: 'c' score == 4", (c_val == 4.0), 4.0, c_val);

  free_dbobj(a_score);
  free_dbobj(b_score);
  free_dbobj(c_score);
  free_dblist(zsets);
}

int main()
{
  dbapi_start_server();

  // test_dbutil_match_keys();
  zset_test_zadd();
  zset_test_zscore();
  zset_test_zcard();
  zset_test_zcount();
  zset_test_zrange();
  zset_test_zrangebyscore();
  zset_test_zrank();
  zset_test_zrem();
  zset_test_zremrangebyscore();
  zset_test_zinterstore();
  zset_test_zunionstore();

  printf("DONE!\n");

  return 0;
}
