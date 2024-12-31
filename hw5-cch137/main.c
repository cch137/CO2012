#include "db/api.h"

int main()
{
  dbapi_start_server();

  // test

  dbapi_flushall();
  dbapi_set("author", "cch");
  dbapi_set("author", "cch137");
  dbapi_set("hw", "3");
  dbapi_set("foo", "bar");
  dbapi_del("foo");
  dbapi_rpush_n("list1", "a", "b", "c", "d", "e", "f", "g", NULL);
  dbapi_lpush_n("list2", "x", "y", "z", NULL);
  dbapi_free(dbapi_rpop("list1"));
  dbapi_free(dbapi_rpop("list1"));
  dbapi_free(dbapi_lpop("list2"));
  dbapi_save();

  dbapi_start_terminal_client();

  return 0;
}
