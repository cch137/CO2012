#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <threads.h>

#include "database.h"

int running = true;

int db_monitor()
{
  while (running)
  {
    DBRequest *request = db_create_request(DB_INFO_DATASET_MEMORY);
    DBReply *reply = db_send_request(request);
    if (reply->ok && reply->type == DB_TYPE_UINT)
      printf("mem usage: %lu bytes\n", reply->value.unsigned_int);
    db_free_request(request);
    db_free_reply(reply);
    // sleep 0.1s
    thrd_sleep(&(struct timespec){.tv_sec = 0, .tv_nsec = 0.1 * 1000000000}, NULL);
  }
  return 0;
}

int main()
{
  db_start();

  thrd_t db_monitor_worker;
  thrd_create(&db_monitor_worker, db_monitor, NULL);

  DBRequest *request = db_create_request(DB_FLUSHALL);
  db_free_reply(db_send_request(request));

  db_reset_request(request, DB_SET);
  db_add_arg_string(request, "k1");
  db_add_arg_string(request, "v1");
  db_free_reply(db_send_request(request));

  db_reset_request(request, DB_SET);
  db_add_arg_string(request, "k2");
  db_add_arg_string(request, "v2");
  db_free_reply(db_send_request(request));

  db_reset_request(request, DB_SET);
  db_add_arg_string(request, "k3");
  db_add_arg_string(request, "v3");
  db_free_reply(db_send_request(request));

  db_reset_request(request, DB_RPUSH);
  db_add_arg_string(request, "l1");
  db_add_arg_string(request, "a");
  db_add_arg_string(request, "b");
  db_add_arg_string(request, "c");
  db_add_arg_string(request, "d");
  db_add_arg_string(request, "e");
  db_add_arg_string(request, "f");
  db_free_reply(db_send_request(request));

  db_reset_request(request, DB_LPUSH);
  db_add_arg_string(request, "l2");
  db_add_arg_string(request, "x");
  db_add_arg_string(request, "y");
  db_add_arg_string(request, "z");
  db_free_reply(db_send_request(request));

  db_reset_request(request, DB_DEL);
  db_add_arg_string(request, "k2");
  db_free_reply(db_send_request(request));

  db_reset_request(request, DB_LPOP);
  db_add_arg_string(request, "l2");
  db_free_reply(db_send_request(request));

  db_reset_request(request, DB_RPOP);
  db_add_arg_string(request, "l2");
  db_free_reply(db_send_request(request));

  printf("k1: ");
  db_reset_request(request, DB_GET);
  db_add_arg_string(request, "k1");
  db_free_reply(db_print_reply(db_send_request(request)));

  printf("k2: ");
  db_reset_request(request, DB_GET);
  db_add_arg_string(request, "k2");
  db_free_reply(db_print_reply(db_send_request(request)));

  printf("k3: ");
  db_reset_request(request, DB_GET);
  db_add_arg_string(request, "k3");
  db_free_reply(db_print_reply(db_send_request(request)));

  printf("l1 len: ");
  db_reset_request(request, DB_LLEN);
  db_add_arg_string(request, "l1");
  db_free_reply(db_print_reply(db_send_request(request)));

  printf("l2 len: ");
  db_reset_request(request, DB_LLEN);
  db_add_arg_string(request, "l2");
  db_free_reply(db_print_reply(db_send_request(request)));

  printf("l1[0:-1]: ");
  db_reset_request(request, DB_LRANGE);
  db_add_arg_string(request, "l1");
  db_add_arg_int(request, 0);
  db_add_arg_int(request, -1);
  db_free_reply(db_print_reply(db_send_request(request)));

  printf("l1[3:4]: ");
  db_reset_request(request, DB_LRANGE);
  db_add_arg_string(request, "l1");
  db_add_arg_int(request, 3);
  db_add_arg_int(request, 4);
  db_free_reply(db_print_reply(db_send_request(request)));

  clock_t t0;
  db_uint_t sample_size = 100000;

  char key[16];
  char value[16];

  t0 = clock();
  for (db_uint_t i = 0; i < sample_size; i++)
  {
    sprintf(key, "test:k%d", i);
    sprintf(value, "test:v%d", i);
    db_reset_request(request, DB_SET);
    db_add_arg_string(request, key);
    db_add_arg_string(request, value);
    db_free_reply(db_send_request(request));
  }
  printf("wrote %lu items in %lums\n", sample_size, (clock() - t0) / (CLOCKS_PER_SEC / 1000));

  t0 = clock();
  for (db_uint_t i = 0; i < sample_size; i++)
  {
    sprintf(key, "test:k%d", i);
    db_reset_request(request, DB_GET);
    db_add_arg_string(request, key);
    db_free_reply(db_send_request(request));
  }
  printf("read %lu items in %lums\n", sample_size, (clock() - t0) / (CLOCKS_PER_SEC / 1000));

  t0 = clock();
  for (db_uint_t i = 0; i < sample_size; i++)
  {
    sprintf(key, "test:k%d", i);
    db_reset_request(request, DB_DEL);
    db_add_arg_string(request, key);
    db_free_reply(db_send_request(request));
  }
  printf("deleted %lu items in %lums\n", sample_size, (clock() - t0) / (CLOCKS_PER_SEC / 1000));

  db_free_request(request);

  sleep(1);

  running = false;

  thrd_join(db_monitor_worker, NULL);

  db_stop();

  printf("process finished with exit code 0\n");

  return 0;
}
