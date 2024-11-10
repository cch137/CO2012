#include <stdio.h>
#include <time.h>

#include "database.h"

int main()
{
  db_start(NULL);

  db_flushall();

  db_set("k1", "v1");
  db_set("k2", "v2");
  db_set("k3", "v3");
  db_rpush("l1", "a", "b", "c", "d", "e", "f", NULL);
  db_rpush("l2", "x", "y", "z", NULL);
  db_del("k2");
  db_free_DLNode(db_lpop("l2", 1));
  db_free_DLNode(db_rpop("l2", 1));

  printf("l1 len: %lu\n", db_llen("l1"));

  DLList *sublist1 = db_lrange("l1", 0, -1);
  printf("sublist1 content:\n");
  DLNode *node = sublist1->head;
  while (node)
    printf("%s%s ", node->data, (node = node->next) ? "," : "");
  printf("\n");
  db_free_DLList(sublist1);

  DLList *sublist2 = db_lrange("l1", 3, 4);
  printf("sublist2 content:\n");
  node = sublist2->head;
  while (node)
    printf("%s%s ", node->data, (node = node->next) ? "," : "");
  printf("\n");
  db_free_DLList(sublist2);

  db_stop();

  return 0;
}
