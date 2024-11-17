#include <stdio.h>
#include <unistd.h>

#include "database.h"
#include "utils.h"

void reset_db_to_test_data()
{
  db_free_reply(db_command("FLUSHALL"));
  db_free_reply(db_command("SET author cch"));
  db_free_reply(db_command("SET author cch137"));
  db_free_reply(db_command("SET hw 3"));
  db_free_reply(db_command("SET foo bar"));
  db_free_reply(db_command("DEL foo"));
  db_free_reply(db_command("RPUSH list1 a b c d e f g"));
  db_free_reply(db_command("LPUSH list2 x y z"));
  db_free_reply(db_command("RPOP list1 2"));
  db_free_reply(db_command("LPOP list2 1"));
  db_free_reply(db_command("SAVE"));
}

int main()
{
  char *command_buffer = NULL;

  db_start();

  reset_db_to_test_data();

  while (db_is_running())
  {
    printf("> ");
    command_buffer = input_string();
    if (!command_buffer)
      continue;
    db_free_reply(db_print_reply(db_command(command_buffer)));
    free(command_buffer);
  }

  printf("process finished with exit code 0\n");

  return 0;
}
