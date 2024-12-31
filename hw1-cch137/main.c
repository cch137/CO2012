#include "./database.h"
#include "./interface.h"

int main()
{
  load_database(DATABASE_FILENAME);
  main_menu();
  save_database(DATABASE_FILENAME);

  return 0;
}
