#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "utils.h"

void memory_error_handler(const char *filename, int line, const char *funcname)
{
  printf("Error: Memory allocation failed in '%s' function\n", funcname);
  printf("    at %s:%d\n", filename, line);
  exit(1);
}

char *helper_strdup(const char *source)
{
  if (!source)
    return NULL;
  char *dup = (char *)malloc((strlen(source) + 1) * sizeof(char));
  if (!dup)
    memory_error_handler(__FILE__, __LINE__, __func__);
  strcpy(dup, source);
  return dup;
}
