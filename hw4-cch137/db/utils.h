#ifndef DB_UTILS_H
#define DB_UTILS_H

#include <stdbool.h>

#include "types.h"

// Handles memory allocation errors by printing an error message and exiting the program.
#define EXIT_ON_MEMORY_ERROR() _exit_on_memory_error(__FILE__, __LINE__, __func__)

#define EXIT_ON_ERROR(message) _exit_on_error(message, __FILE__, __LINE__, __func__)

void to_uppercase(char *str);

char *input_string();

// Duplicates a string, allocating memory for the new string.
char *dbutil_strdup(const char *source);

db_bool_t dbutil_match_keys(const char *source, const char *pattern);

void debug_print(const char *s);

// Don't use this function, please use the marco "EXIT_ON_ERROR()".
void _exit_on_error(const char *message, const char *filename, int line, const char *funcname);

// Don't use this function, please use the marco "EXIT_ON_MEMORY_ERROR()".
void _exit_on_memory_error(const char *filename, int line, const char *funcname);

#endif
