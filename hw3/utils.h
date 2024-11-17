#ifndef CCH137DB_UTILS_H
#define CCH137DB_UTILS_H

void to_uppercase(char *str);

char *input_string();

// Handles memory allocation errors by printing an error message and exiting the program.
void memory_error_handler(const char *filename, int line, const char *funcname);

// Duplicates a string, allocating memory for the new string.
char *helper_strdup(const char *source);

#endif
