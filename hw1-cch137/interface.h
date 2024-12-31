#ifndef CCH137_INTERFACE_H
#define CCH137_INTERFACE_H

#include "./cJSON.h"
#include "./database.h"

// Error handler for memory allocation issues.
// It prints the file name, line number, and function name where the error occurred.
void memory_error_handler(const char *filename, int line, const char *funcname);

// Print the details of a person (DBItem).
void print_person(DBItem *item);

// Create a new person using the given model
void create_person(DBModel *person_model);

// Find a person in the database
void find_person();

// Update a person's information using the given model
void update_person(DBModel *person_model);

// Delete a person from the database
void delete_person();

// Capture user input and create a cJSON object based on a model
cJSON *input_cjson_with_model(DBModel *model, int tab_depth);

// Edit an existing cJSON object based on a model
bool edit_cjson_with_model(DBModel *model, cJSON *json, int tab_depth);

// Display the main menu and handle user input
void main_menu();

#endif
