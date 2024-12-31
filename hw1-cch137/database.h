#ifndef CCH137_DATABASE_H
#define CCH137_DATABASE_H

#include <stdbool.h>
#include "./cJSON.h"

#define DATABASE_FILENAME "database.json"

// Struct for storing key-value pairs in the database.
// Each DBItem contains a key (string) and a value (cJSON object), along with a pointer to the next item (for linked list chaining).
typedef struct DBItem
{
  char *key;
  cJSON *json;
  struct DBItem *next;
} DBItem;

// Check if an item with the given key exists
bool exists(const char *key);

// Retrieve an item by its key
DBItem *get_item(const char *key);

// Set an item with a given key and cJSON value
// If the key already exists, the old item is deleted and replaced with the new one
DBItem *set_item(const char *key, cJSON *json);

// Rename an item's key
DBItem *rename_item(const char *old_key, const char *new_key);

// Delete an item by its key
bool delete_item(const char *key);

// `DBModel_ArrayTypeSymbol` is used to represent array-type attributes (e.g., a phone number list).
#define DBModel_ArrayTypeSymbol NULL

// Enumeration for different types of values and attributes that can be used in the database models.
typedef enum DBModelType
{
  DBModelType_Object,
  DBModelType_Array,
  DBModelType_String,
  DBModelType_Number,
  DBModelType_Boolean,
  DBModelType_Null,
  DBModelAttr_ArrayTypeGetter,
  DBModelAttr_MaxLength,
  DBModelAttr_MinLength
} DBModelType;

// Struct for defining the schema of a database model.
typedef struct DBModel
{
  const char *key;
  DBModelType type;
  int intvalue;
  struct DBModel **attributes;
} DBModel;

// Define a model under a parent model
// If creating a root object, set the parent to NULL
DBModel *def_model(DBModel *parent, const char *key, DBModelType type);

// Add an attribute (like length constraints) to a model
DBModel *def_model_attr(DBModel *model, DBModelType attribute, int value);

// Get a specific attribute of a model
DBModel *get_model_attr(DBModel *model, DBModelType type);

// Struct representing a list of keys for iterating through database items or models.
typedef struct DBKeys
{
  int length;
  const char **keys;
} DBKeys;

// Get all keys in a model
DBKeys *get_model_keys(DBModel *model);

// Get all keys in a cJSON object
DBKeys *get_cjson_keys(cJSON *json);

// Get all keys in the database
DBKeys *get_database_keys();

// Free memory allocated for the keys
void free_keys(DBKeys *keys);

// Load the database from a JSON file
void load_database(const char *filename);

// Save the current state of the database to a JSON file
void save_database(const char *filename);

#endif
