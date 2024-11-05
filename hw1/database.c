#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include "./cJSON.h"
#include "./database.h"
#include "./interface.h"

#define HASH_MOD 5831
#define HASH_SHIFT_BITS 5
#define HASH_TABLE_SIZE 137

DBItem **hash_table = NULL;

pthread_mutex_t _db_mutex = PTHREAD_MUTEX_INITIALIZER;
// The mutex is locked while the database is being read and written.
// We will not destroy the mutex because it has a continuing purpose in the program.
pthread_mutex_t *db_mutex = &_db_mutex;

unsigned long static hash(const char *string);
DBItem static *create_item_with_json(const char *key, cJSON *json);
DBItem static *add_item_to_hash_table(const char *key, DBItem *item);
DBItem static *remove_item_from_hash_table(const char *key);
DBItem static *set_item_key(DBItem *item, const char *key);

// DJB2 hash
unsigned long static hash(const char *string)
{
  if (string == NULL)
    return 0;

  unsigned long hash_value = HASH_MOD;
  int current_char;
  while ((current_char = *string++))
  {
    hash_value = ((hash_value << HASH_SHIFT_BITS) + hash_value) + current_char;
  }
  return hash_value % HASH_TABLE_SIZE;
}

DBItem static *create_item_with_json(const char *key, cJSON *json)
{
  if (json == NULL)
    return NULL;

  DBItem *item = (DBItem *)malloc(sizeof(DBItem));

  if (!item)
    memory_error_handler(__FILE__, __LINE__, __func__);

  item->key = NULL;
  item->json = json;
  item->next = NULL;
  set_item_key(item, key);

  return item;
}

DBItem static *add_item_to_hash_table(const char *key, DBItem *item)
{
  if (item == NULL)
    return NULL;

  unsigned long index = hash(key);
  item->next = hash_table[index];
  hash_table[index] = item;

  return item;
}

DBItem static *remove_item_from_hash_table(const char *key)
{
  if (key == NULL)
    return NULL;

  unsigned long index = hash(key);
  DBItem *prev = NULL;
  DBItem *curr = hash_table[index];

  while (curr != NULL)
  {
    if (strcmp(curr->key, key) == 0)
    {
      if (prev == NULL)
        hash_table[index] = curr->next;
      else
        prev->next = curr->next;

      return curr;
    }
    prev = curr;
    curr = curr->next;
  }

  return NULL;
}

DBItem static *set_item_key(DBItem *item, const char *key)
{
  if (item == NULL || key == NULL)
    return NULL;

  size_t key_length = (strlen(key) + 1) * sizeof(char);
  item->key = (char *)realloc(item->key, key_length);

  if (!item->key)
    memory_error_handler(__FILE__, __LINE__, __func__);

  memset(item->key, 0, key_length);
  strcpy(item->key, key);

  return item;
}

bool exists(const char *key)
{
  return (key != NULL && get_item(key) != NULL);
}

DBItem *get_item(const char *key)
{
  if (key == NULL)
    return NULL;

  unsigned long index = hash(key);
  pthread_mutex_lock(db_mutex);
  DBItem *item = hash_table[index];

  while (item != NULL)
  {
    if (strcmp(item->key, key) == 0)
    {
      pthread_mutex_unlock(db_mutex);
      return item;
    }
    item = item->next;
  }

  pthread_mutex_unlock(db_mutex);
  return NULL;
}

DBItem *set_item(const char *key, cJSON *json)
{
  if (key == NULL || json == NULL)
    return NULL;

  DBItem *oldItem = get_item(key);

  if (oldItem != NULL)
  {
    if (oldItem->json == json)
    {
      return oldItem;
    }
    delete_item(key);
  }

  DBItem *item = create_item_with_json(key, json);
  pthread_mutex_lock(db_mutex);
  add_item_to_hash_table(key, item);

  pthread_mutex_unlock(db_mutex);
  return item;
}

DBItem *rename_item(const char *old_key, const char *new_key)
{
  if (old_key == NULL || new_key == NULL || !exists(old_key) || exists(new_key))
    return NULL;

  pthread_mutex_lock(db_mutex);
  // remove item with old key
  DBItem *item = remove_item_from_hash_table(old_key);

  // add item with new key
  add_item_to_hash_table(new_key, item);
  pthread_mutex_unlock(db_mutex);

  // rename item
  set_item_key(item, new_key);

  return item;
}

// Return true if success, false if fail.
bool delete_item(const char *key)
{
  pthread_mutex_lock(db_mutex);
  DBItem *item = remove_item_from_hash_table(key);
  pthread_mutex_unlock(db_mutex);

  if (item == NULL)
    return false;

  cJSON_Delete(item->json);
  free(item);

  return true;
}

// Returns the attribute Model.
DBModel *def_model(DBModel *parent, const char *key, DBModelType type)
{
  DBModel *model = (DBModel *)malloc(sizeof(DBModel));

  if (!model)
    memory_error_handler(__FILE__, __LINE__, __func__);

  model->key = key;
  model->type = type;
  model->intvalue = 0;
  model->attributes = NULL;

  if (parent == NULL)
    return model;

  parent->attributes = (DBModel **)realloc(parent->attributes, (parent->intvalue + 1) * sizeof(DBModel *));

  if (!parent->attributes)
    memory_error_handler(__FILE__, __LINE__, __func__);

  parent->attributes[parent->intvalue] = model;
  parent->intvalue++;

  return model;
}

// Returns the Model with the property set.
DBModel *def_model_attr(DBModel *model, DBModelType attr, int value)
{
  DBModel *attribute = def_model(model, NULL, attr);
  attribute->intvalue = value;

  return model;
}

DBModel *get_model_attr(DBModel *model, DBModelType type)
{
  if (model == NULL)
    return NULL;

  int attributes_length = model->intvalue;

  if (type == DBModelAttr_ArrayTypeGetter)
  {
    for (int i = 0; i < attributes_length; i++)
    {
      if (model->attributes[i]->key == DBModel_ArrayTypeSymbol)
        return model->attributes[i];
    }
  }
  else
  {
    for (int i = 0; i < attributes_length; i++)
    {
      if (model->attributes[i]->type == type)
        return model->attributes[i];
    }
  }

  return NULL;
}

DBKeys *get_model_keys(DBModel *model)
{
  DBKeys *keys = (DBKeys *)malloc(sizeof(DBKeys));

  if (!keys)
    memory_error_handler(__FILE__, __LINE__, __func__);

  keys->length = 0;
  keys->keys = NULL;

  if (model->type != DBModelType_Object)
    return keys;

  int length = model->intvalue;

  keys->keys = (const char **)malloc(length * sizeof(const char *));

  if (!keys->keys)
    memory_error_handler(__FILE__, __LINE__, __func__);

  keys->length = length;

  for (int i = 0; i < length; i++)
  {
    keys->keys[i] = model->attributes[i]->key;
  }

  return keys;
}

#define GET_KEYS_CHUNK_SIZE 8

DBKeys *get_cjson_keys(cJSON *json)
{
  DBKeys *keys = (DBKeys *)malloc(sizeof(DBKeys));

  if (!keys)
    memory_error_handler(__FILE__, __LINE__, __func__);

  keys->length = 0;
  keys->keys = NULL;
  int count = 0;

  cJSON *cursor = json->child;
  while (cursor != NULL)
  {
    count++;
    if (keys->length < count)
    {
      keys->length += GET_KEYS_CHUNK_SIZE;
      keys->keys = (const char **)realloc(keys->keys, keys->length * sizeof(const char *));

      if (!keys->keys)
        memory_error_handler(__FILE__, __LINE__, __func__);
    }
    keys->keys[count - 1] = cursor->string;
    cursor = cursor->next;
  }

  if (keys->length != count)
  {
    keys->length = count;
    keys->keys = (const char **)realloc(keys->keys, count * sizeof(const char *));
    if (!keys->keys)
      memory_error_handler(__FILE__, __LINE__, __func__);
  }

  return keys;
}

DBKeys *get_database_keys()
{
  DBKeys *keys = (DBKeys *)malloc(sizeof(DBKeys));

  if (!keys)
    memory_error_handler(__FILE__, __LINE__, __func__);

  keys->length = 0;
  keys->keys = NULL;
  int count = 0;
  DBItem *cursor = NULL;

  for (int i = 0; i < HASH_TABLE_SIZE; i++)
  {
    cursor = hash_table[i];
    while (cursor != NULL)
    {
      count++;
      if (keys->length < count)
      {
        keys->length += GET_KEYS_CHUNK_SIZE;
        keys->keys = (const char **)realloc(keys->keys, keys->length * sizeof(const char *));
        if (!keys->keys)
          memory_error_handler(__FILE__, __LINE__, __func__);
      }
      keys->keys[count - 1] = cursor->key;
      cursor = cursor->next;
    }
  }

  if (keys->length != count)
  {
    keys->length = count;
    keys->keys = (const char **)realloc(keys->keys, count * sizeof(const char *));
    if (!keys->keys)
      memory_error_handler(__FILE__, __LINE__, __func__);
  }

  return keys;
}

void free_keys(DBKeys *keys)
{
  if (keys == NULL)
    return;

  free(keys->keys);
  free(keys);
}

void load_database(const char *filename)
{
  // read the JSON file
  FILE *file = fopen(filename, "r");
  char *db_json_string = NULL;
  long length = 0;

  if (file == NULL)
  {
    printf("Warning: Failed to open file %s\n", filename);
  }
  else
  {
    fseek(file, 0, SEEK_END);
    length = ftell(file);
    fseek(file, 0, SEEK_SET);
    db_json_string = (char *)calloc((length + 1), sizeof(char));
    if (!db_json_string)
      memory_error_handler(__FILE__, __LINE__, __func__);
    fread(db_json_string, 1, length, file);
    fclose(file);
    // prevent memory leak
    db_json_string[length] = '\0';
  }

  // clear table if table is not NULL
  if (hash_table != NULL)
  {
    DBItem *item = NULL;
    DBItem *next = NULL;
    for (int i = 0; i < HASH_TABLE_SIZE; i++)
    {
      item = hash_table[i];
      while (item != NULL)
      {
        next = item->next;
        free(item->key);
        free(item);
        item = next;
      }
    }
    free(hash_table);
    hash_table = NULL;
  }

  // create hash table
  hash_table = (DBItem **)calloc(HASH_TABLE_SIZE, sizeof(DBItem *));

  if (!hash_table)
    memory_error_handler(__FILE__, __LINE__, __func__);

  // create json root
  cJSON *json_root = NULL;
  if (db_json_string)
  {
    json_root = cJSON_Parse(db_json_string);
    free(db_json_string);
  }
  if (json_root == NULL)
    json_root = cJSON_CreateObject();

  // load items
  cJSON *json_cursor = json_root->child;
  DBItem *item = NULL;

  pthread_mutex_lock(db_mutex);
  while (json_cursor != NULL)
  {
    item = create_item_with_json(json_cursor->string, cJSON_Duplicate(json_cursor, true));
    add_item_to_hash_table(json_cursor->string, item);
    json_cursor = json_cursor->next;
  }
  pthread_mutex_unlock(db_mutex);

  cJSON_Delete(json_root);
}

void save_database(const char *filename)
{
  FILE *file = fopen(filename, "w");
  if (file == NULL)
    return;

  cJSON *json_root = cJSON_CreateObject();

  pthread_mutex_lock(db_mutex);

  // iter hash table and get items, then set to json root
  DBItem *item = NULL;
  for (int i = 0; i < HASH_TABLE_SIZE; i++)
  {
    item = hash_table[i];
    while (item != NULL)
    {
      cJSON_AddItemReferenceToObject(json_root, item->key, item->json);
      item = item->next;
    }
  }
  pthread_mutex_unlock(db_mutex);

  char *data = cJSON_Print(json_root);
  cJSON_Delete(json_root);
  if (data)
  {
    fprintf(file, "%s", data);
    free(data);
  }
  fclose(file);
}
