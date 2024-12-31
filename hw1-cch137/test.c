#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include "./cJSON.h"
#include "./database.h"

#define PASS "\033[0;32mPASS\033[0m"
#define FAIL "\033[0;31mFAIL\033[0m"

bool test_get_item(const char *key, const char *expected_name)
{
  DBItem *item = get_item(key);

  if (item == NULL)
  {
    if (expected_name == NULL)
    {
      printf("get_item(%s) " PASS "\n", key);
      return true;
    }
    printf("get_item(%s) " FAIL " - returned null\n", key);
    return false;
  }

  cJSON *name_json = cJSON_GetObjectItem(item->json, "name");
  if (!cJSON_IsString(name_json) || strcmp(name_json->valuestring, expected_name) != 0)
  {
    printf("get_item(%s) " FAIL " - name mismatch\n", key);
    return false;
  }
  printf("get_item(%s) " PASS "\n", key);
  return true;
}

bool test_set_item(const char *key, cJSON *json)
{
  set_item(key, json);
  DBItem *item = get_item(key);

  if (key == NULL)
  {
    if (item == NULL)
    {
      printf("set_item(%s, %p) " PASS "\n", key, json);
      return true;
    }
    printf("set_item(%s, %p) " FAIL " - item found\n", key, json);
    return false;
  }

  if (item == NULL)
  {
    if (json == NULL)
    {
      printf("set_item(%s, %p) " PASS "\n", key, json);
      return true;
    }
    printf("set_item(%s, %p) " FAIL " - item not found\n", key, json);
    return false;
  }

  if (item->json != json)
  {
    printf("set_item(%s, %p) " FAIL " - json pointer mismatch\n", key, json);
    return false;
  }

  cJSON *name_json = cJSON_GetObjectItem(item->json, "name");
  const char *item_name = name_json->valuestring;
  if (!cJSON_IsString(name_json) || strcmp(item_name, key) != 0)
  {
    printf("set_item(%s, %p) " FAIL " - name mismatch(%s)\n", key, json, item_name);
    return false;
  }

  printf("set_item(%s, %p) " PASS "\n", key, json);
  return true;
}

bool test_rename_item(const char *old_key, const char *new_key)
{
  bool has_item = exists(old_key);
  DBItem *before_item = get_item(old_key);
  DBItem *result = rename_item(old_key, new_key);
  DBItem *after_item = get_item(new_key);

  if (old_key == NULL || new_key == NULL || !has_item)
  {
    if (result == NULL)
    {
      printf("rename_item(%s, %s) " PASS "\n", old_key, new_key);
      return true;
    }
    printf("rename_item(%s, %s) " FAIL " - returned non-null\n", old_key, new_key);
    return false;
  }

  if (exists(new_key))
  {
    if (result == NULL)
    {
      printf("rename_item(%s, %s) " PASS "\n", old_key, new_key);
      return true;
    }
  }

  if (result != after_item || before_item != after_item)
  {
    printf("rename_item(%s, %s) " FAIL " - returned wrong item\n", old_key, new_key);
    return false;
  }

  printf("rename_item(%s, %s) " PASS "\n", old_key, new_key);
  return true;
}

bool test_delete_item(const char *key, bool expected_value)
{
  bool result = delete_item(key);

  if (result != expected_value)
  {
    printf("delete_item(%s) " FAIL "\n", key);
    return false;
  }

  DBItem *item = get_item(key);
  if (item)
  {
    printf("delete_item(%s) " FAIL " - item still exists\n", key);
    return false;
  }
  printf("delete_item(%s) " PASS "\n", key);
  return true;
}

bool test_get_database_keys(int expected_count)
{
  DBKeys *keys = get_database_keys();
  if (keys == NULL)
  {
    printf("get_database_keys() " FAIL " - returned null\n");
    free_keys(keys);
    return false;
  }
  else if (keys->length != expected_count)
  {
    printf("get_database_keys() " FAIL " - expected %d keys, got %d\n", expected_count, keys->length);
    free_keys(keys);
    return false;
  }
  else
  {
    printf("get_database_keys() " PASS "\n");
    free_keys(keys);
    return true;
  }
}

bool test_get_cjson_keys(cJSON *json, int expected_count)
{
  DBKeys *keys = get_cjson_keys(json);
  if (keys == NULL)
  {
    if (json == NULL)
    {
      printf("get_cjson_keys() " PASS " - null input\n");
      free_keys(keys);
      return true;
    }
    printf("get_cjson_keys() " FAIL " - returned null\n");
    free_keys(keys);
    return false;
  }
  else if (keys->length != expected_count)
  {
    printf("get_cjson_keys() " FAIL " - expected %d keys, got %d\n", expected_count, keys->length);
    free_keys(keys);
    return false;
  }
  else
  {
    printf("get_cjson_keys() " PASS "\n");
    free_keys(keys);
    return true;
  }
}

int main()
{
  // Load the database twice to test the cleaning functionality
  load_database("test-before.json");
  load_database("test-before.json");

  int test_stats[2] = {0, 0}; // Index 0: FAIL, Index 1: PASS

  test_stats[test_get_item("Alice", "Alice")]++;
  test_stats[test_get_item("Unknown", NULL)]++;
  test_stats[test_get_item(NULL, NULL)]++;

  cJSON *new_person1 = cJSON_CreateObject();
  cJSON_AddStringToObject(new_person1, "name", "Person1");
  cJSON_AddStringToObject(new_person1, "jobTitle", "Engineer");
  cJSON *new_person2 = cJSON_CreateObject();
  cJSON_AddStringToObject(new_person2, "name", "Person2");
  cJSON_AddStringToObject(new_person2, "jobTitle", "Manager");
  test_stats[test_set_item("Person1", new_person1)]++;
  test_stats[test_set_item(NULL, new_person2)]++;
  test_stats[test_set_item(NULL, NULL)]++;

  test_stats[test_rename_item("Alice", "Alex")]++;
  test_stats[test_rename_item("Bob", "Bob")]++;
  test_stats[test_rename_item("NotInDBName1", "NotInDBName2")]++;
  test_stats[test_rename_item("Bob", NULL)]++;
  test_stats[test_rename_item(NULL, "Bob")]++;
  test_stats[test_rename_item(NULL, NULL)]++;

  test_stats[test_delete_item("Alex", true)]++;
  test_stats[test_delete_item("Unknown", false)]++;
  test_stats[test_get_database_keys(26)]++;
  test_stats[test_get_cjson_keys(new_person1, 2)]++;

  save_database("test-after.json");

  printf("\ntotal " PASS ": %d\ntotal " FAIL ": %d\n", test_stats[1], test_stats[0]);

  return 0;
}
