#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include <hiredis/hiredis.h>
#include "./database.h"
#include "./interface.h"
#include "./benchmark.h"

#define REDIS_ARR_SEPERATOR ","

redisContext *redis_context;

static clock_t start_timer()
{
  return clock();
}

static clock_t end_timer(clock_t start_at)
{
  return (clock() - start_at) * 1000 / CLOCKS_PER_SEC;
}

size_t get_cjson_memory_usage(cJSON *item)
{
  if (!item)
    return 0;

  size_t size = 0;

  while (item != NULL)
  {
    size += malloc_usable_size(item);
    if (cJSON_IsString(item) && item->valuestring != NULL)
    {
      size += malloc_usable_size(item->valuestring);
    }
    else if (cJSON_IsArray(item) || cJSON_IsObject(item))
    {
      size += get_cjson_memory_usage(item->child); // Recursively calculate size of child elements
    }

    if (item->string != NULL)
    {
      size += malloc_usable_size(item->string);
    }

    item = item->next; // Move to the next item in the chain
  }

  return size;
}

size_t get_db_hash_table_memory_usage()
{
  if (!db_hash_table)
    return 0;

  size_t size = malloc_usable_size(db_hash_table);
  DBItem *item;

  for (int i = 0; i < db_hash_table_size; ++i)
  {
    item = db_hash_table[i];
    while (item != NULL)
    {
      size += get_cjson_memory_usage(item->json);
      size += malloc_usable_size(item);
      size += malloc_usable_size(item->key);
      item = item->next;
    }
  }

  return size;
}

PersonSample *generate_person_sample(int i)
{
  static char tempStringBuffer[100]; // Temporary buffer for string formatting

  PersonSample *person = (PersonSample *)malloc(sizeof(PersonSample));

  if (!person)
    memory_error_handler(__FILE__, __LINE__, __func__);

  snprintf(tempStringBuffer, sizeof(tempStringBuffer), "test_person_%d", i);
  tempStringBuffer[sizeof(tempStringBuffer) - 1] = '\0';
  person->name = helper_strdup(tempStringBuffer);

  snprintf(tempStringBuffer, sizeof(tempStringBuffer), "job_%d", (i % 100));
  tempStringBuffer[sizeof(tempStringBuffer) - 1] = '\0';
  person->jobTitle = helper_strdup(tempStringBuffer);

  person->age = (i % 69);

  snprintf(tempStringBuffer, sizeof(tempStringBuffer), "test_person_%d_test_address", i);
  tempStringBuffer[sizeof(tempStringBuffer) - 1] = '\0';
  person->address = helper_strdup(tempStringBuffer);

  // Generate phone numbers array
  person->phoneNumberCount = 1 + ((i % 4 == 0) ? 1 : 0);
  person->phoneNumbers = (char **)malloc(person->phoneNumberCount * sizeof(char *));
  if (!person->phoneNumbers)
    memory_error_handler(__FILE__, __LINE__, __func__);
  for (int k = 0; k < person->phoneNumberCount; k++)
  {
    snprintf(tempStringBuffer, sizeof(tempStringBuffer), "test_person_%d_phone_%d", i, k);
    tempStringBuffer[sizeof(tempStringBuffer) - 1] = '\0';
    person->phoneNumbers[k] = helper_strdup(tempStringBuffer);
  }

  // Generate email addresses array
  person->emailAddressCount = 1 + ((i % 3 == 0) ? 1 : 0);
  person->emailAddresses = (char **)malloc(person->emailAddressCount * sizeof(char *));
  if (!person->emailAddresses)
    memory_error_handler(__FILE__, __LINE__, __func__);
  for (int k = 0; k < person->emailAddressCount; k++)
  {
    snprintf(tempStringBuffer, sizeof(tempStringBuffer), "test_person_%d_email_%d@example.com", i, k);
    tempStringBuffer[sizeof(tempStringBuffer) - 1] = '\0';
    person->emailAddresses[k] = helper_strdup(tempStringBuffer);
  }

  person->isMarried = (i % 4 == 0);
  person->isEmployed = !(i % 5 == 0);

  return person;
}

void free_person_sample(PersonSample *person)
{
  if (!person)
    return;
  free(person->name);
  free(person->jobTitle);
  free(person->address);
  for (int i = 0; i < person->phoneNumberCount; i++)
    free(person->phoneNumbers[i]);
  free(person->phoneNumbers);
  for (int i = 0; i < person->emailAddressCount; i++)
    free(person->emailAddresses[i]);
  free(person->emailAddresses);
  free(person);
}

cJSON *person_to_cJSON(const PersonSample *person)
{
  cJSON *cjsonPerson = cJSON_CreateObject();
  cJSON_AddStringToObject(cjsonPerson, "name", person->name);
  cJSON_AddStringToObject(cjsonPerson, "jobTitle", person->jobTitle);
  cJSON_AddNumberToObject(cjsonPerson, "age", person->age);
  cJSON_AddStringToObject(cjsonPerson, "address", person->address);

  // Add phone numbers to JSON
  cJSON *jsonPhoneNumbers = cJSON_CreateArray();
  for (int i = 0; i < person->phoneNumberCount; i++)
    cJSON_AddItemToArray(jsonPhoneNumbers, cJSON_CreateString(person->phoneNumbers[i]));
  cJSON_AddItemToObject(cjsonPerson, "phoneNumbers", jsonPhoneNumbers);

  // Add email addresses to JSON
  cJSON *jsonEmailAddresses = cJSON_CreateArray();
  for (int i = 0; i < person->emailAddressCount; i++)
    cJSON_AddItemToArray(jsonEmailAddresses, cJSON_CreateString(person->emailAddresses[i]));
  cJSON_AddItemToObject(cjsonPerson, "emailAddresses", jsonEmailAddresses);

  cJSON_AddBoolToObject(cjsonPerson, "isMarried", person->isMarried);
  cJSON_AddBoolToObject(cjsonPerson, "isEmployed", person->isEmployed);

  return cjsonPerson;
}

PersonSample *cJSON_to_person(const cJSON *person)
{
  PersonSample *newPerson = (PersonSample *)malloc(sizeof(PersonSample));

  if (!newPerson)
    memory_error_handler(__FILE__, __LINE__, __func__);

  newPerson->name = helper_strdup(cJSON_GetObjectItem(person, "name")->valuestring);
  newPerson->jobTitle = helper_strdup(cJSON_GetObjectItem(person, "jobTitle")->valuestring);
  newPerson->age = cJSON_GetObjectItem(person, "age")->valueint;
  newPerson->address = helper_strdup(cJSON_GetObjectItem(person, "address")->valuestring);

  // Extract phone numbers from JSON
  newPerson->phoneNumberCount = cJSON_GetArraySize(cJSON_GetObjectItem(person, "phoneNumbers"));
  newPerson->phoneNumbers = (char **)malloc(newPerson->phoneNumberCount * sizeof(char *));
  for (int i = 0; i < newPerson->phoneNumberCount; i++)
    newPerson->phoneNumbers[i] = helper_strdup(cJSON_GetArrayItem(cJSON_GetObjectItem(person, "phoneNumbers"), i)->valuestring);

  // Extract email addresses from JSON
  newPerson->emailAddressCount = cJSON_GetArraySize(cJSON_GetObjectItem(person, "emailAddresses"));
  newPerson->emailAddresses = (char **)malloc(newPerson->emailAddressCount * sizeof(char *));
  for (int i = 0; i < newPerson->emailAddressCount; i++)
    newPerson->emailAddresses[i] = helper_strdup(cJSON_GetArrayItem(cJSON_GetObjectItem(person, "emailAddresses"), i)->valuestring);

  newPerson->isMarried = cJSON_GetObjectItem(person, "isMarried")->valueint;
  newPerson->isEmployed = cJSON_GetObjectItem(person, "isEmployed")->valueint;

  return newPerson;
}

void redis_write_person_sample(const char *key, const PersonSample *person)
{
  size_t phoneNumbersLen = 0;
  for (int i = 0; i < person->phoneNumberCount; i++)
    phoneNumbersLen += strlen(person->phoneNumbers[i]) + 1;
  char *phoneNumbers = (char *)malloc(phoneNumbersLen * sizeof(char));
  phoneNumbers[0] = '\0';
  for (int i = 0; i < person->phoneNumberCount; i++)
  {
    strcat(phoneNumbers, person->phoneNumbers[i]);
    if (i < person->phoneNumberCount - 1)
      strcat(phoneNumbers, REDIS_ARR_SEPERATOR);
  }

  size_t emailAddressesLen = 0;
  for (int i = 0; i < person->emailAddressCount; i++)
    emailAddressesLen += strlen(person->emailAddresses[i]) + 1;
  char *emailAddresses = (char *)malloc(emailAddressesLen * sizeof(char));
  emailAddresses[0] = '\0';
  for (int i = 0; i < person->emailAddressCount; i++)
  {
    strcat(emailAddresses, person->emailAddresses[i]);
    if (i < person->emailAddressCount - 1)
      strcat(emailAddresses, REDIS_ARR_SEPERATOR);
  }

  redisReply *reply;
  reply = redisCommand(redis_context, "HSET %s name %s jobTitle %s age %d address %s phoneNumbers %s emailAddresses %s isMarried %d isEmployed %d",
                       key,
                       person->name,
                       person->jobTitle,
                       person->age,
                       person->address,
                       phoneNumbers,
                       emailAddresses,
                       person->isMarried,
                       person->isEmployed);

  if (!reply)
  {
    printf("Error: %s\n", redis_context->errstr);
    return;
  }

  freeReplyObject(reply);
  free(phoneNumbers);
  free(emailAddresses);
}

PersonSample *redis_read_person_sample(const char *key)
{
  redisReply *reply = redisCommand(redis_context, "HGETALL %s", key);

  if (!reply)
  {
    printf("Error: %s\n", redis_context->errstr);
    return NULL;
  }

  if (reply->type != REDIS_REPLY_ARRAY || reply->elements == 0)
  {
    freeReplyObject(reply);
    return NULL;
  }

  // Allocate memory for the PersonSample object
  PersonSample *person = (PersonSample *)calloc(1, sizeof(PersonSample));
  if (!person)
    memory_error_handler(__FILE__, __LINE__, __func__);

  // Parse Redis fields to populate PersonSample attributes
  for (size_t i = 0; i < reply->elements; i += 2)
  {
    char *field = reply->element[i]->str;
    char *value = reply->element[i + 1]->str;

    if (strcmp(field, "name") == 0)
    {
      person->name = helper_strdup(value);
    }
    else if (strcmp(field, "jobTitle") == 0)
    {
      person->jobTitle = helper_strdup(value);
    }
    else if (strcmp(field, "age") == 0)
    {
      person->age = atoi(value);
    }
    else if (strcmp(field, "address") == 0)
    {
      person->address = helper_strdup(value);
    }
    else if (strcmp(field, "phoneNumbers") == 0)
    {
      // Split phone numbers based on the separator
      char *token = strtok(value, REDIS_ARR_SEPERATOR);
      while (token)
      {
        person->phoneNumbers = (char **)realloc(person->phoneNumbers, sizeof(char *) * (person->phoneNumberCount + 1));
        if (!person->phoneNumbers)
          memory_error_handler(__FILE__, __LINE__, __func__);
        person->phoneNumbers[person->phoneNumberCount] = helper_strdup(token);
        person->phoneNumberCount++;
        token = strtok(NULL, REDIS_ARR_SEPERATOR);
      }
    }
    else if (strcmp(field, "emailAddresses") == 0)
    {
      // Split email addresses based on the separator
      char *token = strtok(value, REDIS_ARR_SEPERATOR);
      while (token)
      {
        person->emailAddresses = (char **)realloc(person->emailAddresses, sizeof(char *) * (person->emailAddressCount + 1));
        if (!person->emailAddresses)
          memory_error_handler(__FILE__, __LINE__, __func__);
        person->emailAddresses[person->emailAddressCount] = helper_strdup(token);
        person->emailAddressCount++;
        token = strtok(NULL, REDIS_ARR_SEPERATOR);
      }
    }
    else if (strcmp(field, "isMarried") == 0)
    {
      person->isMarried = atoi(value);
    }
    else if (strcmp(field, "isEmployed") == 0)
    {
      person->isEmployed = atoi(value);
    }
  }

  freeReplyObject(reply);

  return person;
}

bool redis_delete_person_sample(const char *key)
{
  redisReply *reply = redisCommand(redis_context, "DEL %s", key);
  if (!reply)
  {
    printf("Error: %s\n", redis_context->errstr);
    return false;
  }

  bool result = (reply->integer > 0);
  freeReplyObject(reply);
  return result;
}

void hw1db_write_person_sample(const char *key, const PersonSample *person)
{
  set_item(person->name, person_to_cJSON(person));
}

PersonSample *hw1db_read_person_sample(const char *key)
{
  return cJSON_to_person(get_item(key)->json);
}

bool hw1db_delete_person_sample(const char *key)
{
  return delete_item(key);
}

size_t get_redis_memory_usage()
{
  redisReply *reply = redisCommand(redis_context, "INFO memory");

  if (!reply)
  {
    printf("Error: %s\n", redis_context->errstr);
    return 0;
  }

  if (reply->type == REDIS_REPLY_NIL)
  {
    printf("Error: No memory usage information available\n");
    freeReplyObject(reply);
    return 0;
  }

  long memory_usage = 0;
  if (reply->type == REDIS_REPLY_STRING)
  {
    char *memory_line = strstr(reply->str, "used_memory_dataset:");
    if (memory_line)
      sscanf(memory_line, "used_memory_dataset:%ld", &memory_usage);
  }

  freeReplyObject(reply);

  return memory_usage;
}

DBTester *create_tester(int32_t sample_size)
{
  DBTester *tester = (DBTester *)malloc(sizeof(DBTester));
  if (!tester)
    memory_error_handler(__FILE__, __LINE__, __func__);

  tester->sample_size = sample_size;

  // Allocate memory for the array of PersonSample pointers
  tester->samples = (PersonSample **)malloc(sample_size * sizeof(PersonSample *));

  if (!tester->samples)
    memory_error_handler(__FILE__, __LINE__, __func__);

  for (int i = 1; i <= sample_size; i++)
    tester->samples[i - 1] = generate_person_sample(i);

  return tester;
}

void free_tester(DBTester *tester)
{
  if (!tester)
    return;

  PersonSample **samples = tester->samples;
  uint32_t sample_size = tester->sample_size;

  if (!samples)
    return free(tester);

  for (int i = 0; i < sample_size; i++)
    free_person_sample(samples[i]);

  free(tester->samples);
  free(tester);
}

DBResourceUsage *exec_tester(DBTester *tester)
{
  DBResourceUsage *usage = (DBResourceUsage *)malloc(sizeof(DBResourceUsage));
  PersonSample **read_results = (PersonSample **)malloc(tester->sample_size * sizeof(PersonSample *));

  if (!usage || !read_results)
    memory_error_handler(__FILE__, __LINE__, __func__);

  PersonSample **samples = tester->samples;
  uint32_t sample_size = tester->sample_size;
  PersonSample *(*read_item)(const char *key) = tester->read_item;
  void (*write_item)(const char *key, const PersonSample *person) = tester->write_item;
  bool (*delete_item)(const char *key) = tester->delete_item;

  // Measure write time
  clock_t timer = start_timer();
  for (unsigned int i = 0; i < sample_size; i++)
    write_item(samples[i]->name, samples[i]);
  usage->write_time_used_ms = end_timer(timer);

  // Measure read time
  timer = start_timer();
  for (unsigned int i = 0; i < sample_size; i++)
    read_results[i] = read_item(samples[i]->name);
  usage->read_time_used_ms = end_timer(timer);

  // Measure memory usage
  usage->memory_used = tester->get_memory_usage();

  // Cleanup after the benchmark
  for (unsigned int i = 0; i < sample_size; i++)
  {
    if (read_results[i])
      free_person_sample(read_results[i]);
    delete_item(samples[i]->name);
  }

  free(read_results);

  return usage;
}

void free_db_benchmark_result(DBBenchmarkResult *result)
{
  if (!result)
    return;

  if (result->hw1db)
    free(result->hw1db);
  if (result->redis)
    free(result->redis);

  free(result);
}

DBBenchmarkResult *run_db_benchmark(int32_t sample_size)
{
  if (!redis_context)
    redis_context = redisConnect(REDIS_IP, REDIS_PORT);

  DBBenchmarkResult *result = (DBBenchmarkResult *)malloc(sizeof(DBBenchmarkResult));

  if (!result)
    memory_error_handler(__FILE__, __LINE__, __func__);

  result->sample_size = sample_size;

  DBTester *tester = create_tester(sample_size);

  // Initialize HW1 database
  size_t hash_table_size = (int)(sample_size / 0.7 + 0.5);
  load_database(DATABASE_FILENAME, hash_table_size);

  // Test HW1 database
  tester->read_item = hw1db_read_person_sample;
  tester->write_item = hw1db_write_person_sample;
  tester->delete_item = hw1db_delete_person_sample;
  tester->get_memory_usage = get_db_hash_table_memory_usage;
  result->hw1db = exec_tester(tester);

  // Clear Redis database
  redisCommand(redis_context, "FLUSHALL");

  // Test Redis
  tester->read_item = redis_read_person_sample;
  tester->write_item = redis_write_person_sample;
  tester->delete_item = redis_delete_person_sample;
  tester->get_memory_usage = get_redis_memory_usage;
  result->redis = exec_tester(tester);

  // Clear Redis database
  redisCommand(redis_context, "FLUSHALL");

  free_tester(tester);

  return result;
}

void print_table_row(const char *dbname, long sample_size, DBResourceUsage *usage)
{
  printf("%6s %12ld %16ld %16ld %16ld \n", dbname, sample_size, usage->write_time_used_ms, usage->read_time_used_ms, usage->memory_used);
}

int benchmark_main()
{
  DBBenchmarkResult *result;

  printf("%16s %16s %16s %16s %16s\n", "db", "sample_size", "write_tu_ms", "read_tu_ms", "mem_used_byte");

  for (int j = 0; j < 5; j++)
  {
    for (int i = 1; i <= 20; i++)
    {
      result = run_db_benchmark(i * 10000);
      printf("%16s %16ld %16ld %16ld %16ld \n", "hw1db", result->sample_size, result->hw1db->write_time_used_ms, result->hw1db->read_time_used_ms, result->hw1db->memory_used);
      printf("%16s %16ld %16ld %16ld %16ld \n", "redis", result->sample_size, result->redis->write_time_used_ms, result->redis->read_time_used_ms, result->redis->memory_used);
      free_db_benchmark_result(result);
    }
  }

  return 0;
}
