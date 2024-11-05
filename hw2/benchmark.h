#ifndef DATABASE_BENCHMARK_H
#define DATABASE_BENCHMARK_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

#define REDIS_IP "localhost"
#define REDIS_PORT 6379

typedef struct PersonSample
{
  char *name;
  char *jobTitle;
  int age;
  char *address;
  char **phoneNumbers;
  int phoneNumberCount;
  char **emailAddresses;
  int emailAddressCount;
  bool isMarried;
  bool isEmployed;
} PersonSample;

typedef struct DBResourceUsage
{
  unsigned long write_time_used_ms;
  unsigned long read_time_used_ms;
  size_t memory_used;
} DBResourceUsage;

typedef struct DBBenchmarkResult
{
  int32_t sample_size;
  DBResourceUsage *hw1db;
  DBResourceUsage *redis;
} DBBenchmarkResult;

typedef struct DBTester
{
  PersonSample **samples;
  uint32_t sample_size;
  PersonSample *(*read_item)(const char *key);
  void (*write_item)(const char *key, const PersonSample *person);
  bool (*delete_item)(const char *key);
  size_t (*get_memory_usage)();
} DBTester;

// Helper functions

// Implement `strdup` in C, duplicates a string, returns a newly allocated copy
static char *_benchmark_strdup(const char *source);
// Starts the timer and returns the start time.
static clock_t start_timer();
// Ends the timer, calculates and returns the elapsed time in milliseconds.
static clock_t end_timer(clock_t start_at);

// Memory Usage functions

// Calculates the memory usage of a cJSON object
size_t get_cjson_memory_usage(const cJSON *item);
// Calculates the memory usage of the hash table database
size_t get_db_hash_table_memory_usage();
// Gets Redis memory usage information
size_t get_redis_memory_usage();

// PersonSample functions

// Generates a PersonSample object with sample data for testing
PersonSample *generate_person_sample(int i);
// Frees memory used by a PersonSample object
void free_person_sample(PersonSample *person);
// Converts a PersonSample to cJSON object
cJSON *person_to_cJSON(const PersonSample *person);
// Converts a cJSON object to a PersonSample
PersonSample *cJSON_to_person(const cJSON *person);

// Redis interaction functions

// Writes a PersonSample object to Redis database
void redis_write_person_sample(const char *key, const PersonSample *person);
// Reads a PersonSample object from Redis database
PersonSample *redis_read_person_sample(const char *key);
// Deletes a PersonSample from the Redis database
bool redis_delete_person_sample(const char *key);

// HW1DB interaction functions

// Writes a PersonSample to a custom database (HW1DB)
void hw1db_write_person_sample(const char *key, const PersonSample *person);
// Reads a PersonSample from a custom database (HW1DB)
PersonSample *hw1db_read_person_sample(const char *key);
// Deletes a PersonSample from a custom database (HW1DB)
bool hw1db_delete_person_sample(const char *key);

// Benchmark functions

// Creates a DBTester with a specified sample size
DBTester *create_tester(int32_t sample_size);
// Frees a DBTester and all associated PersonSample objects
void free_tester(DBTester *tester);
// Executes the benchmark for database operations
DBResourceUsage *exec_tester(DBTester *tester);
// Frees memory used by a DBBenchmarkResult object
DBBenchmarkResult *run_db_benchmark(int32_t sample_size);
// Runs a benchmark to compare Redis and HW1DB
void free_db_benchmark_result(DBBenchmarkResult *result);

#endif
