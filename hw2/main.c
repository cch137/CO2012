#include <stdio.h>
#include <stdlib.h>
#include "./interface.h"
#include "./benchmark.h"

void print_table_row(const char *dbname, long sample_size, DBResourceUsage *usage)
{
  printf("%6s %12ld %16ld %16ld %16ld \n", dbname, sample_size, usage->write_time_used_ms, usage->read_time_used_ms, usage->memory_used);
}

int main()
{
  DBBenchmarkResult *result;

  printf("%6s %12s %16s %16s %16s\n", "db", "sample_size", "Write TU (ms)", "Read TU (ms)", "mem. used (b)");

  for (int j = 0; j < 5; j++)
  {
    for (int i = 1; i <= 20; i++)
    {
      result = run_db_benchmark(i * 10000);
      printf("%6s %12ld %16ld %16ld %16ld \n", "hw1db", result->sample_size, result->hw1db->write_time_used_ms, result->hw1db->read_time_used_ms, result->hw1db->memory_used);
      printf("%6s %12ld %16ld %16ld %16ld \n", "redis", result->sample_size, result->redis->write_time_used_ms, result->redis->read_time_used_ms, result->redis->memory_used);
      free_db_benchmark_result(result);
    }
  }

  return 0;
}
