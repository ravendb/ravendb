#include <rvn.h>
#include <stdint.h>
#include <Windows.h>

uint64_t rvn_get_current_thread_id(void)
{
  return (uint64_t)GetCurrentThreadId();
}

