#include <stdint.h>
#include <Windows.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

uint64_t rvn_get_current_thread_id(void) {
  return (uint64_t)GetCurrentThreadId();
}

