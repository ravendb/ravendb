#if defined(__unix__) && !defined(APPLE)

#define _GNU_SOURCE
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <rvn.h>

EXPORT uint64_t rvn_get_current_thread_id(void) {
  return syscall(SYS_gettid);
}

#endif
