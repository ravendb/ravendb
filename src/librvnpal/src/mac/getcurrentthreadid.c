#ifdef __APPLE__

#include <pthread.h>
#include <rvn.h>

EXPORT uint64_t
rvn_get_current_thread_id()
{
  uint64_t id;
  pthread_threadid_np(NULL, &id);

  return id;
}

#endif
