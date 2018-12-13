#ifdef APPLE

#include <pthread.h>
#include <rvn.h>

unsigned long rvn_get_current_thread_id()
{
  return pthread_self();
}

#endif