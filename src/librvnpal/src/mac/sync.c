#if defined(__APPLE__)

#include <fcntl.h>

#include "rvn.h"
#include "status_codes.h"

PRIVATE int32_t
_flush_file(int32_t fd)
{
  return fcntl(fd, F_FULLFSYNC);
}

PRIVATE int32_t
_sync_directory_allowed(int32_t dir_fd)
{
  return 1; /* always allowed on Mac */
}

#endif
