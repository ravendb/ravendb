#if defined(__APPLE__)

#include <fcntl.h>

#include "rvn.h"
#include "status_codes.h"


int32_t
flush_file (int32_t fd) {

  return fcntl(fd, F_FULLFSYNC);

}

int32_t
sync_directory_allowed (int dir_fd) {
  return 1; // always allowed on Mac
}

#endif
