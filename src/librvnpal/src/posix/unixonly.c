#if defined(__unix__) && !defined(__APPLE__)

#define _GNU_SOURCE
#include <unistd.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <libgen.h>

#include "rvn.h"
#include "status_codes.h"

int32_t
_flush_file(int32_t fd)
{
  return fsync(fd);
}

int32_t
_sync_directory_allowed(int32_t dir_fd)
{
  struct statfs buf;
  if (fstatfs(dir_fd, &buf) == -1)
    return FAIL;

  switch (buf.f_type)
  {
  case NFS_SUPER_MAGIC:
  case CIFS_MAGIC_NUMBER:
  case SMB_SUPER_MAGIC:
    return SYNC_DIR_NOT_ALLOWED;
  default:
    return SYNC_DIR_ALLOWED;
  }
}

#endif
