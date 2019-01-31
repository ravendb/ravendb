#if !defined(__unix__) || defined(__APPLE__)

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/param.h>
#include <sys/mount.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>
#include "rvn.h"
#include "internal_posix.h"
#include "status_codes.h"


EXPORT uint64_t
rvn_get_current_thread_id()
{
    uint64_t id;
    pthread_threadid_np(NULL, &id);

    return id;
}

PRIVATE int32_t
_flush_file(int32_t fd)
{
    return fcntl(fd, F_FULLFSYNC);
}

PRIVATE int32_t
_sync_directory_allowed(int32_t dir_fd)
{
    return 1;
}

PRIVATE int32_t
_finish_open_file_with_odirect(int32_t fd)
{
    /* mac doesn't support O_DIRECT, we fcntl instead: */
    return fcntl(fd, F_NOCACHE, 1);
}

PRIVATE int32_t
_rvn_fallocate(int32_t fd, int64_t offset, int64_t size)
{
    /* mac doesn't support fallocate */
    return EINVAL;
}

PRIVATE char*
_get_strerror_r(int32_t error, char* tmp_buff, int32_t buf_size)
{
  int32_t non_gnu_compliant_rc = strerror_r(error, tmp_buff, buf_size);
  if (non_gnu_compliant_rc != 0)
	  return tmp_buff;
  return NULL;
}

#endif
