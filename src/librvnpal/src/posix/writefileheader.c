#if defined(__unix__) || defined(__APPLE__)

#define _GNU_SOURCE
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

#include "rvn.h"
#include "status_codes.h"


int32_t
rvn_write_header (const char *path, void *header, int32_t size,
		  uint32_t * detailed_error_code) {
  int32_t rc;
  bool syncIsNeeded = false;
  int32_t fd = open (path, O_WRONLY | O_CREAT, S_IWUSR | S_IRUSR);

  if (fd == -1) {
    rc = FAIL_OPEN_FILE;
    goto error_cleanup;
  }

  int32_t remaining = size;

  int64_t sz = lseek (fd, 0L, SEEK_END);
  if (sz == -1) {
    rc = FAIL_SEEK_FILE;
    goto error_cleanup;
  }

  if (lseek (fd, 0L, SEEK_SET) == -1) {
    rc = FAIL_SEEK_FILE;
    goto error_cleanup;
  }

  if (sz != remaining)
    syncIsNeeded = true;

  while (remaining > 0) {
      uint64_t written = write (fd, header, (uint64_t) remaining);
      if (written == -1) {
    	  rc = FAIL_WRITE_FILE;
    	  goto error_cleanup;
    	}

      remaining -= (int) written;
      header += written;
  }
  if (flush_file (fd) == -1) {
    rc = FAIL_FLUSH_FILE;
    goto error_cleanup;
  }

  close (fd);
  fd = -1;

  if (syncIsNeeded == true)
    return sync_directory_for (path, detailed_error_code);
  return SUCCESS;

error_cleanup:

  *detailed_error_code = errno;
  if (fd != -1)
    close (fd);

  return rc;
}


#endif
