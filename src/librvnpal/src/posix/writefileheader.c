#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

EXPORT int32_t
rvn_write_header(const char *path,
                 void *header, int32_t size,
                 int32_t *detailed_error_code)
{
    int32_t rc;
    bool sync_is_needed = false;
    int32_t fd = open(path, O_WRONLY | O_CREAT, S_IWUSR | S_IRUSR);

    if (fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

    int32_t remaining = size;

    struct stat buf;
    if (stat(path, &buf) == -1)
    {
        rc = FAIL_STAT_FILE;
        goto error_cleanup;
    }

    if (buf.st_size != remaining)
    {
        sync_is_needed = true;
        if (rvn_ftruncate(fd, size) == -1)
        {
            rc = FAIL_TRUNCATE_FILE;
            goto error_cleanup;
        }
    }

    while (remaining > 0)
    {
        uint64_t written = write(fd, header, (uint64_t)remaining);
        if (written == -1)
        {
            rc = FAIL_WRITE_FILE;
            goto error_cleanup;
        }

        remaining -= (int)written;
        header += written;
    }

    if (_flush_file(fd) == -1)
    {
        rc = FAIL_FLUSH_FILE;
        goto error_cleanup;
    }

    close(fd);
    fd = -1;

    if (sync_is_needed == true)
        return _sync_directory_for(path, detailed_error_code);
    return SUCCESS;

error_cleanup:
    *detailed_error_code = errno;
    if (fd != -1)
        close(fd);

    return rc;
}
