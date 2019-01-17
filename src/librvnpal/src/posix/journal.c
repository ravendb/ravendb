#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif 

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <assert.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

EXPORT int32_t
rvn_open_journal_for_writes(const char *file_name, int32_t transaction_mode, int64_t initial_file_size, void **handle, int64_t *actual_size, int32_t *detailed_error_code)
{
    assert(initial_file_size > 0);

    int32_t rc;
    struct stat fs;
    int32_t flags = O_DSYNC | O_DIRECT;
    if (transaction_mode == JOURNAL_MODE_DANGER)
        flags = 0;

    if (sizeof(int) == 4) /* 32 bits */
        flags |= O_LARGEFILE;

    int32_t fd = open(file_name, flags | O_WRONLY | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }
    *handle = (void*)(int64_t)fd;

    if ((flags & O_DIRECT) == false && _finish_open_file_with_odirect(fd) == -1)
    {
        rc = FAIL_SYNC_FILE;
        goto error_cleanup;
    } 

    if (fstat(fd, &fs) == -1)
    {
        rc = FAIL_STAT_FILE;
        goto error_cleanup;
    }

    if (fs.st_size < initial_file_size)
    {
        rc = _resize_file(*handle, initial_file_size, detailed_error_code);
        if (rc != SUCCESS)
            goto error_clean_With_error;

        *actual_size = initial_file_size;
    }
    else
    {
        *actual_size = fs.st_size;
    }    
    return SUCCESS;

error_cleanup:
    *detailed_error_code = errno;
error_clean_With_error:
    if (fd != -1)
        close(fd);

    return rc;
}

EXPORT int32_t
rvn_close_journal(void *handle, int32_t *detailed_error_code)
{
    int32_t fd = (int32_t)(int64_t)handle;
    if (close(fd) == -1)
    {
        *detailed_error_code = errno;
        return FAIL_CLOSE;
    }

    return SUCCESS;
}

EXPORT int32_t
rvn_write_journal(void *handle, void *buffer, int64_t size, int64_t offset, int32_t *detailed_error_code)
{
    int32_t fd = (int32_t)(int64_t)handle;
    return _pwrite(fd, buffer, size, offset, detailed_error_code);
}

EXPORT int32_t
rvn_open_journal_for_reads(const char *file_name, void **handle, int32_t *detailed_error_code){
    return _open_file_to_read(file_name, handle, detailed_error_code);
}

EXPORT int32_t
rvn_read_journal(void *handle, void *buffer, int64_t required_size, int64_t offset, int64_t *actual_size, int32_t *detailed_error_code)
{
    return _read_file(handle, buffer, required_size, offset, actual_size, detailed_error_code);
}

EXPORT int32_t
rvn_truncate_journal(const char *file_name, void *handle, int64_t size, int32_t *detailed_error_code)
{
    int32_t rc;
    int32_t fd = (int32_t)(int64_t)handle;

    if (_flush_file(fd) == -1)
    {
        rc = FAIL_SYNC_FILE;
        goto error_cleanup;
    }

    rc = _resize_file(handle, size, detailed_error_code);
    if(rc != SUCCESS)
        return rc;

    return _sync_directory_for(file_name, detailed_error_code);

error_cleanup:
    *detailed_error_code = errno;
    return rc;
}
