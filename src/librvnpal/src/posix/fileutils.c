#if defined(__unix__) || defined(__APPLE__)

#define _GNU_SOURCE
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <assert.h>

#include "rvn.h"
#include "status_codes.h"

int64_t
rvn_nearest_size_to_page_size(int64_t orig_size, int64_t sys_page_size)
{
    int64_t mod = orig_size % sys_page_size;
    if (mod == 0)
    {
        return orig_size;
    }
    return ((orig_size / sys_page_size) + 1) * sys_page_size;
}

int64_t
rvn_pwrite(int32_t fd, void *buffer, uint64_t count, uint64_t offset, int32_t *detailed_error_code)
{
    int64_t actually_written = 0;
    int64_t cifs_retries = 3;
    do
    {
        int64_t result = pwrite(fd, buffer, count - (uint64_t)actually_written, offset + actually_written);
        if (result < 0) /* we assume zero cannot be returned at any case as defined in POSIX */
        {
            if (errno == EINVAL && rvn_sync_directory_allowed(fd) == false && --cifs_retries > 0)
            {
                /* cifs/nfs mount can sometimes fail on EINVAL after file creation
                lets give it few retries with short pauses between them - RavenDB-11954 */

                sleep(1);
                continue; /* retry cifs */
            }
            *detailed_error_code = errno;
            if (cifs_retries != 3)
                return FAIL_PWRITE_WITH_RETRIES;
            return FAIL_PWRITE;
        }
        actually_written += result;
    } while (actually_written < (int64_t)count);

    return SUCCESS;
}

int32_t
rvn_allocate_file_space(int32_t fd, int64_t size, int32_t *detailed_error_code)
{
    int32_t result;
    int32_t retries;
    for (retries = 0; retries < 1024; retries++)
    {
        result = posix_fallocate64(fd, 0, size);

        switch (result)
        {
        case EINVAL:
        case EFBIG: /* can occure on >4GB allocation on fs such as ntfs-3g, W95 FAT32, etc.*/
            /* fallocate is not supported, we'll use lseek instead */
            {
                char b = 0;
                int64_t rc = rvn_pwrite(fd, &b, 1UL, (uint64_t)size - 1UL, detailed_error_code);
                if (rc != SUCCESS)
                    *detailed_error_code = errno;
                return rc;
            }
            break;
        case EINTR:
            *detailed_error_code = errno;
            continue; /* retry */

        case SUCCESS:
            return SUCCESS;

        default:
            *detailed_error_code = errno;
            return result;
        }
    }

    return result; /* return EINTR */
}

int32_t
rvn_pointer_to_int(void *ptr)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wint-conversion"
    return ptr;
#pragma GCC diagnostic pop
}

int32_t
rvn_dispose_handle(const char *filepath, void *handle, int32_t delete_on_close, int32_t *unlink_error_code, int32_t *close_error_code)
{
    int32_t rc = SUCCESS;

    /* the following in two lines to avoid compilation warning */
    int32_t fd = rvn_pointer_to_int(handle);
    *unlink_error_code = 0;
    *close_error_code = 0;

    if (fd != -1)
    {
        if (delete_on_close == true)
        {
            int32_t unlink_rc = unlink(filepath);
            if (unlink_rc != 0)
            {
                /* record the error and continue to close */
                rc |= FAIL_UNLINK;
                *unlink_error_code = errno;
            }
        }
    }

    int32_t close_rc = close(fd);
    if (close_rc != 0)
    {
        rc |= FAIL_CLOSE;
        *close_error_code = errno;
    }
    return rc;
}

#endif