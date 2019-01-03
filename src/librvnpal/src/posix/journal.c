#if defined(__unix__) && !defined(__APPLE__)

#define __USE_FILE_OFFSET64

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdint.h>
#include <rvn.h>
#include <status_codes.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

int64_t
rvn_pwrite(int32_t fd, void *buffer, int64_t count, int64_t offset, int32_t *detailed_error_code)
{
    int64_t actually_written = 0;
    int64_t cifs_retries = 3;
    do
    {
        int64_t result = pwrite(fd, buffer, count - actually_written, offset + actually_written);
        if (result < 0) /* we assume zero cannot be returned at any case as defined in POSIX */
        {
            if (errno == EINVAL && sync_directory_allowed(fd) == false && --cifs_retries > 0)
            {
                /* 
                cifs/nfs mount can sometimes fail on EINVAL after file creation
                lets give it few retries with short pauses between them - RavenDB-11954 
                wait upto 3 time in intervals of : 10mSec, 80mSec, 270mSec
                */
                usleep(10 * ((cifs_retries + 1) ^ 3));         
                continue; /* retry cifs */
            }
            *detailed_error_code = errno;
            if (cifs_retries != 3)
                return FAIL_PWRITE_WITH_RETRIES;
            return FAIL_PWRITE;
        }
        actually_written += result;
    } while (actually_written < count);

    return SUCCESS;
}

int32_t
rvn_allocate_file_space(int32_t fd, int64_t size, int32_t *detailed_error_code)
{
    int32_t result;
    int32_t retries;
    for (retries = 0; retries < 1024; retries++)
    {
        result = EINVAL;

#ifndef    __APPLE__
        result = posix_fallocate64(fd, 0, size);
#endif

        switch (result)
        {
        case EINVAL:
        case EFBIG: /* can occure on >4GB allocation on fs such as ntfs-3g, W95 FAT32, etc.*/
            /* fallocate is not supported, we'll use lseek instead */
            {
                char b = 0;
                int64_t rc = rvn_pwrite(fd, &b, 1UL, (uint64_t)size - 1UL, detailed_error_code);
                return rc;
            }
            break;
        case EINTR:
            *detailed_error_code = errno;
            continue; /* retry */

        case 0:
            return SUCCESS;

        default:
            *detailed_error_code = errno;
            return result;
        }
    }

    return result; /* return EINTR */
}


int32_t
open_journal(char* file_name, int32_t mode, int64_t file_size, void** handle, uint32_t* error_code)
{
    int rc;
    struct stat fs;
    int flags = O_DSYNC | O_DIRECT;
    if (mode & JOURNAL_MODE_DANGER)
        flags = 0;

    if (sizeof(void*) == 4) /* 32 bits */
        flags |= O_LARGEFILE;

    int fd = open(file_name, flags | O_WRONLY | O_CREAT, S_IWUSR | S_IRUSR);
    
    if (fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

#if __APPLE__
    /* mac doesn't support O_DIRECT, we fcntl instead: */
    if (!fcntl(fd, F_NOCACHE, 1) && !(flags & JOURNAL_MODE_DANGER))
    {
        rc = FAIL_SYNC_FILE;
        goto error_cleanup;
    }
#endif

    if (-1 == fstat(fd, &fs))
    {
        rc = FAIL_SEEK_FILE;
        goto error_cleanup;
    }

    if (fs.st_size < file_size)
    {
        rc = rvn_allocate_file_space(fd, file_size, (int32_t*)error_code);
        if(rc)
            goto error_cleanup;
    }
    
    *(int*)handle = fd;
    return SUCCESS;

error_cleanup:
    *(int32_t*)error_code = errno;
    if (fd != -1)
        close(fd);
    
    return rc;
}

int32_t
close_journal(void* handle, uint32_t* error_code)
{
#pragma GCC diagnostic ignored "-Wpointer-to-int-cast"
    int fd = (int)handle;
#pragma GCC diagnostic pop

    *error_code = close(fd);

    if (*error_code)
        return FAIL_CLOSE_FILE;
    return SUCCESS;
}

EXPORT int32_t
write_journal(void* handle, char* buffer, uint64_t size, int64_t offset, uint32_t* error_code)
{
#pragma GCC diagnostic ignored "-Wpointer-to-int-cast"
    return rvn_pwrite((int)handle, buffer, size, offset, (int32_t*)error_code);
#pragma GCC diagnostic pop 
}

#endif
