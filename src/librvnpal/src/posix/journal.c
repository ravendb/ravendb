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
#include <stdio.h>

int32_t
rvn_open_journal(char* file_name, int32_t mode, int64_t required_size, void** handle, int64_t* actual_size, int32_t* error_code)
{
    int rc;
    struct stat fs;
    int flags = O_DSYNC | O_DIRECT;
    if (mode & JOURNAL_MODE_DANGER)
        flags = 0;

    if (sizeof(void*) == 4) /* 32 bits */
        flags |= O_LARGEFILE;

    struct stat buffer;
    int exist = stat(file_name, &buffer);
    int fd = open(file_name, flags | O_WRONLY | O_CREAT, S_IWUSR | S_IRUSR);
    
    if (-1 == fd)
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

    if (fstat(fd, &fs))
    {
        rc = FAIL_SEEK_FILE;
        goto error_cleanup;
    }

    int remainder = required_size % 4096;
    required_size = remainder ? required_size + 4096 - remainder : required_size;
    if (fs.st_size < required_size)
    {
        rc = rvn_allocate_file_space(fd, required_size, (int32_t*)error_code);
        if(rc)
            goto error_cleanup;
    }
    
    *actual_size = required_size;
    *(int*)handle = fd;
    return SUCCESS;

error_cleanup:
    *(int32_t*)error_code = errno;
    if (fd != -1)
    {
        close(fd);
        if(-1 == exist)
        {
            remove(file_name);
        }
    }
    
    return rc;
}

int32_t
rvn_close_journal(void* handle, int32_t* error_code)
{
#pragma GCC diagnostic ignored "-Wpointer-to-int-cast"
    int fd = (int)handle;
#pragma GCC diagnostic pop

    if(close(fd))
    {
        *error_code = errno;
        return FAIL_CLOSE;
    }

    return SUCCESS;
}

EXPORT int32_t
rvn_write_journal(void* handle, char* buffer, uint64_t size, int64_t offset, int32_t* error_code)
{

    int fd = (int)(long)handle;

    return rvn_pwrite(fd, buffer, size, offset, (int32_t*)error_code);
}

EXPORT int32_t
rvn_read_journal(char* file_name, void** handle, char* buffer, uint64_t required_size, int64_t offset, uint64_t* actual_size, int32_t* error_code)
{
    int fd = *(int*)handle;
    *actual_size = fd;
    int rc;
    if (-1 == fd)
    {
        fd = open(file_name, O_RDONLY, S_IRUSR);

        if (-1 == fd)
        {
            rc = FAIL_OPEN_FILE;
            goto error_cleanup;
        }
        
        *(int*)handle = fd;
    }
    
    int remain_size = required_size;
    int already_read;
    while (remain_size > 0)
    {
        already_read = pread(fd, buffer, remain_size, offset);
        if(-1 == already_read)
        {
            rc = FAIL_READ_FILE;
            goto error_cleanup;
        }
        if (0 == already_read) /*eof*/
            break;
            
        remain_size -= already_read;
        buffer += already_read;
        offset += already_read;
    }
    
    *actual_size = required_size - remain_size;
    return SUCCESS;
    
    error_cleanup:
        *error_code = errno;
        return rc;
}

EXPORT int32_t
rvn_truncate_journal(char* file_name, void* handle, uint64_t size, int32_t* error_code)
{
    int fd = (int)(long)handle;

    int rc;
    if (ftruncate(fd, size))
    {
        rc = FAIL_TRUNCATE;
        goto error_cleanup;
    }
    
#if __APPLE__
    if(fcntl(fd, F_FULLFSYNC, 0)) /*F_FULLFSYNC ignores args*/
#else
    if(fsync(fd))
#endif
    {
        rc = FAIL_SYNC_FILE;
        goto error_cleanup;
    }    

    return rvn_sync_directory_for(file_name, error_code);
    
error_cleanup:
    *error_code = errno;
    return rc;
}

#endif
