#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <errno.h>
#if defined(__APPLE__)
    #include <sys/mount.h>
    #define STAT_STRUCT statfs
    #define STAT_FUNC statfs
#elif defined(__linux__)
    #include <sys/statvfs.h>
    #define STAT_STRUCT statvfs
    #define STAT_FUNC statvfs
#endif

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

EXPORT int32_t
rvn_get_system_information(struct SYSTEM_INFORMATION *sys_info,
                           int32_t *detailed_error_code)
{
    int64_t page_size = sysconf(_SC_PAGE_SIZE);
    if (page_size == -1)
        goto error;

    sys_info->page_size = page_size;
    sys_info->prefetch_status = true;
    sys_info->voron_page_size = VORON_PAGE_SIZE;

    return SUCCESS;
    
error:
    *detailed_error_code = errno;
    return FAIL;
}


EXPORT int32_t
rvn_get_path_disk_space(const char* path, uint64_t* total_free_bytes, uint64_t* total_size_bytes, int32_t* detailed_error_code)
{
    int rc;
    struct STAT_STRUCT buffer;
    *detailed_error_code = 0;

    rc = STAT_FUNC(path, &buffer);

    if (rc != 0) {
        *detailed_error_code = errno;
        return FAIL_STAT_FILE;
    }

    *total_free_bytes = (uint64_t)buffer.f_bsize * (uint64_t)buffer.f_bavail;
    *total_size_bytes = (uint64_t)buffer.f_bsize * (uint64_t)buffer.f_blocks;

    return SUCCESS;
}
