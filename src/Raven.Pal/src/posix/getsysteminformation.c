#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <errno.h>
#include <sys/statvfs.h>

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

    return SUCCESS;
    
error:
    *detailed_error_code = errno;
    return FAIL;
}


EXPORT int32_t
rvn_get_path_disk_space(const char* path, uint64_t* total_free_bytes, uint64_t* total_size_bytes, int32_t* detailed_error_code)
{
    int rc;
    struct statvfs buffer;
    *detailed_error_code = 0;

    rc = statvfs(path, &buffer);

    if (rc != 0) {
        *detailed_error_code = errno;
        return FAIL_STAT_FILE;
    }

    *total_free_bytes = (uint64_t)buffer.f_bsize * (uint64_t)buffer.f_bavail;
    *total_size_bytes = (uint64_t)buffer.f_bsize * (uint64_t)buffer.f_blocks;

    return SUCCESS;
}
