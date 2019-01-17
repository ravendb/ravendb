#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <errno.h>

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
