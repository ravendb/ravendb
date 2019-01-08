#if defined(__unix__) || defined(__APPLE__)

#define _GNU_SOURCE
#include <unistd.h>
#include <errno.h>

#include "rvn.h"
#include "status_codes.h"

EXPORT int32_t
rvn_get_system_information(struct SYSTEM_INFORMATION *sys_info,
                           int32_t *detailed_error_code)
{
    int64_t page_size = sysconf(_SC_PAGE_SIZE);
    if (page_size == -1)
        goto error;

    sys_info->page_size = page_size;
    sys_info->can_prefetch = true;

    return SUCCESS;
    
error:
    *detailed_error_code = errno;
    return FAIL;
}

#endif