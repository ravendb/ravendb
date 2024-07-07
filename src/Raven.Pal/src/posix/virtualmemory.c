#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

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
#include "internal_posix.h"

EXPORT int32_t
rvn_prefetch_ranges(struct RVN_RANGE_LIST *range_list, int32_t count, int32_t *detailed_error_code)
{
    int32_t i = 0;
    for (i = 0; i < count; i++)
    {
        struct RVN_RANGE_LIST record = range_list[i];
        if(!madvise(record.virtual_address, record.number_of_bytes, MADV_WILLNEED))
        {
            *detailed_error_code = errno;
            return FAIL;
        }
    }
    return SUCCESS;
}
