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
        // the kernel drops the part of a WILLNEED request beyond what the device reads at once
        size_t request_size = record.request_size != 0 ? record.request_size : record.number_of_bytes;
        for (size_t done = 0; done < record.number_of_bytes; done += request_size)
        {
            size_t size = record.number_of_bytes - done < request_size ? record.number_of_bytes - done : request_size;
            if (madvise((char *)record.virtual_address + done, size, MADV_WILLNEED) != 0)
            {
                *detailed_error_code = errno;
                return FAIL;
            }
        }
    }
    return SUCCESS;
}
