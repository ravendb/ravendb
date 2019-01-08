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

EXPORT int32_t
rvn_prefetch_virtual_memory(void *virtualAddress, int64_t length, int32_t *detailed_error_code)
{
    int32_t rc = madvise(virtualAddress, length, MADV_WILLNEED);
    if (rc != SUCCESS)
        *detailed_error_code = errno;
    return rc;
}

EXPORT int32_t
rvn_prefetch_ranges(struct RVN_RANGE_LIST *range_list, int32_t count, int32_t *detailed_error_code)
{
    int32_t i = 0;
    for (i = 0; i < count; i++)
    {
        struct RVN_RANGE_LIST record = range_list[i];
        if (rvn_prefetch_virtual_memory(record.virtual_address, record.number_of_bytes, detailed_error_code) != 0)
            return FAIL;
    }
    return SUCCESS;
}

EXPORT int32_t
rvn_protect_range(void *start_address, int64_t size, bool protection, int32_t *detailed_error_code)
{
    int32_t mprotect_flags = PROT_READ;
    if (protection == false)
        mprotect_flags |= PROT_WRITE;

    int32_t rc = mprotect(start_address, size, mprotect_flags);

    if (rc != 0)
        *detailed_error_code = errno;
    return rc;
}

#endif