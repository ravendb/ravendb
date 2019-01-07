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

int32_t
rvn_prefetch_virtual_memory(void *virtualAddress, int64_t length, int32_t *detailed_error_code)
{
    int32_t rc = madvise(virtualAddress, length, MADV_WILLNEED);
    if (rc != SUCCESS)
        *detailed_error_code = errno;
    return rc;
}

int32_t
rvn_prefetch_ranges(struct RVN_RANGE_LIST *range_list, int32_t count, int32_t *detailed_error_code)
{
    int32_t i = 0;
    int32_t rc = 0;
    int32_t prefetch_errno = 0;
    for (i = 0; i < count; i++)
    {
        struct RVN_RANGE_LIST record = range_list[i];
        int32_t prefetch_rc = rvn_prefetch_virtual_memory(record.virtual_address, record.number_of_bytes, &prefetch_errno);
        if (rc == 0) /* record first failure */
        {
            rc = prefetch_rc;
            *detailed_error_code = prefetch_errno;
        }
    }
    return rc;
}

int32_t
rvn_protect_range(void *start_address, int64_t size, int32_t flags, int32_t *detailed_error_code)
{
    int32_t mprotect_flags = 0;
    if (flags & MPROTECT_OPTIONS_PROT_NONE)
        mprotect_flags |= PROT_NONE;
    if (flags & MPROTECT_OPTIONS_PROT_READ)
        mprotect_flags |= PROT_READ;
    if (flags & MPROTECT_OPTIONS_PROT_WRITE)
        mprotect_flags |= PROT_WRITE;
    if (flags & MPROTECT_OPTIONS_PROT_EXEC)
        mprotect_flags |= PROT_EXEC;
#ifndef __APPLE__        
    if (flags & MPROTECT_OPTIONS_PROT_GROWSUP)
        mprotect_flags |= PROT_GROWSUP;
    if (flags & MPROTECT_OPTIONS_PROT_GROWSDOWN)
        mprotect_flags |= PROT_GROWSDOWN;
#endif

    int32_t rc = mprotect(start_address, size, mprotect_flags);

    if (rc != 0)
        *detailed_error_code = errno;
    return rc;
}

#endif