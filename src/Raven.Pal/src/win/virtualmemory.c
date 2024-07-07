#include <windows.h>
#include <processthreadsapi.h>
#include <errhandlingapi.h>
#include <memoryapi.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"


EXPORT int32_t
rvn_prefetch_ranges(struct RVN_RANGE_LIST *range_list, int32_t count, int32_t *detailed_error_code)
{
    int32_t rc;
#ifndef RVN_WIN7
    HANDLE handle = GetCurrentProcess();

    WIN32_MEMORY_RANGE_ENTRY entries[16];
    do
    {
        int internal_count;
        if (count > 16)
        {
            internal_count = 16;
        }
        else
        {
            internal_count = count;
        }
        count -= internal_count;

        for (int32_t i = 0; i < internal_count; i++)
        {
            entries[i].NumberOfBytes = (int32_t)range_list[i].number_of_bytes;
            entries[i].VirtualAddress = range_list[i].virtual_address;
        }

        if (PrefetchVirtualMemory(handle, internal_count, entries, 0) == FALSE)
        {
            *detailed_error_code = GetLastError();
            rc = FAIL_PREFETCH;
            goto error_cleanup;
        }

        range_list += internal_count;
    } while (count > 0);
#endif
    return SUCCESS;
error_cleanup:
    *detailed_error_code = GetLastError();
    return rc;
}

