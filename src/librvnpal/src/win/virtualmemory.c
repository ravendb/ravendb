#define _WIN32_WINNT 0x0603
#include <windows.h>
#include <processthreadsapi.h>
#include <errhandlingapi.h>
#include <memoryapi.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

EXPORT int32_t
rvn_prefetch_virtual_memory(void *virtual_address, int64_t length, int32_t *detailed_error_code)
{
    HANDLE handle = GetCurrentProcess();
    WIN32_MEMORY_RANGE_ENTRY entry;
    entry.NumberOfBytes = (SIZE_T)length;
    entry.VirtualAddress = virtual_address;

    if (PrefetchVirtualMemory(handle, 1, &entry, 0) == FALSE)
    {
        *detailed_error_code = GetLastError();
        return FAIL_PREFETCH;
    }

    return SUCCESS;
}

EXPORT int32_t
rvn_prefetch_ranges(struct RVN_RANGE_LIST *range_list, int32_t count, int32_t *detailed_error_code)
{
    int32_t rc;
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
            goto cleanup;
        }

        range_list += internal_count * sizeof(struct RVN_RANGE_LIST);
    } while (count > 0);

    rc = SUCCESS;
cleanup:
    *detailed_error_code = GetLastError();
    return rc;
}

