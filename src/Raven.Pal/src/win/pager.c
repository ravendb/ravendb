#include <windows.h>
#include <VersionHelpers.h>
#include <stdio.h>
#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

#ifdef _MSC_VER
static __forceinline int clzl(uint64_t x)
{
    unsigned long r = 0;
    BitScanReverse64(&r, x);
    return (int)(r ^ 63);
}
#else
__forceinline int clzl(uint64_t x)
{
    return __builtin_clzl(x);
}
#endif

struct handle
{
    HANDLE file_handle;
    HANDLE file_mapping_handle;
    void *read_address;
    void *write_address;
    uint64_t allocation_size;
    int32_t open_flags;
};

BOOL CALLBACK InitializeCriticalSectionOnce(PINIT_ONCE initOnce, PVOID Parameter, PVOID *Context)
{
    InitializeCriticalSection(Parameter);
    return TRUE;
}

uint64_t _GetNearestFileSize(uint64_t needed_size)
{
    const uint64_t POWER_OF_TWO_THRESHOLD = (uint64_t)512 * 1024ul * 1024ul; // 512MB
    if (needed_size == 0)
    {
        return (uint64_t)1024 * 1024;
    }
    if (needed_size < POWER_OF_TWO_THRESHOLD)
    {
        int32_t idx = clzl(needed_size);
        if (idx)
        {
            return (uint64_t)1 << (32 - idx);
        }
        return 1024 * 1024;
    }
    const uint64_t ONE_GB = (uint64_t)1024 * 1024 * 1024;
    // if it is over 0.5 GB, then we grow at 1 GB intervals
    return (needed_size + ONE_GB - 1) & ~ONE_GB;
}

BOOL _RecoverFromMemoryLockFailure(void *mem, SIZE_T size)
{
    static INIT_ONCE initOnce = INIT_ONCE_STATIC_INIT;
    static CRITICAL_SECTION working_set_increase_cs;
    InitOnceExecuteOnce(&initOnce, InitializeCriticalSectionOnce, &working_set_increase_cs, NULL);
    EnterCriticalSection(&working_set_increase_cs);

    BOOL rc;
    SIZE_T min_working_set, max_working_set;
    if (!GetProcessWorkingSetSize(GetCurrentProcess(),
                                  &min_working_set,
                                  &max_working_set))
    {
        rc = FALSE;
        goto Exit;
    }

    // From: https://msdn.microsoft.com/en-us/library/windows/desktop/ms686234(v=vs.85).aspx
    // "The maximum number of pages that a process can lock is equal to the number of pages in its minimum working set minus a small overhead"
    // let's increase the max size of memory we can lock by increasing the MinWorkingSet. On Windows, that is available for all users
    SIZE_T next_working_set_size = (SIZE_T)_GetNearestFileSize(min_working_set + size);
    if (next_working_set_size > INT32_MAX)
    {
        if (sizeof(void *) == 4)
        {
            next_working_set_size = INT32_MAX;
        }
    }
    // Minimum working set size must be less than or equal to the maximum working set size.
    // Let's increase the max as well.
    max_working_set = rvn_max(max_working_set, next_working_set_size);

    rc = SetProcessWorkingSetSize(GetCurrentProcess(),
                                  next_working_set_size,
                                  max_working_set);

Exit:
    LeaveCriticalSection(&working_set_increase_cs);
    return rc;
}

int32_t rvn_lock_memory(int32_t open_flags, void *mem, int64_t size, int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    if (sizeof(SIZE_T) == 4)
    {
        if (size >= INT32_MAX) // we won't uspport large values on 32 bits
        {
            rc = FAIL_SIZE_INVALID_32_BITS;
            goto Exit;
        }
    }
    if (size <= 0)
    {
        rc = FAIL_SIZE_NEGATIVE_OR_ZERO;
        goto Exit;
    }

    for (int i = 0; i < 10; ++i)
    {
        if (VirtualLock(mem, (SIZE_T)size) ||
            open_flags & OPEN_FILE_DO_NOT_CONSIDER_MEMORY_LOCK_FAILURE_AS_CATASTROPHIC_ERROR)
        {
            rc = SUCCESS;
            break;
        }

        if (!_RecoverFromMemoryLockFailure(mem, size))
        {
            rc = FAIL_LOCK_MEMORY;
            break;
        }
    }
Exit:
    *detailed_error_code = GetLastError();
    return rc;
}

EXPORT
int32_t rvn_unmap_memory(
    void *mem,
    int64_t size,
    int32_t *detailed_error_code)
{
    (void)size;
    *detailed_error_code = 0;
    if (!UnmapViewOfFile(mem))
    {
        *detailed_error_code = GetLastError();
        return FAIL_UNMAP_VIEW_OF_FILE;
    }
    return SUCCESS;
}
EXPORT
int32_t rvn_map_memory(void *handle,
                       int64_t offset,
                       int64_t size,
                       void **mem,
                       int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    if (sizeof(void *) == 4)
    {
        if (size > INT32_MAX)
        {
            rc = FAIL_SIZE_INVALID_32_BITS;
            goto Exit;
        }
    }
    if (size <= 0)
    {
        rc = FAIL_SIZE_NEGATIVE_OR_ZERO;
        goto Exit;
    }

    struct handle *handle_ptr = handle;
    DWORD dwDesiredAccess = (handle_ptr->open_flags & OPEN_FILE_WRITABLE_MAP) ? (FILE_MAP_READ | FILE_MAP_WRITE) : ((handle_ptr->open_flags & OPEN_FILE_COPY_ON_WRITE) ? FILE_MAP_COPY : FILE_MAP_READ);
    *mem = MapViewOfFile(handle_ptr->file_mapping_handle,
                         dwDesiredAccess,
                         offset >> 32,
                         (DWORD)offset,
                         (SIZE_T)size);

    if (*mem == NULL)
    {
        rc = FAIL_MAP_VIEW_OF_FILE;
        goto Exit;
    }

    if (handle_ptr->open_flags & OPEN_FILE_LOCK_MEMORY)
    {
        // intentionally returning the error code & rc from the lock_memory call
        int mem_lock_rc = rvn_lock_memory(handle_ptr->open_flags, *mem, size, detailed_error_code);
        if (rc != SUCCESS)
        {
            UnmapViewOfFile(*mem);
            *mem = NULL;
            return mem_lock_rc;
        }
    }

Exit:
    *detailed_error_code = GetLastError();
    return rc;
}


int32_t _open_pager_file(HANDLE h,
                         int32_t open_flags,
                         int64_t req_file_size,
                         void **handle,
                         void **memory,
                         void** writable_memory,
                         int64_t *memory_size,
                         int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    HANDLE m = INVALID_HANDLE_VALUE;
    struct handle *handle_ptr = NULL;
    void* mem = NULL;
    void* wmem = NULL;

    handle_ptr = calloc(1, sizeof(struct handle));
    if (handle_ptr == NULL)
    {
        rc = FAIL_NOMEM;
        goto Error;
    }

    LARGE_INTEGER file_size;
    if (!GetFileSizeEx(h, &file_size) || file_size.QuadPart < 0)
    {
        rc = FAIL_GET_FILE_SIZE;
        goto Error;
    }
    int64_t min_file_size = rvn_max(
        (req_file_size + ALLOCATION_GRANULARITY - 1) & ~(ALLOCATION_GRANULARITY - 1),
        ALLOCATION_GRANULARITY);

    if (min_file_size > file_size.QuadPart && !(open_flags & OPEN_FILE_READ_ONLY))
    {
        file_size.QuadPart = min_file_size;
        rc = _resize_file(h, min_file_size, detailed_error_code);
        if(rc != SUCCESS)
            goto Error;
    }
    else if( file_size.QuadPart == 0 && (open_flags & OPEN_FILE_READ_ONLY))
    {
        // we allow opening zero len files with read only mode, but don't try to map them
        handle_ptr->file_handle = h;
        handle_ptr->open_flags = open_flags | OPEN_FILE_DO_NOT_MAP;
        handle_ptr->file_mapping_handle = INVALID_HANDLE_VALUE;
        *memory_size = 0;
        *handle = handle_ptr;
        return SUCCESS;
    }

    DWORD flProtect = (open_flags & OPEN_FILE_WRITABLE_MAP) ? PAGE_READWRITE : PAGE_READONLY;
    m = CreateFileMapping(h, NULL, flProtect, 0, 0, NULL);

    if (m == NULL)
    {
        m = INVALID_HANDLE_VALUE;
        rc = FAIL_MMAP64;
        goto Error;
    }

    if ((open_flags & OPEN_FILE_DO_NOT_MAP))
    {
        handle_ptr->file_handle = h;
        handle_ptr->open_flags = open_flags;
        handle_ptr->file_mapping_handle = m;
        *memory_size = file_size.QuadPart;
        *handle = handle_ptr;
        return SUCCESS;
    }

    DWORD dwDesiredAccess = ((open_flags & OPEN_FILE_COPY_ON_WRITE) ? FILE_MAP_COPY : FILE_MAP_READ);

    mem = MapViewOfFile(m, dwDesiredAccess, 0, 0, 0);
    if (mem == NULL)
    {
        rc = FAIL_MAP_VIEW_OF_FILE;
        goto Error;
    }

    if (open_flags & OPEN_FILE_WRITABLE_MAP)
    {
        wmem = MapViewOfFile(m, FILE_MAP_WRITE, 0, 0, 0);
        if (wmem == NULL)
        {
            rc = FAIL_MAP_VIEW_OF_FILE;
            goto Error;
        }
    }

    CloseHandle(m);

    m = INVALID_HANDLE_VALUE;

    if (open_flags & OPEN_FILE_LOCK_MEMORY &&
        rvn_lock_memory(open_flags, mem, file_size.QuadPart, detailed_error_code) && 
        wmem != NULL && 
        rvn_lock_memory(open_flags, wmem, file_size.QuadPart, detailed_error_code))
    {
        rc = FAIL_LOCK_MEMORY;
        goto Error;
    }

    handle_ptr->file_handle = h;
    handle_ptr->read_address = mem;
    handle_ptr->write_address = wmem;
    handle_ptr->allocation_size = file_size.QuadPart;
    handle_ptr->open_flags = open_flags;
    handle_ptr->file_mapping_handle = INVALID_HANDLE_VALUE;
    *handle = handle_ptr;
    *memory = mem;
    *writable_memory = wmem;
    *memory_size = file_size.QuadPart;
    return SUCCESS;

Error:
    *detailed_error_code = GetLastError();
    if (mem != NULL)
    {
        UnmapViewOfFile(mem);
    }
    if(wmem != NULL)
    {
        UnmapViewOfFile(wmem);
    }
    CloseHandle(m);
    CloseHandle(h);
    free(handle_ptr);
    return rc;
}

EXPORT int32_t
rvn_init_pager(const char *filename,
               int64_t initial_file_size,
               int32_t open_flags,
               void **handle,
               void **memory,
               void** writable_memory,
               int64_t *memory_size,
               int32_t *detailed_error_code)
{
    *memory_size = 0;
    *memory = NULL;
    *handle = NULL;

    DWORD dwDesiredAccess = ((open_flags & OPEN_FILE_READ_ONLY) | (open_flags & OPEN_FILE_COPY_ON_WRITE)) ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE;
    DWORD dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
    dwFlagsAndAttributes |= open_flags & OPEN_FILE_TEMPORARY ? FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE : 0;
    dwFlagsAndAttributes |= open_flags & OPEN_FILE_SEQUENTIAL_SCAN ? FILE_FLAG_SEQUENTIAL_SCAN : FILE_FLAG_RANDOM_ACCESS;
    dwFlagsAndAttributes |= open_flags & OPEN_FILE_READ_ONLY ? FILE_ATTRIBUTE_READONLY : 0;

    HANDLE h = CreateFileW((LPCWSTR)filename, dwDesiredAccess,
                          FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
                          NULL, OPEN_ALWAYS, dwFlagsAndAttributes, NULL);
    if (h == INVALID_HANDLE_VALUE)
    {
        *detailed_error_code = GetLastError();
        return FAIL_OPEN_FILE;
    }

    return _open_pager_file(h, open_flags, initial_file_size, handle, memory, writable_memory, memory_size, detailed_error_code);
}

int32_t
rvn_increase_pager_size(void *handle,
                        int64_t new_length,
                        void **new_handle,
                        void **memory,
                        void **writable_memory,
                        int64_t *memory_size,
                        int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    *memory = NULL;
    *memory_size = 0;
    HANDLE h;
    if (!DuplicateHandle(GetCurrentProcess(),
                         handle_ptr->file_handle,
                         GetCurrentProcess(),
                         &h,
                         0,
                         FALSE,
                         DUPLICATE_SAME_ACCESS))
    {
        *detailed_error_code = GetLastError();
        return FAIL_DUPLICATE_HANDLE;
    }

    return _open_pager_file(h, handle_ptr->open_flags, new_length, new_handle, memory, writable_memory, memory_size, detailed_error_code);
}

EXPORT int32_t
rvn_close_pager(
    void *handle,
    int32_t *detailed_error_code)
{
    if(handle == NULL)
    {
        return FAIL_INVALID_HANDLE;
    }
    struct handle *handle_ptr = handle;
    *detailed_error_code = 0;
    int rc = SUCCESS;
    if (!(handle_ptr->open_flags & OPEN_FILE_DO_NOT_MAP))
    {
        if (!UnmapViewOfFile(handle_ptr->read_address))
        {
            *detailed_error_code = GetLastError();
            rc = FAIL_MAP_VIEW_OF_FILE;
        }
        if(handle_ptr->open_flags & OPEN_FILE_WRITABLE_MAP)
        {
            if (!UnmapViewOfFile(handle_ptr->write_address))
            {
                rc = FAIL_MAP_VIEW_OF_FILE;
                if (*detailed_error_code == 0)
                    *detailed_error_code = GetLastError();
            }
        }
    }
    if (!CloseHandle(handle_ptr->file_handle))
    {
        if (*detailed_error_code == 0)
            *detailed_error_code = GetLastError();
        if (rc == SUCCESS)
            rc = FAIL_CLOSE;
    }
    free(handle_ptr);
    return rc;
}

EXPORT int32_t
rvn_sync_pager(void *handle,
               int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    if (!FlushFileBuffers(handle_ptr->file_handle))
    {
        *detailed_error_code = GetLastError();
        return FAIL_SYNC_FILE;
    }
    return SUCCESS;
}

EXPORT
int32_t rvn_pager_get_file_handle(
    void *handle,
    void **file_handle,
    int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    HANDLE h;
    if (!DuplicateHandle(GetCurrentProcess(),
                         handle_ptr->file_handle,
                         GetCurrentProcess(),
                         &h,
                         0,
                         FALSE,
                         DUPLICATE_SAME_ACCESS))
    {
        *file_handle = NULL;
        *detailed_error_code = GetLastError();
        return FAIL_DUPLICATE_HANDLE;
    }
    *file_handle = h;
    return SUCCESS;
}
