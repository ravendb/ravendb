#include <windows.h>
#include <assert.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_win.h"



EXPORT int32_t
rvn_sync_directories(void* handle, char** folders, int32_t count, int32_t *detailed_error_code)
{
    return SUCCESS;
}

EXPORT int32_t
rvn_open_journal_for_writes(const char* file_name, int32_t transaction_mode, int64_t initial_file_size, int32_t durability_support, void** handle, int64_t* actual_size, int32_t* detailed_error_code)
{
    assert(initial_file_size > 0);

    DWORD access_flags;
    DWORD share_flags = FILE_SHARE_READ;
    switch (transaction_mode)
    {
        case JOURNAL_MODE_DANGER :
            access_flags = 0;
            break;
        case JOURNAL_MODE_PURE_MEMORY:
            access_flags = FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE;
            share_flags |= FILE_SHARE_WRITE | FILE_SHARE_DELETE;
            break;
        default:
            if (durability_support == DURABILITY_NOT_SUPPORTED)
            {
                access_flags = 0;
            }
            else
            {
                /* FILE_FLAG_OVERLAPPED is required so that concurrent journal writes are not serialized on the
                 * file object - each one brings its own completion event through the write context */
                access_flags = FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH | FILE_FLAG_OVERLAPPED;
            }
            break;
    }

    int32_t rc;
    HANDLE hFile = CreateFileW(
        (LPCWSTR)file_name,
        GENERIC_WRITE | GENERIC_READ,
        share_flags,
        NULL,
        OPEN_ALWAYS,
        access_flags,
        NULL);
    
    if (hFile == INVALID_HANDLE_VALUE)
    {
        *detailed_error_code = GetLastError();
        return FAIL_OPEN_FILE;
    }

    LARGE_INTEGER size;
    if (GetFileSizeEx(hFile, &size) == FALSE)
    {
        rc = FAIL_GET_FILE_SIZE;
        goto error_cleanup;
    }

    if (size.QuadPart <= initial_file_size)
    {
        rc = _pre_allocate_file(hFile, initial_file_size, detailed_error_code);
        if (rc != SUCCESS)
            goto error_clean_With_error;
        *actual_size = initial_file_size;
    }
    else
    {
        *actual_size = size.QuadPart;
    }

    *handle = hFile;
    return SUCCESS;

error_cleanup:
    *detailed_error_code = GetLastError();
error_clean_With_error:

    CloseHandle(hFile);
    return rc;
}

EXPORT int32_t
rvn_close_journal(void* handle, int32_t* detailed_error_code)
{
    HANDLE hFile = (HANDLE)handle;
    if (hFile != INVALID_HANDLE_VALUE &&
        hFile != NULL &&
        !CloseHandle(hFile))
    {
        *detailed_error_code = GetLastError();
        return FAIL_CLOSE;
    }

    return SUCCESS;
}

EXPORT int32_t
rvn_create_journal_write_context(void** context, int32_t* detailed_error_code)
{
    HANDLE hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
    if (!hEvent)
    {
        *detailed_error_code = GetLastError();
        return FAIL_CREATE_EVENT;
    }
    *context = hEvent;
    return SUCCESS;
}

EXPORT int32_t
rvn_free_journal_write_context(void* context, int32_t* detailed_error_code)
{
    if (context == NULL)
        return SUCCESS;

    HANDLE hEvent = (HANDLE)context;    

    if (hEvent != INVALID_HANDLE_VALUE &&
        hEvent != NULL &&
        !CloseHandle(hEvent))
    {
        *detailed_error_code = GetLastError();
        return FAIL_CLOSE;
    }

    return SUCCESS;
}

EXPORT int32_t
rvn_write_journal(void* handle, void* context, struct journal_entry* buffer, int64_t count_of_entries, int64_t offset, int32_t* detailed_error_code)
{
    HANDLE hFile = (HANDLE)handle;
    HANDLE hEvent = (HANDLE)context;

    for (int64_t entryIdx = 0; entryIdx < count_of_entries; entryIdx++)
    {
        int64_t size = buffer[entryIdx].number_of_4kbs * SYS_PAGE_SIZE;
        if(size / SYS_PAGE_SIZE != buffer[entryIdx].number_of_4kbs)
        {
            *detailed_error_code = ERROR_ARITHMETIC_OVERFLOW;
            return FAIL_MATH_OVERFLOW;
        }
        int32_t rc = _write_file(hFile, hEvent, buffer[entryIdx].base, size, offset, detailed_error_code);
        if(rc != SUCCESS)
            return rc;
        offset += size;
    }
    return SUCCESS;
}

EXPORT int32_t
rvn_open_journal_for_reads(const char *file_name, void **handle, int32_t *detailed_error_code)
{
    HANDLE hFile;
    int rc = _open_file_to_read(file_name, &hFile, detailed_error_code);
    if(rc != SUCCESS)
        return rc;

    *handle = hFile;
    return SUCCESS;
}

EXPORT int32_t
rvn_read_journal(void* handle, void* buffer, int64_t required_size, int64_t offset, int64_t* actual_size, int32_t* detailed_error_code)
{
    return _read_file((HANDLE)handle, buffer, required_size, offset, actual_size, detailed_error_code);
}

EXPORT int32_t
rvn_truncate_journal(void* handle, int64_t size, int32_t* detailed_error_code)
{
    HANDLE hFile = (HANDLE)handle;

    if (FlushFileBuffers(hFile) == FALSE)
    {
        *detailed_error_code = GetLastError();
        return FAIL_FLUSH_FILE;
    }

    return _truncate_file(hFile, size, detailed_error_code);
}

EXPORT int32_t
rvn_create_zeroed_file(const char *path, int64_t size, int32_t *detailed_error_code)
{
    // This will land in the .bss, so these all will be mapped to the same physical page (zero). 
    static char _zeroed_file_buffer[1024 * 1024] __attribute__((aligned(4096)));

    HANDLE h = CreateFileW((LPCWSTR)path, GENERIC_WRITE, 0, NULL, CREATE_NEW,
                           FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (h == INVALID_HANDLE_VALUE)
    {
        *detailed_error_code = GetLastError();
        return FAIL_OPEN_FILE;
    }

    int64_t offset = 0;
    while (offset < size)
    {
        DWORD len = (DWORD)(size - offset < (int64_t)sizeof(_zeroed_file_buffer) ? size - offset : (int64_t)sizeof(_zeroed_file_buffer));
        DWORD written = 0;
        OVERLAPPED ov = {0};
        ov.Offset = (DWORD)(offset & 0xFFFFFFFF);
        ov.OffsetHigh = (DWORD)(offset >> 32);
        if (WriteFile(h, _zeroed_file_buffer, len, &written, &ov) == FALSE || written != len)
        {
            *detailed_error_code = GetLastError();
            CloseHandle(h);
            DeleteFileW((LPCWSTR)path);
            return FAIL_WRITE_FILE;
        }
        offset += len;
    }

    if (FlushFileBuffers(h) == FALSE)
    {
        *detailed_error_code = GetLastError();
        CloseHandle(h);
        DeleteFileW((LPCWSTR)path);
        return FAIL_SYNC_FILE;
    }

    CloseHandle(h);
    return SUCCESS;
}

EXPORT int32_t
rvn_move_file_durable(const char *src, const char *dst, int32_t *detailed_error_code)
{
    if (MoveFileExW((LPCWSTR)src, (LPCWSTR)dst, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
        return SUCCESS;
    *detailed_error_code = GetLastError();
    return FAIL_MOVE_FILE;
}

EXPORT int32_t 
rvn_hard_link_non_durable(const char *src, const char *dst, int32_t *detailed_error_code)
{
    if(CreateHardLinkW((LPCWSTR)dst, (LPCWSTR)src, NULL))
        return SUCCESS;
    *detailed_error_code = GetLastError();
    return FAIL_HARD_LINK;
}

EXPORT int32_t
rvn_is_same_hard_link(const char *src, const char *dst, char *is_same, int32_t *detailed_error_code) {
    BY_HANDLE_FILE_INFORMATION src_info, dst_info;
    HANDLE src_handle = INVALID_HANDLE_VALUE;
    HANDLE dst_handle = INVALID_HANDLE_VALUE;
    int32_t rc = SUCCESS;
    src_handle = CreateFileW((LPCWSTR)src, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (src_handle == INVALID_HANDLE_VALUE) {
        *detailed_error_code = GetLastError();
        rc = FAIL_OPEN_FILE;
        goto End;
    }

    dst_handle = CreateFileW((LPCWSTR)dst, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (dst_handle == INVALID_HANDLE_VALUE) {
        int32_t error = GetLastError();
        if(error == ERROR_FILE_NOT_FOUND)
        {
            *is_same = false;
            rc = SUCCESS;
            goto End;
        }
        *detailed_error_code = error;
        rc = FAIL_OPEN_FILE;
        goto End;
    }

    // Get file information for source and destination
    if (!GetFileInformationByHandle(src_handle, &src_info)) {
        *detailed_error_code = GetLastError();
        rc = FAIL_STAT_FILE;
        goto End;
    }

    if (!GetFileInformationByHandle(dst_handle, &dst_info)) {
        *detailed_error_code = GetLastError();
        rc = FAIL_STAT_FILE;
        goto End;
    }

    // if same volume and same file index, then they are hard links
    *is_same = (src_info.dwVolumeSerialNumber == dst_info.dwVolumeSerialNumber) &&
               (src_info.nFileIndexHigh == dst_info.nFileIndexHigh) &&
               (src_info.nFileIndexLow == dst_info.nFileIndexLow);

    End:
    if(src_handle != INVALID_HANDLE_VALUE)
        CloseHandle(src_handle);
    if(dst_handle != INVALID_HANDLE_VALUE)
        CloseHandle(dst_handle);
    return rc;
}

EXPORT int32_t
rvn_is_hard_link(const char *path, char *is_hard_link, int32_t *detailed_error_code)
{
    BY_HANDLE_FILE_INFORMATION info;
    HANDLE h = CreateFileW((LPCWSTR)path, GENERIC_READ,
                           FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                           NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (h == INVALID_HANDLE_VALUE)
    {
        int32_t error = GetLastError();
        if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND)
        {
            *is_hard_link = false;
            *detailed_error_code = 0;
            return SUCCESS;
        }
        *detailed_error_code = error;
        return FAIL_OPEN_FILE;
    }

    int32_t rc = SUCCESS;
    if (GetFileInformationByHandle(h, &info))
    {
        *is_hard_link = info.nNumberOfLinks > 1;
    }
    else
    {
        *is_hard_link = false;
        *detailed_error_code = GetLastError();
        rc = FAIL_STAT_FILE;
    }

    CloseHandle(h);
    return rc;
}
