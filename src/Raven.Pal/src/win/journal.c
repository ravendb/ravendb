#include <windows.h>
#include <assert.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_win.h"


int32_t
rvn_write_journal_gather(void* handle, struct journal_entry* buffer, int64_t count_of_entries, int64_t offset, int32_t* detailed_error_code);
int32_t
rvn_write_journal_file(void* handle, struct journal_entry* buffer, int64_t count_of_entries, int64_t offset, int32_t* detailed_error_code);

EXPORT int32_t
rvn_open_journal_for_writes(const char* file_name, int32_t transaction_mode, int64_t initial_file_size, int32_t durability_support, void** handle, int64_t* actual_size, int32_t* detailed_error_code)
{
    assert(initial_file_size > 0);

    struct journal_handle* jrnl_handle = calloc(1, sizeof(struct journal_handle));
    if(jrnl_handle == NULL)
    {
        *detailed_error_code = GetLastError();
        return FAIL_CALLOC;
    }
    DWORD access_flags;
    DWORD share_flags = FILE_SHARE_READ;
    switch (transaction_mode)
    {
        case JOURNAL_MODE_DANGER :
            access_flags = 0;
            jrnl_handle->writer = rvn_write_journal_file;
            break;
        case JOURNAL_MODE_PURE_MEMORY:
            access_flags = FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE;
            share_flags |= FILE_SHARE_WRITE | FILE_SHARE_DELETE;
            jrnl_handle->writer = rvn_write_journal_file;
            break;
        default:
            if (durability_support == DURABILITY_NOT_SUPPORTED)
            {
                access_flags = 0;
                jrnl_handle->writer = rvn_write_journal_file;
            }
            else
            {
                access_flags = FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH | FILE_FLAG_OVERLAPPED;
                jrnl_handle->writer = rvn_write_journal_gather;
            }
            break;
    }

    int32_t rc;
    jrnl_handle->hFile = CreateFileW(
        (LPCWSTR)file_name,
        GENERIC_WRITE | GENERIC_READ,
        share_flags,
        NULL,
        OPEN_ALWAYS,
        access_flags,
        NULL);
    
    if (jrnl_handle->hFile  == INVALID_HANDLE_VALUE)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

    jrnl_handle->hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
    if(!jrnl_handle->hEvent)
    {
        rc = FAIL_CREATE_EVENT;
        goto error_cleanup;
    }
    LARGE_INTEGER size;
    if (GetFileSizeEx(jrnl_handle->hFile, &size) == FALSE)
    {
        rc = FAIL_GET_FILE_SIZE;
        goto error_cleanup;
    }

    if (size.QuadPart <= initial_file_size)
    {
        rc = _pre_allocate_file(jrnl_handle->hFile, initial_file_size, detailed_error_code);
        if (rc != SUCCESS)
            goto error_clean_With_error;
        *actual_size = initial_file_size;
    }
    else
    {
        *actual_size = size.QuadPart;
    }

    *handle = jrnl_handle;
    return SUCCESS;

error_cleanup:
    *detailed_error_code = GetLastError();
error_clean_With_error:
    
    if(jrnl_handle != NULL)
    {
        int32_t ignored;
        rvn_close_journal(jrnl_handle, &ignored);
    }   
    return rc;
}

EXPORT int32_t
rvn_close_journal(void* handle, int32_t* detailed_error_code)
{
    struct journal_handle* jrnl_handle = handle;
    bool failure = false;
    if (jrnl_handle->hEvent != INVALID_HANDLE_VALUE &&
        jrnl_handle->hEvent != NULL &&
        !CloseHandle(jrnl_handle->hEvent))
    {
        *detailed_error_code = GetLastError();
        failure = true;
    }
    if (jrnl_handle->hFile != INVALID_HANDLE_VALUE &&
        jrnl_handle->hFile != NULL &&
        !CloseHandle(jrnl_handle->hFile))
    {
        *detailed_error_code = GetLastError();
        failure = true;
    }
    free(jrnl_handle->elements);
    free(jrnl_handle);
    return failure ? FAIL_CLOSE : SUCCESS;
}

PRIVATE
int32_t ensure_enough_elements(struct journal_handle *jrnl_handle, uint64_t required, int32_t* detailed_error_code){
     if(required < jrnl_handle->elements_count)
        return SUCCESS;
    uint64_t new_count = nextPowerOf2(required + 1);
    uint64_t new_bytes = new_count * sizeof(FILE_SEGMENT_ELEMENT);
    if(new_bytes / new_count != sizeof(FILE_SEGMENT_ELEMENT))
    {
        *detailed_error_code = ERROR_ARITHMETIC_OVERFLOW;
        return FAIL_NOMEM;
    }
    void* new_elements = realloc(jrnl_handle->elements, new_bytes);
    if(new_elements == NULL)
    {
        *detailed_error_code = GetLastError();
        return FAIL_NOMEM;
    }
    jrnl_handle->elements = new_elements;
    jrnl_handle->elements_count = new_count;
    return SUCCESS;
}

PRIVATE
int32_t flush_entries(struct journal_handle *jrnl_handle, int64_t offset, DWORD bytesWritten, int32_t* detailed_error_code)
{
    if(bytesWritten == 0)
        return SUCCESS;

    OVERLAPPED ov = {
        .Offset = (int)(offset & 0xffffffff),
        .OffsetHigh = (int)(offset >> 32),
        .hEvent = jrnl_handle->hEvent
    };
    ResetEvent(jrnl_handle->hEvent);
    if (WriteFileGather(jrnl_handle->hFile, jrnl_handle->elements, bytesWritten, NULL, &ov) == FALSE)
    {
        DWORD err = GetLastError();
        if (err != ERROR_IO_PENDING)
        {
            *detailed_error_code = err;
            return FAIL_WRITE_FILE;
        }
        DWORD expectedBytesWritten;
        if(GetOverlappedResult(jrnl_handle->hFile, &ov, &expectedBytesWritten, TRUE) == FALSE || 
            expectedBytesWritten != bytesWritten)
        {
            *detailed_error_code = GetLastError();
            return FAIL_WRITE_COMPLETION;
        }
    }
    return SUCCESS;
}


EXPORT int32_t
rvn_write_journal(void* handle, struct journal_entry* buffer, int64_t count_of_entries, int64_t offset, int32_t* detailed_error_code)
{
    struct journal_handle *jrnl_handle = handle;
    return rvn_write_journal_file(jrnl_handle, buffer, count_of_entries, offset, detailed_error_code);
}

PRIVATE int32_t
rvn_write_journal_gather(void* handle, struct journal_entry* buffer, int64_t count_of_entries, int64_t offset, int32_t* detailed_error_code)
{
    struct journal_handle *jrnl_handle = handle;
    int64_t element_idx = 0;
    DWORD bytesWritten = 0;
    int32_t rc = SUCCESS;
    for (int64_t entryIdx = 0; entryIdx < count_of_entries; entryIdx++)
    {
        int64_t required = element_idx + buffer[entryIdx].number_of_4kbs;
        rc = ensure_enough_elements(jrnl_handle, required, detailed_error_code);
        if(rc != SUCCESS)
        {
            return rc;
        }

        for (int64_t i = 0; i < buffer[entryIdx].number_of_4kbs; i++)
        {
            jrnl_handle->elements[element_idx++].Buffer = (char*)buffer[entryIdx].base + (i * SYS_PAGE_SIZE);
            bytesWritten += SYS_PAGE_SIZE;
            if (bytesWritten >= INT_MAX)
            {
                rc = flush_entries(jrnl_handle, offset, bytesWritten, detailed_error_code);
                if(rc != SUCCESS)
                    return rc;

                offset += bytesWritten;
                bytesWritten = 0;
                element_idx = 0;
            }
        }
    }
    return flush_entries(jrnl_handle, offset, bytesWritten, detailed_error_code);
}

EXPORT int32_t
rvn_open_journal_for_reads(const char *file_name, void **handle, int32_t *detailed_error_code)
{
    struct journal_handle* jrnl_handle = calloc(1, sizeof(struct journal_handle));
    if(jrnl_handle == NULL)
    {
        *detailed_error_code = GetLastError();
        return FAIL_CALLOC;
    }
    jrnl_handle->writer = NULL;
    int rc = _open_file_to_read(file_name, &jrnl_handle->hFile, detailed_error_code);
    if(rc != SUCCESS)
    {
        int32_t ignored;
        rvn_close_journal(jrnl_handle, &ignored);
        return rc;
    }
    *handle = jrnl_handle;
    return SUCCESS;
}

EXPORT int32_t
rvn_read_journal(void* handle, void* buffer, int64_t required_size, int64_t offset, int64_t* actual_size, int32_t* detailed_error_code)
{
    struct journal_handle* jrnl_handle = handle;
    return _read_file(jrnl_handle->hFile, buffer, required_size, offset, actual_size, detailed_error_code);
}

EXPORT int32_t
rvn_truncate_journal(void* handle, int64_t size, int32_t* detailed_error_code)
{
    struct journal_handle* jrnl_handle = handle;
    
    if (FlushFileBuffers(jrnl_handle->hFile) == FALSE)
    {
        *detailed_error_code = GetLastError();
        return FAIL_FLUSH_FILE;
    }

    return _truncate_file(jrnl_handle->hFile, size, detailed_error_code);
}

EXPORT int32_t 
rvn_hard_link(const char *src, const char *dst, int32_t *detailed_error_code)
{
    if(CreateHardLinkW((LPCWSTR)dst, (LPCWSTR)src, NULL))
        return SUCCESS;
    *detailed_error_code = GetLastError();
    return FAIL_HARD_LINK;
}

PRIVATE int32_t
rvn_write_journal_file(void* handle, struct journal_entry* buffer, int64_t count_of_entries, int64_t offset, int32_t* detailed_error_code)
{
    struct journal_handle* jrnl_handle = handle;

    for (int64_t entryIdx = 0; entryIdx < count_of_entries; entryIdx++)
    {
        int64_t size = buffer[entryIdx].number_of_4kbs * SYS_PAGE_SIZE;
        if(size / SYS_PAGE_SIZE != buffer[entryIdx].number_of_4kbs)
        {
            *detailed_error_code = ERROR_ARITHMETIC_OVERFLOW;
            return FAIL_MATH_OVERFLOW;
        }
        int32_t rc = _write_file(jrnl_handle, buffer[entryIdx].base, size, offset, detailed_error_code);
        if(rc != SUCCESS)
            return rc;
        offset += size;
    }
    return SUCCESS;
}

EXPORT int32_t
rvn_is_same_hard_link(const char *src, const char *dst, bool *is_same, int32_t *detailed_error_code) {
    BY_HANDLE_FILE_INFORMATION src_info, dst_info;
    HANDLE src_handle = INVALID_HANDLE_VALUE;
    HANDLE dst_handle = INVALID_HANDLE_VALUE;
    int32_t rc = SUCCESS;
    src_handle = CreateFileW(src, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (src_handle == INVALID_HANDLE_VALUE) {
        *detailed_error_code = GetLastError();
        rc = FAIL_OPEN_FILE;
        goto End;
    }

    dst_handle = CreateFileW(dst, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (dst_handle == INVALID_HANDLE_VALUE) {
        *detailed_error_code = GetLastError();
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
