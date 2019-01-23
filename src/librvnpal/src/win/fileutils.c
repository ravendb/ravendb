#include <windows.h>
#include <stdint.h>
#include <assert.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

PRIVATE int32_t
_resize_file(void *handle, int64_t size, int32_t *detailed_error_code)
{
    assert(size % 4096 == 0);

    int32_t rc;
    LARGE_INTEGER distance_to_move;
    distance_to_move.QuadPart = size;
    if (SetFilePointerEx(handle, distance_to_move, NULL, FILE_BEGIN) == FALSE)
    {
        rc = FAIL_SET_FILE_POINTER;
        goto error_cleanup;
    }

    if (SetEndOfFile(handle) == FALSE)
    {
        rc = FAIL_SET_EOF;
        goto error_cleanup;
    }

    return SUCCESS;

error_cleanup:
    *detailed_error_code = GetLastError();
    return rc;
}

PRIVATE int32_t
write_file_in_sections(void* handle, const char* buffer, int64_t size, int64_t offset, uint32_t section_size, int32_t* detailed_error_code)
{
    OVERLAPPED overlapped;
    memset(&overlapped, 0, sizeof(overlapped));
    overlapped.Offset = (int)(offset & 0xffffffff);
    overlapped.OffsetHigh = (int)(offset >> 32);

    DWORD actual_size_to_write;
    while (size > 0)
    {
        if (size < section_size)
        {
            actual_size_to_write = (DWORD)size;
        }
        else
        {
            actual_size_to_write = section_size;
        }

        if (WriteFile(handle, buffer, actual_size_to_write, NULL, &overlapped) == FALSE)
        {
            *detailed_error_code = GetLastError();
            return FAIL_WRITE_FILE;
        }

        buffer += actual_size_to_write;
        size -= actual_size_to_write;

        offset += actual_size_to_write;
        overlapped.Offset = (int)(offset & 0xffffffff);
        overlapped.OffsetHigh = (int)(offset >> 32);
    }

    return SUCCESS;
}

PRIVATE int32_t
_write_file(void* handle, const void* buffer, int64_t size, int64_t offset, int32_t* detailed_error_code)
{
    const int32_t WRITE_INCREMENT = 4096;

    assert(size % WRITE_INCREMENT == 0);
    assert((int64_t)buffer % WRITE_INCREMENT == 0);

    int32_t rc = write_file_in_sections(handle, (char*)buffer, size, offset, UINT32_MAX, detailed_error_code);
    if (rc == SUCCESS)
        return SUCCESS;

    if (*detailed_error_code != ERROR_WORKING_SET_QUOTA)
        return FAIL_WRITE_FILE;

    // this error can happen under low memory conditions, instead of trying to write the whole thing in a single shot
    // we'll write it in 4KB increments. This is likely to be much slower, but failing here will fail the entire DB
    return write_file_in_sections(handle, (char*)buffer, size, offset, WRITE_INCREMENT, detailed_error_code);
}

PRIVATE int32_t
_open_file_to_read(const char *file_name, void **handle, int32_t *detailed_error_code)
{
    HANDLE hfile = CreateFile(
        file_name,
        GENERIC_READ,
        FILE_SHARE_WRITE | FILE_SHARE_READ | FILE_SHARE_DELETE,
        0,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        0);

    if (hfile == INVALID_HANDLE_VALUE)
    {
        *detailed_error_code = GetLastError();
        return FAIL_OPEN_FILE;
    }

    *handle = hfile;
    return SUCCESS;
}

PRIVATE int32_t 
_read_file(void* handle, void* buffer, int64_t required_size, int64_t offset, int64_t* actual_size, int32_t* detailed_error_code)
{
    int32_t rc;

    OVERLAPPED overlapped;
    memset(&overlapped, 0, sizeof(overlapped));
    
    DWORD internal_actual_size;
    DWORD internal_required_size;
    while(required_size > 0 )
    {
        overlapped.Offset = (int)(offset & 0xffffffff);
        overlapped.OffsetHigh = (int)(offset >> 32);

        if(required_size > UINT_MAX)
        {
            internal_required_size = UINT_MAX;
        }
        else
        {
            internal_required_size = (DWORD)required_size;
        }

        if (ReadFile(handle, buffer, internal_required_size, &internal_actual_size, &overlapped) == FALSE)
        {
            if (GetLastError() == ERROR_HANDLE_EOF)
            {
                rc = FAIL_EOF;
            }
            else
            {
                rc = FAIL_READ_FILE;
            }
            goto error_cleanup;
        }
        *actual_size += internal_actual_size;

        buffer = (char*)buffer + internal_actual_size;
        offset += internal_actual_size;
        required_size -= internal_actual_size;
    }

    return SUCCESS;

error_cleanup:
    *detailed_error_code = GetLastError();
    return rc;
}
