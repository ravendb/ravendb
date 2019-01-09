#include <windows.h>
#include <stdint.h>
#include <assert.h>
#include <rvn.h>
#include <status_codes.h>

int32_t rvn_open_journal(char* file_name, int32_t mode, int64_t required_size, void** handle, int64_t* actual_size, int32_t* error_code)
{
    assert(required_size > 0);

    DWORD flags = FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;

    if (mode == JOURNAL_MODE_DANGER)
        flags = 0;

    if (mode == JOURNAL_MODE_PURE_MEMORY)
        flags = FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE;

    int32_t rc;
    HANDLE hFile = CreateFile(
        file_name,
        GENERIC_WRITE,
        FILE_SHARE_READ,
        NULL,
        OPEN_ALWAYS,
        flags,
        NULL);
    DWORD create_file_err = GetLastError();

    if (hFile == INVALID_HANDLE_VALUE)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

    LARGE_INTEGER size;
    if (!GetFileSizeEx(hFile, &size))
    {
        rc = FAIL_GET_FILE_SIZE;
        goto error_cleanup;
    }

    int remainder = required_size % 4096;
    required_size = remainder ? required_size + 4096 - remainder : required_size;
    if (size.QuadPart < required_size)
    {
        size.QuadPart = required_size;
        if (!SetFilePointerEx(hFile, size, NULL, FILE_BEGIN))
        {
            rc = FAIL_SET_FILE_POINTER;
            goto error_cleanup;
        }

        if (!SetEndOfFile(hFile))
        {
            rc = FAIL_SET_EOF;
            goto error_cleanup;
        }
    }

    *actual_size = size.QuadPart;
    *handle = hFile;

    return SUCCESS;

error_cleanup:
    *error_code = GetLastError();
    if (hFile != INVALID_HANDLE_VALUE)
        CloseHandle(hFile);

    if (create_file_err != ERROR_ALREADY_EXISTS)
    {
        DeleteFileA(file_name);
    }
    return rc;
}

int32_t rvn_close_journal(void* handle, int32_t* error_code)
{
    if (CloseHandle(handle))
        return SUCCESS;

    *error_code = GetLastError();

    return FAIL_CLOSE;
}

int32_t rvn_write_journal(void* handle, char* buffer, uint64_t size, int64_t offset, int32_t* error_code)
{
    const int WRITE_INCREMENT = 4096;

    assert(size % WRITE_INCREMENT == 0);
    assert((int64_t)buffer % WRITE_INCREMENT == 0);

    OVERLAPPED overlapped;
    memset(&overlapped, 0, sizeof(overlapped));

    overlapped.Offset = (int)(offset & 0xffffffff);
    overlapped.OffsetHigh = (int)(offset >> 32);
    overlapped.hEvent = 0;

    if (WriteFile(handle, buffer, (DWORD)size, NULL, &overlapped))
        return SUCCESS;

    *error_code = GetLastError();

    if (*error_code != ERROR_WORKING_SET_QUOTA)
        return FAIL_WRITE_FILE;

    // this error can happen under low memory conditions, instead of trying to write the whole thing in a single shot
    // we'll write it in 4KB increments. This is likely to be much slower, but failing here will fail the entire DB
    while (size > 0)
    {
        if (0 == WriteFile(handle, buffer, WRITE_INCREMENT, NULL, &overlapped))
        {
            *error_code = GetLastError();
            return FAIL_WRITE_FILE;
        }

        offset += WRITE_INCREMENT;
        buffer += WRITE_INCREMENT;
        size -= WRITE_INCREMENT;

        overlapped.Offset = (int)(offset & 0xffffffff);
        overlapped.OffsetHigh = (int)(offset >> 32);
    }

    return SUCCESS;
}

int32_t rvn_read_journal(char* file_name, void** handle, char* buffer, uint64_t required_size, int64_t offset, uint64_t* actual_size, int32_t* error_code)
{
    int32_t rc;
    HANDLE hfile = *handle;
    if (!hfile)
    {
        hfile = CreateFile(
            file_name,
            GENERIC_READ,
            FILE_SHARE_WRITE | FILE_SHARE_READ | FILE_SHARE_DELETE,
            0,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            0);

        if (hfile == INVALID_HANDLE_VALUE)
        {
            rc = FAIL_OPEN_FILE;
            goto error_cleanup;
        }

        *handle = hfile;
    }

    OVERLAPPED overlapped;
    memset(&overlapped, 0, sizeof(overlapped));
    overlapped.Offset = (int)(offset & 0xffffffff);
    overlapped.OffsetHigh = (int)(offset >> 32);
    overlapped.hEvent = 0;

#pragma warning( push )
#pragma warning( disable : 4244)
#pragma warning( disable : 4242)
    if (0 == ReadFile(hfile, buffer, required_size, (LPDWORD)actual_size, &overlapped))
#pragma warning( pop ) 
    {
        rc = FAIL_READ_FILE;
        goto error_cleanup;
    }

    return SUCCESS;

error_cleanup:
    *error_code = GetLastError();
    return rc;
}

int32_t rvn_truncate_journal(void* handle, uint64_t size, int32_t* error_code)
{
    /*TODO Should size be aligned to 4K?*/
    /*TODO Should check the function doesn't try to make the file bigger?*/
    int32_t rc;
    if (0 == FlushFileBuffers(handle))
    {
        rc = FAIL_FLUSH_FILE;
        goto error_cleanup;
    }

    LARGE_INTEGER distance_to_move;
    distance_to_move.QuadPart = size;
    if (0 == SetFilePointerEx(handle, distance_to_move, NULL, FILE_BEGIN))
    {
        rc = FAIL_SET_FILE_POINTER;
        goto error_cleanup;
    }

    if (0 == SetEndOfFile(handle))
    {
        rc = FAIL_SET_EOF;
        goto error_cleanup;
    }

    return SUCCESS;

error_cleanup:
    *error_code = GetLastError();
    return rc;
}
