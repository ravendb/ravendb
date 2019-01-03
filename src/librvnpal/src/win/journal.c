#include <windows.h>
#include <stdint.h>
#include <rvn.h>
#include <status_codes.h>

int32_t close_journal(void* handle, uint32_t* error_code)
{
    if (CloseHandle(handle))
        return SUCCESS;

    *error_code = GetLastError();

    return FAIL_CLOSE_FILE;
}


int32_t open_journal(char* file_name, int64_t file_size, void** handle,  uint32_t* error_code)
{
    DWORD flags = FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;

    if (flags & JOURNAL_MODE_DANGER)
        flags = 0;

    int32_t rc;
    HANDLE hFile = CreateFile(
        file_name,
        GENERIC_WRITE,
        FILE_SHARE_READ,
        NULL,
        OPEN_ALWAYS,
        flags,
        NULL);

    if(hFile == INVALID_HANDLE_VALUE)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

    LARGE_INTEGER size;
    size.QuadPart = file_size;
    if(!SetFilePointerEx(hFile, size, NULL, FILE_BEGIN))
    {
        rc = FAIL_SEEK_FILE;
        goto error_cleanup;
    }

    if(!SetEndOfFile(hFile))
    {
       rc = FAIL_ALLOC_FILE;
       goto error_cleanup;
    }

    *handle = hFile;

    return SUCCESS;

error_cleanup:
    *error_code = GetLastError();
    if(hFile != INVALID_HANDLE_VALUE)
        CloseHandle(hFile);
    return rc;
}


int32_t write_journal(void* handle, char* buffer, uint64_t size, int64_t offset_in_file, uint32_t* error_code)
{
    const int WRITE_INCREMENT = 4096;

    assert(size % WRITE_INCREMENT == 0);
    assert((int64_t)buffer % WRITE_INCREMENT == 0);

    OVERLAPPED overlapped;
    memset(&overlapped, 0, sizeof(overlapped));

    overlapped.Offset = (int)(offset_in_file & 0xffffffff);
    overlapped.OffsetHigh = (int)(offset_in_file >> 32);
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
        if (!WriteFile(handle, buffer, WRITE_INCREMENT, NULL, &overlapped))
        {
            *error_code = GetLastError();
            return FAIL_WRITE_FILE;
        }

        offset_in_file += WRITE_INCREMENT;
        buffer += WRITE_INCREMENT;
        size -= WRITE_INCREMENT;

        overlapped.Offset = (int)(offset_in_file & 0xffffffff);
        overlapped.OffsetHigh = (int)(offset_in_file >> 32);
    }

    return SUCCESS;
}
