#include <windows.h>
#include <stdint.h>
#include <rvn.h>
#include <status_codes.h>

int32_t open_journal(char* file_name, int64_t file_size, void** handle,  uint32_t* error_code)
{
    int32_t rc;
    HANDLE hFile = CreateFile(
        file_name,
        GENERIC_WRITE,
        FILE_SHARE_READ,
        NULL,
        OPEN_ALWAYS,
        FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH,
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
