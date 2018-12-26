#include <windows.h>
#include <assert.h>
#include <fileapi.h>
#include <stdint.h>
#include <rvn.h>
#include <status_codes.h>

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

    if(WriteFile(handle, buffer, (DWORD)size, NULL, &overlapped))
        return SUCCESS;

    *error_code = GetLastError();

    if (*error_code != ERROR_WORKING_SET_QUOTA)
        return FAIL_WRITE_FILE;

    // this error can happen under low memory conditions, instead of trying to write the whole thing in a single shot
    // we'll write it in 4KB increments. This is likely to be much slower, but failing here will fail the entire DB
    while(size > 0)
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
