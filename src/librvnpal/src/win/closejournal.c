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
