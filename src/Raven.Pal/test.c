#include "rvn.h"
#include "status_codes.h"
#include <stdio.h>
#include <windows.h>

void main() {
    char buffer[MAX_PATH];
    GetCurrentDirectory(MAX_PATH, buffer);
    printf("%s\n", buffer);
    void* handle;
    void* mem;
    int64_t size;
    int32_t err;
    int rc = rvn_init_pager("test.db", 0, OPEN_FILE_NONE, &handle, &mem, &size, &err);
    rvn_increase_pager_size(handle, 2147483648, &handle, &mem, &size, &err);
}
