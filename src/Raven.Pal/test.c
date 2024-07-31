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
    void* wmem;
    int64_t size;
    int32_t err;
    int rc = rvn_init_pager("test.db", 0, OPEN_FILE_TEMPORARY | OPEN_FILE_ENCRYPTED | OPEN_FILE_WRITABLE_MAP | OPEN_FILE_LOCK_MEMORY, &handle, &mem, &wmem, &size, &err);
    rvn_increase_pager_size(handle, 2147483648, &handle, &mem, &wmem, &size, &err);
}
