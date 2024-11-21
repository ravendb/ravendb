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
    int rc = rvn_init_pager(L"test.db", 1024*64, OPEN_FILE_LOCK_MEMORY | OPEN_FILE_WRITABLE_MAP, &handle, &mem, &wmem, &size, &err);
    void* new_handle;
    rc = rvn_increase_pager_size(handle, 1024 * 1024 * 16, &new_handle, &mem, &wmem, &size, &err);
    int64_t t, p;
    rc = rvn_pager_get_file_size(handle, &t, &p, &err);
}
