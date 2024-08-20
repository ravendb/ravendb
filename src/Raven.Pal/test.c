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
    int rc = rvn_init_pager(L"test.db", 1024*1024*64, OPEN_FILE_NONE, &handle, &mem, &wmem, &size, &err);
    rc = rvn_pager_set_sparse_region(handle, 1024*1024, 1024*1024*8, &err);
    int64_t t, p;
    rc = rvn_pager_get_file_size(handle, &t, &p, &err);
}
