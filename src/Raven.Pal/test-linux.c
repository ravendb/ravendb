#include <stdio.h>
#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

//  The command to build and run this is:
//  zig cc -Wall -O0 -g -fPIC -Iinc -target x86_64-linux-gnu ../../libs/liburing/liburing-2.8.1-x64.a -o test src/shared_all.c src/rvngetpalver.c src/posix/fileutils.c src/posix/geterrorstring.c src/posix/getsysteminformation.c src/posix/journal.c src/posix/mapping.c src/posix/pager.c src/posix/sync.c src/posix/virtualmemory.c src/posix/writefileheader.c src/posix/linuxonly.c test-linux.c
int main()
{
    void *handle;
    void *mem;
    void *wmem;
    int64_t size;
    int64_t start = 0;
    int64_t offset=0;
    int32_t err;

    struct rvn_configuration cfg = {
        .io_ring_queue_size = 16,
        .low_priority_io = true,
        .write_mode = rvn_write_mode_io_ring};
    int32_t ec;
    int32_t rc = rvn_startup_configure(&cfg, &ec);

    rc = rvn_init_pager("/home/ayende/work/ravendb/8.0/src/Raven.Server/bin/release/net10.0/Databases/t/Raven.voron", 1024 * 64, OPEN_FILE_WRITABLE_MAP, &handle, &mem, &wmem, &size, &err);
    
    while(1){
        rc = rvn_pager_get_next_sparse_region(handle, start, &offset, &size, &err);
        if(size == -1 && offset == -1)
            break;
        if(rc != SUCCESS)
            break;
        printf("Hole - Start: %.2f MB, End: %.2f MB, Size: %.2f MB\n", 
               offset / (1024.0 * 1024.0), 
               (offset + size) / (1024.0 * 1024.0), 
               size / (1024.0 * 1024.0));
        start = offset + size;   
    }
    return rc;
}
