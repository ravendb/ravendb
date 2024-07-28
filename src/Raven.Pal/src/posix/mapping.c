#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libgen.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

EXPORT int32_t
rvn_mmap_anonymous(void** address, uint64_t size, int32_t *detailed_error_code){
    void* res = rvn_mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if(res == NULL){
        *detailed_error_code = errno;
        return FAIL_CALLOC;
    }
    *address = res;
    return SUCCESS;
}

EXPORT int32_t
rvn_mumap_anonymous(void* address, uint64_t size, int32_t *detailed_error_code) {
    if(munmap(address, size) == 0){
        return SUCCESS;
    }
    *detailed_error_code = errno;
    return FAIL_CALLOC;
}

EXPORT int32_t
rvn_discard_virtual_memory(void* address, int64_t size, int32_t* detailed_error_code)
{
    int32_t rc = madvise(address, size, MADV_DONTNEED);
    if (rc != 0)
        *detailed_error_code = errno;
    return rc;
}
