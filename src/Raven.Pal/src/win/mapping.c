#include <windows.h>
#include <VersionHelpers.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

EXPORT int32_t
rvn_mmap_anonymous(void** address, uint64_t size, int32_t *detailed_error_code){
    void* res = VirtualAlloc(NULL, size, MEM_COMMIT,PAGE_READWRITE);
    if(res == NULL){
        *detailed_error_code = GetLastError();
        return FAIL_CALLOC;
    }
    *address = res;
    return SUCCESS;
}

EXPORT int32_t
rvn_mumap_anonymous(void* address,  uint64_t size, int32_t *detailed_error_code) {
    (void)size; // unused
    if(VirtualFree(address, 0, MEM_RELEASE)){
        return SUCCESS;
    }
    *detailed_error_code = GetLastError();
    return FAIL_CALLOC;
}

EXPORT int32_t
rvn_discard_virtual_memory(void* address, int64_t size, int32_t* detailed_error_code)
{    
    // Calling DiscardVirtualMemory on memory mapped files result in ERROR_USER_MAPPED_FILE
    // on Windows, so we aren't going to try (this is meant as a hint to the OS, not a hard 
    // requirement  anyway)
    return SUCCESS;
}
