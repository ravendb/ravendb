#include <windows.h>
#include <VersionHelpers.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_win.h"

typedef DWORD (WINAPI *pDiscardVirtualMemory)(PVOID, SIZE_T);

PRIVATE pDiscardVirtualMemory 
_discard_virtual_memory_func;

PRIVATE bool
_init_discard_virtual_memory_flag = false;

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
    int32_t rc;
    if (_init_discard_virtual_memory_flag == false)
    {        
        HMODULE handle = GetModuleHandle(TEXT("kernel32.dll"));
        if(handle == NULL)
        {
            rc = FAIL_GET_MODULE_HANDLE;
            goto error_cleanup;
        }
        
        _discard_virtual_memory_func = (pDiscardVirtualMemory) GetProcAddress(
                handle,
                "DiscardVirtualMemory");
        _init_discard_virtual_memory_flag = true;
    }

    if (_discard_virtual_memory_func != NULL)
    {
        /*Return Value of DiscardVirtualMemory - ERROR_SUCCESS(0) if successful; a System Error Code otherwise.*/
        *detailed_error_code = _discard_virtual_memory_func(address, (size_t)size);
        if (*detailed_error_code != 0)
        {
            rc = FAIL_DISCARD_VIRTUAL_MEMORY;
            goto cleanup;
        }
    }

    rc = SUCCESS;
    goto cleanup;
error_cleanup:
    *detailed_error_code = GetLastError();
cleanup:
    return rc;
}
