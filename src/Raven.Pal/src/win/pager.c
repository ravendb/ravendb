// required to enable io ring support
#define NTDDI_VERSION NTDDI_WIN10_NI

#include <windows.h>
#include <VersionHelpers.h>
#include <stdio.h>
#include <ioringapi.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_win.h"

#ifdef _MSC_VER
static __forceinline int clzl(uint64_t x)
{
    unsigned long r = 0;
    BitScanReverse64(&r, x);
    return (int)(r ^ 63);
}
#else
__forceinline int clzl(uint64_t x)
{
    return __builtin_clzl(x);
}
#endif

#define IO_RING_SIZE (256)

// This state is shared across all instances of the pager for a particular file
struct handle_global_state
{
    // This lock handles:
    // * Writing to the file
    // * Extending the file and creating new handle
    // * Closing the handle
    // 
    // We explicitly want to deny concurrent writes to the file, because:
    // * Voron doesn't need that
    // * We want to use io_ring, which requires single threaded access to the ring
    // * Avoid race conditions between writes extending the file & writes to the file past the eof
    //
    // Voron already ensures that you are either single threaded when writing or extending the file.
    // There is a potential race between writes & closing a file handle, but that is likely to be rare
    // and will usually only block the finalizer for a single write duration
    CRITICAL_SECTION lock;

    uint32_t ref_count;
    HIORING io_ring;
    int32_t open_flags;
    char* file_path;
};

// This state represent a single handle to the pager on a file
// multiple such instances may exists at the same time
struct handle
{
    HANDLE file_handle;
    HANDLE file_mapping_handle;
    void *read_address;
    void *write_address;
    int64_t allocation_size;
    int32_t status_flags;
    int64_t locked_memory;
    struct handle_global_state* global_state;
};

extern MemoryLockCallback g_locked_memory_callback;
extern RecoveryMemoryLockFailureCallback g_recovery_memory_lock_failure_callback;


bool _IsIoRingSupported() {
    IORING_CAPABILITIES capabilities; 
    return IsWindowsVersionOrGreater(10, 0, 22000) && 
        SUCCEEDED(QueryIoRingCapabilities(&capabilities)) &&
        capabilities.MaxCompletionQueueSize != IORING_VERSION_INVALID;
}

uint64_t _GetNearestFileSize(uint64_t needed_size)
{
    const uint64_t POWER_OF_TWO_THRESHOLD = (uint64_t)512 * 1024ul * 1024ul; // 512MB
    if (needed_size == 0)
    {
        return (uint64_t)1024 * 1024;
    }
    if (needed_size < POWER_OF_TWO_THRESHOLD)
    {
        int32_t idx = clzl(needed_size);
        if (idx)
        {
            return (uint64_t)1 << (64 - idx);
        }
        return 1024 * 1024;
    }
    const uint64_t ONE_GB = (uint64_t)1024 * 1024 * 1024;
    // if it is over 0.5 GB, then we grow at 1 GB intervals
    return (needed_size + ONE_GB - 1) & ~ONE_GB;
}

int32_t rvn_lock_memory(struct handle* handle, void *mem, int64_t size, int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    if (sizeof(SIZE_T) == 4)
    {
        if (size >= INT32_MAX) // we won't uspport large values on 32 bits
        {
            rc = FAIL_SIZE_INVALID_32_BITS;
            goto Exit;
        }
    }
    if (size <= 0)
    {
        rc = FAIL_SIZE_NEGATIVE_OR_ZERO;
        goto Exit;
    }

    for (int i = 0; i < 10; ++i)
    {
        if (VirtualLock(mem, (SIZE_T)size) ||
            handle->global_state->open_flags & OPEN_FILE_DO_NOT_CONSIDER_MEMORY_LOCK_FAILURE_AS_CATASTROPHIC_ERROR)
        {
            // note that we *explicitly* account for locked memory even if we failed to do so
            // if we don't consider this as catastrophic error, because otherwise accounting 
            // for its removal is really complicated
            g_locked_memory_callback(size, handle->global_state->file_path);
            rc = SUCCESS;
            break;
        }

        if (!g_recovery_memory_lock_failure_callback(size, handle->global_state->file_path))
        {
            rc = FAIL_LOCK_MEMORY;
            break;
        }
    }
Exit:
    *detailed_error_code = GetLastError();
    return rc;
}

EXPORT int32_t
rvn_pager_get_file_size(void* handle,
    int64_t* total_size,
    int64_t* phyiscal_size,
    int32_t* detailed_error_code)
{
   struct handle *handle_ptr = handle;
   FILE_STANDARD_INFO standard_info = {0};
   
   if(!GetFileInformationByHandleEx(handle_ptr->file_handle, FileStandardInfo , &standard_info, sizeof(FILE_STANDARD_INFO)))
   {
         *detailed_error_code = GetLastError();
         return FAIL_GET_FILE_SIZE;
   }
   *phyiscal_size = standard_info.AllocationSize.QuadPart;
   *total_size = standard_info.EndOfFile.QuadPart;
   return SUCCESS;
}

EXPORT int32_t
rvn_pager_set_sparse_region(void* handle,
    int64_t offset,
    int64_t size,
    int32_t* detailed_error_code)
{
    struct handle *handle_ptr = handle;
    if((handle_ptr->status_flags & PAGER_STATUS_SPARSE) == 0)
    {
        if(handle_ptr->status_flags & PAGER_STATUS_SPARSE_NOT_SUPPORTED)
            return FAIL_SPARSE_NOT_SUPPORTED;

        DWORD fileSystemFlags;
        if(!GetVolumeInformationByHandleW(handle_ptr->file_handle, 
                NULL, 0, NULL, NULL, &fileSystemFlags, NULL, 0))
        {
            *detailed_error_code = GetLastError();
            return FAIL_GET_VOLUME_DETAILS;
        }
        if((fileSystemFlags & FILE_SUPPORTS_SPARSE_FILES) == 0)
        {
            handle_ptr->status_flags |= PAGER_STATUS_SPARSE_NOT_SUPPORTED;
            return FAIL_SPARSE_NOT_SUPPORTED;
        }
        if (!DeviceIoControl(handle_ptr->file_handle,
            FSCTL_SET_SPARSE,
            NULL,
            0,
            NULL,
            0,
            NULL,
            NULL))
        {
            *detailed_error_code = GetLastError();
            return FAIL_SET_SPARSE;
        }
        handle_ptr->status_flags |= PAGER_STATUS_SPARSE;
    }
    DWORD dwTemp;
    FILE_ZERO_DATA_INFORMATION fzdi;
    fzdi.FileOffset.QuadPart = offset;
    fzdi.BeyondFinalZero.QuadPart = offset + size;
    if(!DeviceIoControl(handle_ptr->file_handle,
        FSCTL_SET_ZERO_DATA,
         &fzdi,
        sizeof(fzdi),
        NULL,
        0,
        &dwTemp,
        NULL))
    {
        *detailed_error_code = GetLastError();
        return FAIL_SET_SPARSE_RANGE;
    }
    return SUCCESS;
}


EXPORT
int32_t rvn_unmap_memory(
    void* handle,
    void *mem,
    int64_t size,
    int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    *detailed_error_code = 0;
    if (!UnmapViewOfFile(mem))
    {
        *detailed_error_code = GetLastError();
        return FAIL_UNMAP_VIEW_OF_FILE;
    }
    if(handle_ptr->global_state->open_flags & OPEN_FILE_LOCK_MEMORY)
    {
        g_locked_memory_callback(-size, handle_ptr->global_state->file_path);
    }
    return SUCCESS;
}
EXPORT
int32_t rvn_map_memory(void *handle,
                       int64_t offset,
                       int64_t size,
                       void **mem,
                       int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    if (sizeof(void *) == 4)
    {
        if (size > INT32_MAX)
        {
            rc = FAIL_SIZE_INVALID_32_BITS;
            goto Exit;
        }
    }
    if (size <= 0)
    {
        rc = FAIL_SIZE_NEGATIVE_OR_ZERO;
        goto Exit;
    }

    struct handle *handle_ptr = handle;
    DWORD dwDesiredAccess = (handle_ptr->global_state->open_flags & OPEN_FILE_WRITABLE_MAP) ? 
        (FILE_MAP_READ | FILE_MAP_WRITE) : 
        ((handle_ptr->global_state->open_flags & OPEN_FILE_COPY_ON_WRITE) ? FILE_MAP_COPY : FILE_MAP_READ);

    *mem = MapViewOfFile(handle_ptr->file_mapping_handle,
                         dwDesiredAccess,
                         offset >> 32,
                         (DWORD)offset,
                         (SIZE_T)size);

    if (*mem == NULL)
    {
        rc = FAIL_MAP_VIEW_OF_FILE;
        goto Exit;
    }

    if (handle_ptr->global_state->open_flags & OPEN_FILE_LOCK_MEMORY)
    {
        // intentionally returning the error code & rc from the lock_memory call
        int mem_lock_rc = rvn_lock_memory(handle_ptr, *mem, size, detailed_error_code);
        if (rc != SUCCESS)
        {
            UnmapViewOfFile(*mem);
            *mem = NULL;
            return mem_lock_rc;
        }
    }

Exit:
    *detailed_error_code = GetLastError();
    return rc;
}


char* ConvertLPCWSTRToUTF8(LPCWSTR lpWideCharStr)
{
    int utf8Size = WideCharToMultiByte(CP_UTF8, 0, lpWideCharStr, -1, NULL, 0, NULL, NULL);
    if (utf8Size == 0) {
        return NULL;
    }

    char* utf8Str = calloc(utf8Size, sizeof(char));
    if (utf8Str == NULL) {
        return NULL;
    }
    int result = WideCharToMultiByte(CP_UTF8, 0, lpWideCharStr, -1, utf8Str, utf8Size, NULL, NULL);
    if (result == 0) {
        free(utf8Str);
        return NULL;
    }
    return utf8Str;
}

void delete_global_state(struct handle_global_state* global_state)
{
    if(global_state == NULL)
    {
        return;
    }
    if (global_state->io_ring != NULL)
    {
        CloseIoRing(global_state->io_ring);
    }
    if (global_state->file_path != NULL)
    {
        free(global_state->file_path);
    }
    DeleteCriticalSection(&global_state->lock);
    free(global_state);
}


int32_t _open_pager_file(HANDLE h,
                         struct handle_global_state* global_state,
                         int64_t req_file_size,
                         void **handle,
                         void **memory,
                         void** writable_memory,
                         int64_t *memory_size,
                         int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    HANDLE m = INVALID_HANDLE_VALUE;
    struct handle *handle_ptr = NULL;
    void* mem = NULL;
    void* wmem = NULL;

    handle_ptr = calloc(1, sizeof(struct handle));
    if (handle_ptr == NULL)
    {
        rc = FAIL_NOMEM;
        goto Error;
    }
    handle_ptr->global_state = global_state;


    LARGE_INTEGER file_size;
    if (!GetFileSizeEx(h, &file_size) || file_size.QuadPart < 0)
    {
        rc = FAIL_GET_FILE_SIZE;
        goto Error;
    }
    int64_t min_file_size = rvn_max(
        (req_file_size + ALLOCATION_GRANULARITY - 1) & ~(ALLOCATION_GRANULARITY - 1),
        ALLOCATION_GRANULARITY);

    if (min_file_size > file_size.QuadPart && !(global_state->open_flags & OPEN_FILE_READ_ONLY))
    {
        file_size.QuadPart = min_file_size;
        rc = _pre_allocate_file(h, min_file_size, detailed_error_code);
        if(rc != SUCCESS)
            goto Error;
    }
    else if( file_size.QuadPart == 0 && (global_state->open_flags & OPEN_FILE_READ_ONLY))
    {
        // we allow opening zero len files with read only mode, but don't try to map them
        global_state->open_flags |= OPEN_FILE_DO_NOT_MAP;

        handle_ptr->file_handle = h;
        handle_ptr->file_mapping_handle = INVALID_HANDLE_VALUE;
        *memory_size = 0;
        *handle = handle_ptr;
        return SUCCESS;
    }

    DWORD flProtect = (global_state->open_flags  & OPEN_FILE_WRITABLE_MAP) ? PAGE_READWRITE : PAGE_READONLY;
    m = CreateFileMapping(h, NULL, flProtect, 0, 0, NULL);

    if (m == NULL)
    {
        m = INVALID_HANDLE_VALUE;
        rc = FAIL_MMAP64;
        goto Error;
    }

    if ((global_state->open_flags & OPEN_FILE_DO_NOT_MAP))
    {
        handle_ptr->file_handle = h;
        handle_ptr->file_mapping_handle = m;
        *memory_size = file_size.QuadPart;
        *handle = handle_ptr;
        return SUCCESS;
    }

    DWORD dwDesiredAccess = ((global_state->open_flags & OPEN_FILE_COPY_ON_WRITE) ? FILE_MAP_COPY : FILE_MAP_READ);

    mem = MapViewOfFile(m, dwDesiredAccess, 0, 0, 0);
    if (mem == NULL)
    {
        rc = FAIL_MAP_VIEW_OF_FILE;
        goto Error;
    }

    if (global_state->open_flags & OPEN_FILE_WRITABLE_MAP)
    {
        wmem = MapViewOfFile(m, FILE_MAP_WRITE, 0, 0, 0);
        if (wmem == NULL)
        {
            rc = FAIL_MAP_VIEW_OF_FILE;
            goto Error;
        }
    }

    CloseHandle(m);

    m = INVALID_HANDLE_VALUE;

    if (global_state->open_flags & OPEN_FILE_LOCK_MEMORY &&
        // We map the memory twice if we have WRITE access, but in phsyical memory, it ends up being the same
        // thing, so we only lock it once. 
        rvn_lock_memory(handle_ptr, mem, file_size.QuadPart, detailed_error_code) != SUCCESS)
    {
        
        rc = FAIL_LOCK_MEMORY;
        goto Error;
    }

    handle_ptr->file_handle = h;
    handle_ptr->read_address = mem;
    handle_ptr->write_address = wmem;
    handle_ptr->allocation_size = file_size.QuadPart;
    handle_ptr->file_mapping_handle = INVALID_HANDLE_VALUE;
    *handle = handle_ptr;
    *memory = mem;
    *writable_memory = wmem;
    *memory_size = file_size.QuadPart;
    return SUCCESS;

Error:
    *detailed_error_code = GetLastError();
    if (mem != NULL)
    {
        UnmapViewOfFile(mem);
    }
    if(wmem != NULL)
    {
        UnmapViewOfFile(wmem);
    }
    if(m != INVALID_HANDLE_VALUE)
    {
        CloseHandle(m);
    }
    free(handle_ptr);
    return rc;
}

EXPORT int32_t
rvn_init_pager(const char *filename,
               int64_t initial_file_size,
               int32_t open_flags,
               void **handle,
               void **memory,
               void** writable_memory,
               int64_t *memory_size,
               int32_t *detailed_error_code)
{
    *memory_size = 0;
    *memory = NULL;
    *handle = NULL;
    HANDLE h = INVALID_HANDLE_VALUE;

    int32_t rc = -1;

    DWORD dwDesiredAccess = ((open_flags & OPEN_FILE_READ_ONLY) | (open_flags & OPEN_FILE_COPY_ON_WRITE)) ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE;
    DWORD dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
    dwFlagsAndAttributes |= open_flags & OPEN_FILE_TEMPORARY ? FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE : 0;
    dwFlagsAndAttributes |= open_flags & OPEN_FILE_SEQUENTIAL_SCAN ? FILE_FLAG_SEQUENTIAL_SCAN : FILE_FLAG_RANDOM_ACCESS;
    dwFlagsAndAttributes |= open_flags & OPEN_FILE_READ_ONLY ? FILE_ATTRIBUTE_READONLY : 0;

    rvn_write_mode write_mode = _get_writer_mode();
    if(write_mode == rvn_write_mode_mmap)
    {
        open_flags |= OPEN_FILE_WRITABLE_MAP;
    }

    struct handle_global_state* global_state = calloc(1, sizeof(struct handle_global_state));
    if(global_state == NULL)
    {
        *detailed_error_code = GetLastError();
        return FAIL_NOMEM;
    }
    global_state->open_flags = open_flags;
    global_state->ref_count = 1;
    InitializeCriticalSection(&global_state->lock);
    LPCWSTR file_path_unicode = (LPCWSTR)filename;
    global_state->file_path = ConvertLPCWSTRToUTF8(file_path_unicode);
    if (global_state->file_path == NULL)
    {
        *detailed_error_code = GetLastError();
        rc = FAIL_NOMEM;
        goto Error;
    }
    if (_IsIoRingSupported()) {
        // For writable maps, we don't need to create an io ring
        if((open_flags & OPEN_FILE_WRITABLE_MAP) == 0)
        {
            IORING_CREATE_FLAGS flags = { 0 };
            HRESULT hr = CreateIoRing(IORING_VERSION_3, flags, IO_RING_SIZE, IO_RING_SIZE * 2, &global_state->io_ring);
            if (FAILED(hr)) {
                *detailed_error_code = hr;
                rc = FAIL_CREATE_IO_RING;
                goto Error;
            }
        }
    } 
    else if(write_mode == rvn_write_mode_io_ring)
    {
        *detailed_error_code = ERROR_NOT_SUPPORTED;
        rc = FAIL_CREATE_IO_RING;
        goto Error;
    }
    h = CreateFileW(file_path_unicode, dwDesiredAccess,
                          FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
                          NULL, OPEN_ALWAYS, dwFlagsAndAttributes, NULL);
    if (h == INVALID_HANDLE_VALUE)
    {
        *detailed_error_code = GetLastError();
        rc = FAIL_OPEN_FILE;
        goto Error;
    }
  
    rc = _open_pager_file(h, global_state, initial_file_size, handle, memory, writable_memory, memory_size, detailed_error_code);
    if (rc == SUCCESS)
    {
        return SUCCESS;
    }

Error:
    if(h != INVALID_HANDLE_VALUE)
    {
        CloseHandle(h);
    }
    delete_global_state(global_state);
    return rc;
}

int32_t
rvn_increase_pager_size(void *handle,
                        int64_t new_length,
                        void **new_handle,
                        void **memory,
                        void **writable_memory,
                        int64_t *memory_size,
                        int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    *memory = NULL;
    *memory_size = 0;
    HANDLE h;
    if (!DuplicateHandle(GetCurrentProcess(),
                         handle_ptr->file_handle,
                         GetCurrentProcess(),
                         &h,
                         0,
                         FALSE,
                         DUPLICATE_SAME_ACCESS))
    {
        *detailed_error_code = GetLastError();
        return FAIL_DUPLICATE_HANDLE;
    }

    EnterCriticalSection(&handle_ptr->global_state->lock);
    int32_t rc = _open_pager_file(h, 
        handle_ptr->global_state, new_length, 
        new_handle, memory, writable_memory,
        memory_size, detailed_error_code);
    if(rc == SUCCESS)
    {
        handle_ptr->global_state->ref_count++;   
    }
    LeaveCriticalSection(&handle_ptr->global_state->lock);
    if(rc != SUCCESS)
    {
        CloseHandle(h);
    }
    return rc;
}

EXPORT int32_t
rvn_close_pager(
    void *handle,
    int32_t *detailed_error_code)
{
    if(handle == NULL)
    {
        return FAIL_INVALID_HANDLE;
    }
    struct handle *handle_ptr = handle;
    *detailed_error_code = 0;
    int rc = SUCCESS;
    if (!(handle_ptr->global_state->open_flags & OPEN_FILE_DO_NOT_MAP))
    {
        if(handle_ptr->global_state->open_flags & OPEN_FILE_LOCK_MEMORY)
        {
            g_locked_memory_callback(-handle_ptr->allocation_size, handle_ptr->global_state->file_path);
        }

        if (!UnmapViewOfFile(handle_ptr->read_address))
        {
            *detailed_error_code = GetLastError();
            rc = FAIL_MAP_VIEW_OF_FILE;
        }
        if(handle_ptr->global_state->open_flags & OPEN_FILE_WRITABLE_MAP)
        {
            if (!UnmapViewOfFile(handle_ptr->write_address))
            {
                rc = FAIL_MAP_VIEW_OF_FILE;
                if (*detailed_error_code == 0)
                    *detailed_error_code = GetLastError();
            }
        }
    }
    else // OPEN_FILE_DO_NOT_MAP
    {
        if(!CloseHandle(handle_ptr->file_mapping_handle))
        {
            if (*detailed_error_code == 0)
                *detailed_error_code = GetLastError();
            if (rc == SUCCESS)
                rc = FAIL_CLOSE;
        }
    }
    if (!CloseHandle(handle_ptr->file_handle))
    {
        if (*detailed_error_code == 0)
            *detailed_error_code = GetLastError();
        if (rc == SUCCESS)
            rc = FAIL_CLOSE;
    }
    EnterCriticalSection(&handle_ptr->global_state->lock);
    uint32_t refs = --handle_ptr->global_state->ref_count;
    LeaveCriticalSection(&handle_ptr->global_state->lock);

    if(refs == 0)
    {
        // here we _know_ we are the only ones
        delete_global_state(handle_ptr->global_state);
    }
    free(handle_ptr);
    return rc;
}

EXPORT int32_t
rvn_sync_pager(void *handle,
               int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    if (!FlushFileBuffers(handle_ptr->file_handle))
    {
        *detailed_error_code = GetLastError();
        return FAIL_SYNC_FILE;
    }
    return SUCCESS;
}

EXPORT
int32_t rvn_pager_get_file_handle(
    void *handle,
    void **file_handle,
    int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    HANDLE h;
    if (!DuplicateHandle(GetCurrentProcess(),
                         handle_ptr->file_handle,
                         GetCurrentProcess(),
                         &h,
                         0,
                         FALSE,
                         DUPLICATE_SAME_ACCESS))
    {
        *file_handle = NULL;
        *detailed_error_code = GetLastError();
        return FAIL_DUPLICATE_HANDLE;
    }
    *file_handle = h;
    return SUCCESS;
}

int32_t rvn_write_file_io(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
)
{
    struct handle *handle_ptr = handle;
    OVERLAPPED ov = {0};
    for(int32_t i = 0; i < count; i++)
    {
        int64_t offset = buffers[i].page_num * VORON_PAGE_SIZE;
        int64_t size = (int64_t)buffers[i].count_of_pages * VORON_PAGE_SIZE;
        while(size > 0)
        {
            ov.Offset = (DWORD)offset;
            ov.OffsetHigh = (DWORD)(offset >> 32);

            int32_t size_to_write = (int32_t)(rvn_min(size, INT32_MAX));
            if(WriteFile(handle_ptr->file_handle, buffers[i].ptr, size_to_write, NULL, &ov))
            {
                offset += size_to_write;
                size -= size_to_write;
                continue;
            }    
            *detailed_error_code = GetLastError();
            return FAIL_WRITE_FILE;
        }
    }
    return SUCCESS;
}

int32_t _submit_and_wait(
    HIORING io_ring,
    int32_t count,
    int32_t* detailed_error_code)
{
    HRESULT hr = SubmitIoRing(io_ring, count, INFINITE, NULL);
    if (FAILED(hr))
    {
        *detailed_error_code = hr;
        return FAIL_IO_RING_SUBMIT;
    }
    IORING_CQE cqe;
    for(int i = 0; i < count; i++)
    {
        hr = PopIoRingCompletion(io_ring, &cqe);
        if (hr != S_OK)
        {
            *detailed_error_code = hr;
            return FAIL_IO_RING_NO_RESULT;
        }
        if(FAILED(cqe.ResultCode))
        {
            *detailed_error_code = cqe.ResultCode;
            return FAIL_IO_RING_WRITE_RESULT;
        }
    }
    return SUCCESS;
}

int32_t rvn_write_io_ring(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
)
{
    struct handle *handle_ptr = handle;
    IORING_HANDLE_REF file_handle_ref =  IoRingHandleRefFromHandle(handle_ptr->file_handle);
    uint32_t submitted = 0;
    HRESULT hr;
    int32_t rc = SUCCESS;

    EnterCriticalSection(&handle_ptr->global_state->lock);
    for(int32_t i = 0; i < count; i++)
    {
        IORING_BUFFER_REF buf = IoRingBufferRefFromPointer(buffers[i].ptr);
        int64_t offset = buffers[i].page_num * VORON_PAGE_SIZE;
        int64_t size = (int64_t)buffers[i].count_of_pages * VORON_PAGE_SIZE;
        while(size > 0)
        {
            int32_t size_to_write = (int32_t)(rvn_min(size, INT32_MAX));
            hr = BuildIoRingWriteFile(handle_ptr->global_state->io_ring, file_handle_ref, 
                buf, size_to_write, offset, FILE_WRITE_FLAGS_NONE, 0,
                IOSQE_FLAGS_NONE);
            size -= size_to_write;
            offset += size_to_write;

            if (FAILED(hr))
            {
                *detailed_error_code = hr;
                return FAIL_IO_RING_WRITE;
            }
            if(++submitted >= IO_RING_SIZE)
            {  
                rc = _submit_and_wait(handle_ptr->global_state->io_ring, submitted, detailed_error_code);
                if(rc != SUCCESS)
                {
                    break;
                }
                submitted = 0;
            }
        }
    }
    if(rc == SUCCESS)
    {
        rc =  _submit_and_wait(handle_ptr->global_state->io_ring, submitted, detailed_error_code);
    }
    LeaveCriticalSection(&handle_ptr->global_state->lock);
    return rc;
}


int32_t rvn_write_mmap(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
)
{
    struct handle *handle_ptr = handle;
    for(int32_t i = 0; i < count; i++)
    {
        int64_t offset = buffers[i].page_num * VORON_PAGE_SIZE;
        int64_t size = (int64_t)buffers[i].count_of_pages * VORON_PAGE_SIZE;
        memcpy((char*)handle_ptr->write_address + offset, buffers[i].ptr, (size_t)size);
    }
    if(!FlushViewOfFile(handle_ptr->write_address, 0))
    {
        *detailed_error_code = GetLastError();
        return FAIL_FLUSH_VIEW_OF_FILE;
    }
    return SUCCESS;
}


int32_t rvn_write_invalid_setup(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
)
{
    *detailed_error_code = ERROR_NOT_SUPPORTED;
    return FAIL_INVALID_HANDLE;
}


EXPORT
rvn_writer rvn_get_writer(void* handle)
{
    struct handle *handle_ptr = handle;
    if(handle_ptr->write_address)
        return rvn_write_mmap;
    if(handle_ptr->global_state->io_ring)
        return rvn_write_io_ring;
        
    rvn_write_mode mode = _get_writer_mode();
    switch (mode)
    {
        case rvn_write_mode_io_ring:
            return rvn_write_invalid_setup;

        case rvn_write_mode_mmap:
            return rvn_write_invalid_setup;
            
        case rvn_write_mode_vectored_file_io:
        case rvn_write_mode_file_io:
          default:
            return rvn_write_file_io;
    }
}
