#ifndef RVN_H
#define RVN_H

#ifdef __APPLE__
#define EXPORT __attribute__((visibility("default")))
#define PRIVATE __attribute__((visibility("hidden")))
#elif _WIN32
#define EXPORT _declspec(dllexport)
#define PRIVATE
#else
#define EXPORT __attribute__((visibility("default")))
#define PRIVATE __attribute__((visibility("hidden")))
#endif

#include <stdint.h>

typedef int32_t bool;
#define true 1
#define false 0

#define rvn_max(x, y) ((x) >= (y)) ? (x) : (y)
#define rvn_min(x, y) ((x) <= (y)) ? (x) : (y)

enum
{
    OPEN_FILE_NONE = 0,
    OPEN_FILE_TEMPORARY = (1 << 1),
    OPEN_FILE_READ_ONLY = (1 << 2),
    OPEN_FILE_SEQUENTIAL_SCAN = (1 << 3),
    OPEN_FILE_WRITABLE_MAP = (1 << 4),
    OPEN_FILE_ENCRYPTED = (1 << 5),
    OPEN_FILE_LOCK_MEMORY = (1 << 6),
    OPEN_FILE_DO_NOT_CONSIDER_MEMORY_LOCK_FAILURE_AS_CATASTROPHIC_ERROR = (1 << 7),
    OPEN_FILE_COPY_ON_WRITE = (1 << 8),
    OPEN_FILE_DO_NOT_MAP = (1<<9)
};

#define ALLOCATION_GRANULARITY (64*1024)

EXPORT struct SYSTEM_INFORMATION
{
    int32_t page_size;
    int32_t prefetch_status;

    /* can_prefetch => prefetch_status == true */
} SYSTEM_INFORMATION;

EXPORT struct RVN_RANGE_LIST
{
    void *virtual_address;
    size_t number_of_bytes;
} RVN_RANGE_LIST;

EXPORT
int32_t rvn_pager_get_file_handle(
    void *handle,
    void** file_handle,
    int32_t* detailed_error_code);

EXPORT
int32_t rvn_unmap_memory(
    void* mem,
    int32_t *detailed_error_code);

EXPORT
int32_t rvn_map_memory(void* handle,
    int64_t offset,
    int64_t size,
    void** mem,
    int32_t *detailed_error_code);

EXPORT int32_t
rvn_init_pager(const char* filename,
    int64_t initial_file_size,
    int32_t open_flags,
    void** handle,
    void** memory,
    int64_t *memory_size,
    int32_t* detailed_error_code);

EXPORT int32_t
rvn_increase_pager_size(void* handle,
    int64_t new_length,
    void** new_handle,
    void** memory,
    int64_t* memory_size,
    int32_t* detailed_error_code);

EXPORT int32_t
rvn_close_pager(
    void *handle,
    const void* memory,
    int32_t* detailed_error_code);


EXPORT int32_t
rvn_sync_pager(void* handle,
    int32_t* detailed_error_code);

EXPORT uint64_t
rvn_get_current_thread_id(void);

EXPORT int32_t
rvn_write_header(const char *path, void *header, int32_t size, int32_t *detailed_error_code);

EXPORT int32_t
rvn_get_error_string(int32_t error, char *buf, int32_t buf_size, int32_t *special_errno_flags);

EXPORT int32_t
rvn_create_and_mmap64_file(const char *path, int64_t initial_file_size, int32_t flags, void **handle, void **base_addr, int64_t *actual_file_size, int32_t *detailed_error_code);

EXPORT int32_t
rvn_get_system_information(struct SYSTEM_INFORMATION *sys_info, int32_t *detailed_error_code);

EXPORT int32_t
rvn_mmap_dispose_handle(void *handle, int32_t *detailed_error_code);


EXPORT int32_t
rvn_prefetch_ranges(struct RVN_RANGE_LIST *range_list, int32_t count, int32_t *detailed_error_code);


EXPORT int32_t
rvn_open_journal_for_writes(const char *file_name, int32_t transaction_mode, int64_t initial_file_size, int32_t durability_support, void **handle, int64_t *actual_size, int32_t *detailed_error_code);

EXPORT int32_t
rvn_close_journal(void* handle, int32_t* detailed_error_code);

EXPORT int32_t
rvn_write_journal(void* handle, void* buffer, int64_t size, int64_t offset, int32_t* detailed_error_code);

EXPORT int32_t
rvn_open_journal_for_reads(const char *file_name, void **handle, int32_t *detailed_error_code);

EXPORT int32_t
rvn_read_journal(void* handle, void* buffer, int64_t required_size, int64_t offset, int64_t* actual_size, int32_t* detailed_error_code);

EXPORT int32_t
rvn_truncate_journal(void* handle, int64_t size, int32_t* detailed_error_code);

EXPORT int32_t
rvn_discard_virtual_memory(void* address, int64_t size, int32_t* detailed_error_code);

EXPORT int32_t
rvn_test_storage_durability(const char *temp_file_name, int32_t *detailed_error_code);

EXPORT int32_t
rvn_get_path_disk_space(const char * path, uint64_t* total_free_bytes, uint64_t* total_size_bytes, int32_t* detailed_error_code);

/* For internal use: */
PRIVATE int64_t
_nearest_size_to_page_size(int64_t orig_size, int64_t sys_page_size);

#endif
