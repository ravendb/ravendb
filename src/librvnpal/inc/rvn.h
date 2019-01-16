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

#define max(x, y) ((x) >= (y)) ? (x) : (y)
#define min(x, y) ((x) <= (y)) ? (x) : (y)

#define ALLOCATION_GRANULARITY (64*1024)

EXPORT struct SYSTEM_INFORMATION
{
    int32_t page_size;
    int32_t prefetch_status;

    /* can_prefetch => prefetch_status == true */
};

EXPORT struct RVN_RANGE_LIST
{
    void *virtual_address;
    int32_t number_of_bytes;
};

EXPORT uint64_t
rvn_get_current_thread_id(void);

EXPORT int32_t
rvn_write_header(const char *path, void *header, int32_t size, int32_t *detailed_error_code);

EXPORT int32_t
rvn_get_error_string(int32_t error, char *buf, int32_t buf_size, int32_t *special_errno_flags);

EXPORT int32_t
rvn_create_and_mmap64_file(const char *path, int64_t initial_file_size, int32_t flags, void **handle, void **base_addr, int64_t *actual_file_size, int32_t *detailed_error_code);

EXPORT int32_t
rvn_prefetch_virtual_memory(void *virtualAddress, int64_t length, int32_t *detailed_error_code);

EXPORT int32_t
rvn_get_system_information(struct SYSTEM_INFORMATION *sys_info, int32_t *detailed_error_code);

EXPORT int32_t
rvn_memory_sync(void *address, int64_t size, int32_t *detailed_error_code);

EXPORT int32_t
rvn_dispose_handle(const char *filepath, void *handle, int32_t delete_on_close, int32_t *detailed_error_code);

EXPORT int32_t
rvn_unmap(void *address, int64_t size, int32_t delete_on_close, int32_t *detailed_error_code);

EXPORT int32_t
rvn_prefetch_ranges(struct RVN_RANGE_LIST *range_list, int32_t count, int32_t *detailed_error_code);

EXPORT int32_t
rvn_protect_range(void *start_address, int64_t size, int32_t protection, int32_t *detailed_error_code);

EXPORT int32_t
rvn_allocate_more_space(const char *filename, int64_t new_length_after_adjustment, void *handle, int32_t flags, void **new_address, int32_t *detailed_error_code);

EXPORT int32_t
rvn_open_journal_for_writes(const char *file_name, int32_t transaction_mode, int64_t initial_file_size, void **handle, int64_t *actual_size, int32_t *detailed_error_code);

EXPORT int32_t
rvn_close_journal(void* handle, int32_t* detailed_error_code);

EXPORT int32_t
rvn_write_journal(void* handle, void* buffer, int64_t size, int64_t offset, int32_t* detailed_error_code);

EXPORT int32_t
rvn_open_journal_for_reads(const char *file_name, void **handle, int32_t *detailed_error_code);

EXPORT int32_t
rvn_read_journal(void* handle, void* buffer, int64_t required_size, int64_t offset, int64_t* actual_size, int32_t* detailed_error_code);

EXPORT int32_t
rvn_truncate_journal(const char* file_name, void* handle, int64_t size, int32_t* detailed_error_code);

/* For internal use: */
PRIVATE int64_t
_nearest_size_to_page_size(int64_t orig_size, int64_t sys_page_size);

PRIVATE int32_t
_ensure_path_exists(const char* path);

PRIVATE int32_t
_open_file_to_read(const char* file_name, void** handle, int32_t* detailed_error_code);

PRIVATE int32_t
_read_file(void* handle, void* buffer, int64_t required_size, int64_t offset, int64_t* actual_size, int32_t* detailed_error_code);

PRIVATE int32_t
_resize_file(void *handle, int64_t size, int32_t *detailed_error_code);
#endif
