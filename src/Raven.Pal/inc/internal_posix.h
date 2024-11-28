#ifndef INTERNALPOSIX_H
#define INTERNALPOSIX_H

#include <pthread.h>

#ifdef __APPLE__
#define rvn_mmap mmap
#define rvn_ftruncate ftruncate
#define rvn_pread pread
#define rvn_pwrite pwrite
#define rvn_pwritev pwritev
#define O_DIRECT 0
#define O_LARGEFILE 0

struct io_uring
{
    int ring_fd;
};

#else

#include "liburing.h"

#define rvn_mmap mmap64
#define rvn_ftruncate ftruncate64
#define rvn_pread pread64
#define rvn_pwrite pwrite64
#define rvn_pwritev pwritev64
#endif


#if defined(__unix__) || defined(__APPLE__)


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
    pthread_mutex_t lock;

    uint32_t ref_count;
    struct io_uring ring;
    int32_t open_flags;
    int32_t status_flags;
    char* file_path;
};

struct handle
{
    struct handle_global_state *global_state;
    void *read_address;
    void *write_address;
    uint64_t allocation_size;
    int file_fd;
};


PRIVATE
int32_t _setup_io_ring(struct handle_global_state *global_state, int32_t *detailed_error_code);

PRIVATE 
void _close_io_ring(struct handle_global_state *global_state);

PRIVATE
int32_t rvn_write_io_ring(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
);

PRIVATE
int32_t rvn_write_vectored_file_io(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
);

PRIVATE
int32_t rvn_write_file_io(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
);

PRIVATE
int32_t rvn_write_invalid_setup(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
);


PRIVATE
int32_t rvn_write_mmap(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
);


PRIVATE
bool _io_ring_supported();

PRIVATE int32_t /* different impl for linux and mac */
_flush_file(int32_t fd);

PRIVATE int32_t /* different impl for linux and mac */
_sync_directory_allowed(int32_t dir_fd);

PRIVATE int32_t /* different impl for linux and mac */
_finish_open_file_with_odirect(int32_t fd);

PRIVATE int32_t /* different impl for linux and mac */
_rvn_fallocate(int32_t fd, int64_t offset, int64_t size);

PRIVATE char*   /* different impl for linux and mac */
_get_strerror_r(int32_t error, char* tmp_buff, int32_t buf_size);

PRIVATE int64_t
_pwrite(int32_t fd, void *buffer, uint64_t count, uint64_t offset, int32_t *detailed_error_code);

PRIVATE int32_t
_sync_directory_for(const char *file_path, int32_t *detailed_error_code);

PRIVATE int32_t
_sync_directory_for_internal(char *dir_path, int32_t *detailed_error_code);

PRIVATE int32_t
_sync_directory_maybe_symblink(char *dir_path, int32_t *detailed_error_code);

PRIVATE int32_t
_allocate_file_space(int32_t fd, int64_t size, int32_t *detailed_error_code);

PRIVATE int32_t
_open_file_to_read(const char* file_name, int32_t *fd, int32_t* detailed_error_code);

PRIVATE int32_t
_resize_file(int32_t fd, int64_t size, int32_t *detailed_error_code);

PRIVATE int32_t
_read_file(int32_t fd, void* buffer, int64_t required_size, int64_t offset, int64_t* actual_size, int32_t* detailed_error_code);

int32_t
_ensure_path_exists(const char *path, int32_t *detailed_error_code);

#endif
#endif
