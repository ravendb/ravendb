#ifndef INTERNALWIN_H
#define INTERNALWIN_H

#if defined(_WIN32)


struct dirty_bitmap;

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
    int32_t open_flags;
    HANDLE notify;
    char* file_path;
    void* arena;
    size_t arena_size;

    struct dirty_bitmap* dirty_bitmap;
};

// This state represent a single handle to the pager on a file
// multiple such instances may exists at the same time
struct handle
{
    HANDLE file_handle;
    HANDLE file_mapping_handle;
    void* read_address;
    void* write_address;
    int64_t allocation_size;
    int32_t status_flags;
    int64_t locked_memory;
    struct handle_global_state* global_state;
};

PRIVATE int32_t
_write_file(HANDLE hFile, HANDLE hEvent, const void* buffer, int64_t size, int64_t offset, int32_t* detailed_error_code);

PRIVATE int32_t
_write_file_in_sections(HANDLE hFile, HANDLE hEvent, const char* buffer, int64_t size, int64_t offset, uint32_t section_size, int32_t* detailed_error_code);

PRIVATE int32_t
_open_file_to_read(const char *file_name, HANDLE *handle, int32_t *detailed_error_code);

PRIVATE int32_t
_pre_allocate_file(HANDLE handle, int64_t size, int32_t *detailed_error_code);

PRIVATE int32_t
_truncate_file(HANDLE handle, int64_t size, int32_t *detailed_error_code);

PRIVATE int32_t
_read_file(HANDLE handle, void* buffer, int64_t required_size, int64_t offset, int64_t* actual_size, int32_t* detailed_error_code);

PRIVATE
int32_t rvn_write_io_ring(
    void* handle,
    struct page_to_write* buffers,
    int32_t count,
    int32_t* detailed_error_code);

PRIVATE void
_mark_dirty_pages(void *handle, struct page_to_write *buffers, int32_t count);

PRIVATE void
_free_dirty_bitmaps(struct dirty_bitmap *bm);

#endif
#endif
