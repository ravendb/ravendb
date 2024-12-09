#ifndef INTERNALWIN_H
#define INTERNALWIN_H

#if defined(_WIN32)


typedef int32_t
(*rvn_journal_writer)(void* handle, struct journal_entry* buffer, int64_t count_of_entries, int64_t offset, int32_t* detailed_error_code);

struct journal_handle
{
    HANDLE hFile;
    HANDLE hEvent;
    uint64_t elements_count;
    FILE_SEGMENT_ELEMENT* elements;
    rvn_journal_writer writer;
};


PRIVATE int32_t
_write_file(struct journal_handle* handle, const void* buffer, int64_t size, int64_t offset, int32_t* detailed_error_code);

PRIVATE int32_t
_write_file_in_sections(struct journal_handle* handle, const char* buffer, int64_t size, int64_t offset, uint32_t section_size, int32_t* detailed_error_code);

PRIVATE int32_t
_open_file_to_read(const char *file_name, HANDLE *handle, int32_t *detailed_error_code);

PRIVATE int32_t
_pre_allocate_file(HANDLE handle, int64_t size, int32_t *detailed_error_code);

PRIVATE int32_t
_truncate_file(HANDLE handle, int64_t size, int32_t *detailed_error_code);

PRIVATE int32_t
_read_file(HANDLE handle, void* buffer, int64_t required_size, int64_t offset, int64_t* actual_size, int32_t* detailed_error_code);

#endif
#endif
